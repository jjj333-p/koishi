#!/usr/bin/env python3
# python stdlib
import json
import logging
import asyncio
import mimetypes
import uuid
import urllib

# xml parsing
import xml.etree.ElementTree as ET

# xmpp library
from slixmpp import stanza
from slixmpp.componentxmpp import ComponentXMPP

# temporary matrix library
from nio import AsyncClient, MatrixRoom, RoomMessageText, RoomMessageMedia, RoomSendResponse, RoomMessageNotice

# make fetching shit work
import httpx

# database
from psycopg.errors import UniqueViolation

import util
from db import KoishiDB
from webserver import KoishiWebserver

# collect background tasks
background_tasks = set()

# connect=5.0: Wait max 5s to establish connection
# read=300.0: Wait max 5 mins for data to arrive (or set to None for infinite)
# write=5.0: Wait max 5s to send request
# pool=5.0: Wait max 5s to get a connection from the pool
timeout_config = httpx.Timeout(300.0, connect=5.0)

# get login details
with open('login.json', 'r', encoding='utf-8') as login_file:
    login = json.load(login_file)

db: KoishiDB = KoishiDB(
    conn_str=login['postgresql_conn'],
    min_connections=1,
    max_connections=3,
)

webserver: KoishiWebserver = KoishiWebserver(db, login['matrix']['domain'])

# Global variables to store instances
xmpp_side = None
matrix_side = None

xmpp_side_started = asyncio.Event()

# basic deduplication
bridged_jnics: set[str] = set()
bridged_jids: set[str] = set()
bridged_stanzaid: set[str] = set()
bridged_mx_eventid: set[str] = set()

# lazy
cached_matrix_nick: dict[str, str] = {}

# dont join the same puppet many times
is_joining: dict[str, asyncio.Event] = {}

tmp_muc_id = "chaos@group.pain.agency"  # TODO


async def message_handler(room: MatrixRoom, event: RoomMessageText) -> None:

    # temporary until appservice is used
    if event.sender == login['matrix']['mxid']:
        return

    jid = f"{event.sender[1:].replace(':','_')}@{login['xmpp']['jid']}"

    assert xmpp_side, "xmpp_side should be defined before matrix side is connected"

    # hold onto events until they can be bridged
    await xmpp_side_started.wait()

    new_bridged_muc_jid = util.escape_nickname(
        "chaos@group.pain.agency",
        room.user_name(event.sender)
    )
    new_bridged_nick = new_bridged_muc_jid.resource

    create_row_task: asyncio.Task = asyncio.create_task(
        db.insert_msg_from_mtrx(
            event.event_id,
            event.body,
            new_bridged_muc_jid,
            event.sender
        )
    )

    bridged_mx_eventid.add(event.event_id)

    # dont proceede if a join is in progress
    if is_joining.get(jid) is not None:
        await is_joining[jid].wait()

    # puppet joining logic
    if new_bridged_nick != cached_matrix_nick.get(event.sender) or not jid in bridged_jids or event.body.startswith("!join"):

        # avoid duplicate joins
        asyncio_event = is_joining.get(jid)
        if asyncio_event is None:
            is_joining[jid] = asyncio_event = asyncio.Event()
        else:
            asyncio_event.clear()

        try:
            print("joining", new_bridged_nick)
            await xmpp_side.plugin['xep_0045'].join_muc(
                room=tmp_muc_id,
                nick=new_bridged_nick,
                pfrom=jid,
            )
            print("joined", new_bridged_nick)

            cached_matrix_nick[event.sender] = new_bridged_nick
            asyncio_event.set()

        except Exception as e:

            asyncio_event.set()

            await matrix_side.room_send(
                room_id=room.room_id,
                message_type="m.room.message",
                content={
                    "msgtype": "m.text",
                    "m.mentions": {
                        "user_ids": [
                            event.sender
                        ]
                    },
                    "m.relates_to": {
                        "m.in_reply_to": {
                            "event_id": event.event_id
                        }
                    },
                    "body": f"Could not join puppet because of {type(e)} error:\n{e}",
                }
            )

            return

        bridged_jids.add(jid)
        bridged_jnics.add(new_bridged_nick)

    mx_reply_to_id = event.source \
        .get('content', {}) \
        .get("m.relates_to", {}) \
        .get("m.in_reply_to", {}) \
        .get("event_id", None)

    result = None
    stanza_id = None
    reply_jid = None
    content = None
    if mx_reply_to_id:
        try:
            result = await db.get_xmpp_reply_data(mx_reply_to_id)
        except Exception as e:
            print(e)

    if result:
        if len(result) > 2:
            stanza_id, reply_jid, content, *_ = result
        else:
            stanza_id = result[0]

    if stanza_id:

        message: stanza.Message = xmpp_side['xep_0461'].make_reply(
            reply_jid or "chaos@group.pain.agency/",
            stanza_id or "",
            fallback=content or "",
            mto=tmp_muc_id,
            mbody=event.body,
            mtype='groupchat',
            mfrom=jid,
        )

    else:

        message: stanza.Message = xmpp_side.make_message(
            mto=tmp_muc_id,
            mbody=event.body,
            mtype='groupchat',
            mfrom=jid,

        )

    message.set_id(event.event_id)

    # dont send across until we have recorded it in the database
    try:
        await create_row_task
    except UniqueViolation as _:
        # this is working as intended as to not duplicate messages
        return
    except Exception as e:
        await matrix_side.room_send(
            room_id=room.room_id,
            message_type="m.room.message",
            content={
                "msgtype": "m.text",
                "m.mentions": {
                    "user_ids": [
                        event.sender
                    ]
                },
                "m.relates_to": {
                    "m.in_reply_to": {
                        "event_id": event.event_id
                    }
                },
                "body": f"Could not bridge your message because of database error of type {type(e)}. error:\n{e}",
            }
        )
        return

    try:
        message.send()
    except Exception as e:

        await matrix_side.room_send(
            room_id=room.room_id,
            message_type="m.room.message",
            content={
                "msgtype": "m.text",
                "m.mentions": {
                    "user_ids": [
                        event.sender
                    ]
                },
                "m.relates_to": {
                    "m.in_reply_to": {
                        "event_id": event.event_id
                    }
                },
                "body": f"Could not bridge your message because of {type(e)} error:\n{e}",
            }
        )


async def message_callback(room: MatrixRoom, event) -> None:
    """actually coroutine this shit"""
    task: asyncio.Task = asyncio.create_task(message_handler(room, event))
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)


async def media_handler(room: MatrixRoom, event: RoomMessageMedia) -> None:

    # temporary until appservice is used
    if event.sender == login['matrix']['mxid']:
        return

    assert xmpp_side, "xmpp_side should be defined before matrix side is connected"

    # hold onto events until they can be bridged
    await xmpp_side_started.wait()

    jid = f"{event.sender[1:].replace(':','_')}@{login['xmpp']['jid']}"

    media_id: str = str(uuid.uuid4())
    raw_mx_filename = event.source.get('content', {}).get('filename')
    filename: str = urllib.parse.quote_plus(
        (raw_mx_filename or event.body).split('/')[-1]
    )

    new_bridged_muc_jid = util.escape_nickname(
        "chaos@group.pain.agency",
        room.user_name(event.sender)
    )
    new_bridged_nick = new_bridged_muc_jid.resource

    create_row_task: asyncio.Task = asyncio.create_task(
        db.insert_media_msg_from_mtrx(
            event.event_id,
            event.url,
            media_id,
            event.body,
            filename,
            new_bridged_muc_jid,
            event.sender
        )
    )

    # dont proceede if a join is in progress
    if is_joining.get(jid) is not None:
        await is_joining[jid].wait()

    # join puppet logic
    if new_bridged_nick != cached_matrix_nick.get(event.sender) or not jid in bridged_jids:

        # avoid duplicate joins
        asyncio_event = is_joining.get(jid)
        if asyncio_event is None:
            is_joining[jid] = asyncio_event = asyncio.Event()
        else:
            asyncio_event.clear()

        try:
            await xmpp_side.plugin['xep_0045'].join_muc(
                room=tmp_muc_id,
                nick=new_bridged_nick,
                pfrom=jid,
            )

            cached_matrix_nick[event.sender] = new_bridged_nick
            asyncio_event.set()

        except Exception as e:

            asyncio_event.set()

            await matrix_side.room_send(
                room_id=room.room_id,
                message_type="m.room.message",
                content={
                    "msgtype": "m.text",
                    "m.mentions": {
                        "user_ids": [
                            event.sender
                        ]
                    },
                    "m.relates_to": {
                        "m.in_reply_to": {
                            "event_id": event.event_id
                        }
                    },
                    "body": f"Could not join puppet because of {type(e)} error:\n{e}",
                }
            )

            return

        bridged_jids.add(jid)
        bridged_jnics.add(new_bridged_nick)

    try:
        await create_row_task
    except UniqueViolation as _:
        # this is working as intended as to not duplicate messages
        return
    except Exception as e:
        await matrix_side.room_send(
            room_id=room.room_id,
            message_type="m.room.message",
            content={
                "msgtype": "m.text",
                "m.mentions": {
                    "user_ids": [
                        event.sender
                    ]
                },
                "m.relates_to": {
                    "m.in_reply_to": {
                        "event_id": event.event_id
                    }
                },
                "body": f"Could not bridge your message because of database error of type {type(e)}. error:\n{e}",
            }
        )
        return

    bridged_mx_eventid.add(event.event_id)

    url: str = f"https://{login['http_domain']}/matrix-proxy/{media_id}/{filename}"

    mx_reply_to_id = event.source \
        .get('content', {}) \
        .get("m.relates_to", {}) \
        .get("m.in_reply_to", {}) \
        .get("event_id", None)

    result = None
    stanza_id = None
    reply_jid = None
    content = None
    if mx_reply_to_id:
        try:
            result = await db.get_xmpp_reply_data(mx_reply_to_id)
        except Exception as e:
            print(e)

    if result:
        if len(result) > 2:
            stanza_id, reply_jid, content, *_ = result
        else:
            stanza_id = result[0]

    if stanza_id:

        message: stanza.Message = xmpp_side['xep_0461'].make_reply(
            reply_jid or "chaos@group.pain.agency/",
            stanza_id or "",
            fallback=content or "",
            mto=tmp_muc_id,
            mbody=url if raw_mx_filename is None else f"{event.body}\n{url}",
            mtype='groupchat',
            mfrom=jid,
        )

    else:

        message: stanza.Message = xmpp_side.make_message(
            mto=tmp_muc_id,
            mbody=url if raw_mx_filename is None else f"{event.body}\n{url}",
            mtype='groupchat',
            mfrom=jid,

        )

    message.set_id(event.event_id)

    # attach media tag
    # pylint: disable=invalid-sequence-index
    message['oob']['url'] = url
    message.send()


async def media_callback(room, event) -> None:
    """actually coroutine this shit pt 2"""
    task: asyncio.Task = asyncio.create_task(media_handler(room, event))
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)


async def nio_main() -> None:
    global matrix_side  # <--- THIS IS MISSING

    matrix_side = AsyncClient(
        login['matrix']['domain'], login['matrix']['mxid'])
    webserver.set_matrix_side(matrix_side)
    matrix_side.add_event_callback(
        message_callback,  # callback that throws it into a thread
        (RoomMessageText, RoomMessageNotice)  # types to handle
    )
    matrix_side.add_event_callback(
        media_callback,  # callback that throws it into a thread
        RoomMessageMedia  # type to handle
    )

    await matrix_side.login(login['matrix']['password'])

    # Join room if necessary
    # await client.join("!odwJFwanVTgIblSUtg:matrix.org")

    await matrix_side.room_send(
        room_id="!odwJFwanVTgIblSUtg:matrix.org",  # TODO
        message_type="m.room.message",
        content={"msgtype": "m.text", "body": "Hello world!"},
    )

    # Sync forever acts as the "run_forever" for the Matrix side
    await matrix_side.sync_forever(timeout=30000)


class EchoComponent(ComponentXMPP):
    def __init__(self, jid, secret, server, port):
        ComponentXMPP.__init__(self, jid, secret, server, port)

        self.add_event_handler('session_start', self.start)
        self.add_event_handler("message", self.message)
        self.add_event_handler("message_error", self.message_error)

        # Register plugins
        self.register_plugin('xep_0030')  # Service Discovery (Disco)
        self.register_plugin('xep_0004')  # Data Forms
        self.register_plugin('xep_0060')  # Publish-Subscribe
        self.register_plugin('xep_0199')  # XMPP Ping
        self.register_plugin('xep_0045')  # Multi-User Chat (MUC)
        self.register_plugin('xep_0461')  # Message Replies
        self.register_plugin('xep_0428')  # Fallback Indication
        self.register_plugin('xep_0359')  # Unique and Stable Stanza IDs
        self.register_plugin('xep_0066')  # Out of Band Data

    async def start(self, _):
        self.send_presence()

        # set started flag for async loop
        xmpp_side_started.set()

        self['xep_0030'].add_identity(
            category='gateway',
            itype='matrix',      # This triggers the Matrix icon in clients
            name='Koishi Matrix Bridge'
        )

        # Always register disco#info and disco#items
        self['xep_0030'].add_feature('http://jabber.org/protocol/disco#info')
        self['xep_0030'].add_feature('http://jabber.org/protocol/disco#items')

        print("XMPP Component Joined")

    async def message_error(self, msg):
        # TODO: look up from, should be muc jid corresponding to matrix id
        ET.indent(msg.xml, space="  ")
        await matrix_side.room_send(
            room_id="!odwJFwanVTgIblSUtg:matrix.org",
            message_type="m.room.message",
            content={
                "msgtype": "m.text",
                "m.relates_to": {
                    "m.in_reply_to": {
                        "event_id": msg['id'],
                    },
                },
                "body": str(msg)
            },
        )

    # Change 'def' to 'async def' so you can use 'await' inside
    async def message(self, msg):
        # TODO figure out how to hold until it comes online, a la mutex or js promise

        if matrix_side is None:
            print("Not sending to matrix because offline")
            return

        # blank sender
        msg_from = msg.get('from', '')
        if msg_from == '':
            return

        match(msg.get_type()):
            case ('groupchat'):

                # server-assigned XEP-0359 Stanza ID used for deduplication and archiving)
                stanzaid = msg.get('stanza_id', {}).get('id')
                if stanzaid is None or stanzaid in bridged_stanzaid:
                    return
                bridged_stanzaid.add(stanzaid)

                if msg.get('id', '') in bridged_mx_eventid:
                    try:
                        await db.set_xmpp_stanzaid(stanzaid, msg.get('id', ''))
                    except Exception as e:
                        print(e)
                    finally:
                        bridged_mx_eventid.remove(msg.get('id', ''))
                        try:
                            await matrix_side.room_read_markers(
                                "!odwJFwanVTgIblSUtg:matrix.org", msg.get('id', ''), msg.get('id', ''))
                        except Exception as _:
                            pass
                        finally:
                            # if its in the map can short circut regardless of the return
                            # pylint: disable=return-in-finally
                            # pylint: disable=lost-exception
                            return

                # ignore all puppets
                # TODO clean this up
                if msg_from.resource in bridged_jnics or msg_from.bare in bridged_jids:
                    return

                xmpp_replyto_id = msg.get('reply', {}).get('id')
                matrix_replyto_id = None
                replyto_mxid = None
                result = None
                if xmpp_replyto_id:
                    try:
                        result = await db.get_matrix_reply_data(xmpp_replyto_id)
                    except Exception as e:
                        print(e)

                if result:
                    if len(result) > 1:
                        matrix_replyto_id, replyto_mxid, *_ = result
                    else:
                        matrix_replyto_id = result[0]

                fallback_range = msg['fallback']['body']
                b = msg.get('body', 'No body found !?')
                if fallback_range == '':
                    body = b
                else:
                    # get start and end of the fallback
                    start = int(fallback_range.get('start', 0))
                    end = int(fallback_range.get('end', 0))

                    # sanity check ranges
                    if end <= start or not start < len(b) or not end < len(b):
                        body = b
                    else:
                        # cut around range
                        if start > 0:
                            part1 = b[:start]
                        else:
                            part1 = ''
                        part2 = b[end:]

                        body = part1 + part2

                url: str | None = msg.get('oob', {}).get('url')
                if not url:
                    try:
                        # You must 'await' this, otherwise the message is never sent!
                        resp: RoomSendResponse = await matrix_side.room_send(
                            room_id="!odwJFwanVTgIblSUtg:matrix.org",
                            message_type="m.room.message",
                            content={
                                "msgtype": "m.text",
                                "body": f"{msg['from'].resource}:\n{body}",
                                # TODO: properly parse out mentions based on bridged displayname
                                ** (
                                    {
                                        "m.mentions": {
                                            "user_ids": [
                                                replyto_mxid
                                            ],
                                        },
                                    } if replyto_mxid else {}
                                ),
                                ** (
                                    {
                                        "m.relates_to": {
                                            "m.in_reply_to": {
                                                "event_id": matrix_replyto_id
                                            },
                                        },
                                    } if matrix_replyto_id else {}
                                ),
                            }
                        )
                    except Exception as e:
                        print(e)
                        return

                    try:
                        await db.insert_message_mapping(
                            stanzaid,
                            resp.event_id,
                            body,
                            str(msg['from'])
                        )
                    except Exception as e:
                        self['xep_0461'].make_reply(
                            msg['from'],
                            stanzaid,
                            body,
                            mto="chaos@group.pain.agency",
                            mbody=f"could not bridge message because of database error of type {type(e)}\n{e}",
                            mtype='groupchat',
                            mfrom="koishi.pain.agency",
                        )
                        return

                else:

                    # xmpp clients just get this information from the url so we have to add it
                    filename: str = url.split('/')[-1]
                    mime_type, _ = mimetypes.guess_type(filename)
                    if mime_type is None:
                        mime_type = "application/octet-stream"
                    main_type, _ = mime_type.split('/')

                    try:
                        resp: RoomSendResponse = await matrix_side.room_send(
                            room_id="!odwJFwanVTgIblSUtg:matrix.org",
                            message_type="m.room.message",
                            content={
                                "msgtype": "m.text",
                                "body": f"{msg['from'].resource} sent a(n) {mime_type}",
                                # TODO: properly parse out mentions based on bridged displayname
                                ** (
                                    {
                                        "m.mentions": {
                                            "user_ids": [
                                                replyto_mxid,
                                            ],
                                        },
                                    } if replyto_mxid else {}
                                ),
                                ** (
                                    {
                                        "m.relates_to": {
                                            "m.in_reply_to": {
                                                "event_id": matrix_replyto_id
                                            },
                                        },
                                    } if matrix_replyto_id else {}
                                ),
                            },
                        )
                    except Exception as e:
                        print(e)
                        return

                    file_id = str(uuid.uuid4())

                    try:
                        await db.insert_xmpp_media_message_mapping(
                            stanzaid, url, file_id,
                            body, msg['from']
                        ),

                    except Exception as e:
                        self['xep_0461'].make_reply(
                            msg['from'],
                            stanzaid,
                            body,
                            mto="chaos@group.pain.agency",
                            mbody=f"could not bridge message because of database error of type {type(e)}\n{e}",
                            mtype='groupchat',
                            mfrom="koishi.pain.agency",
                        )
                        return

                    try:
                        resp: RoomSendResponse = await matrix_side.room_send(
                            room_id="!odwJFwanVTgIblSUtg:matrix.org",
                            message_type="m.room.message",
                            content={
                                "msgtype": f"m.{main_type if main_type in ['image', 'video', 'audio'] else 'file'}",
                                "body": body,
                                "url": f"mxc://{login['http_domain']}/{file_id}",
                                "info": {"mimetype": mime_type},
                                "filename:": filename
                            },
                        )
                    except Exception as e:
                        self['xep_0461'].make_reply(
                            msg['from'],
                            stanzaid,
                            body,
                            mto="chaos@group.pain.agency",
                            mbody=f"could not bridge message because of matrix error of type {type(e)}\n{e}",
                            mtype='groupchat',
                            mfrom="koishi.pain.agency",
                        )
                        return

                    try:
                        await db.set_mtrx_eventid(resp.event_id, stanzaid)
                    except Exception as e:
                        self['xep_0461'].make_reply(
                            msg['from'],
                            stanzaid,
                            body,
                            mto="chaos@group.pain.agency",
                            mbody=f"could not bridge message because of database error of type {type(e)}\n{e}",
                            mtype='groupchat',
                            mfrom="koishi.pain.agency",
                        )
                        return

                print(resp)


async def main():
    global xmpp_side

    # Setup Logging
    logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname)-8s %(message)s')

    # Initialize XMPP
    xmpp_side = EchoComponent(
        login['xmpp']['jid'],
        login['xmpp']['secret'],
        login['xmpp']['domain'],
        login['xmpp']['port'],
    )

    await db.connect()

    # This attaches XMPP to the event loop in the background.
    xmpp_side.connect()

    # Perform initial sends
    # TODO: remove
    xmpp_side.send_message(
        mto="jjj333@pain.agency",
        mbody="fart",
        mfrom=login['xmpp']['jid'],
    )

    await xmpp_side.plugin['xep_0045'].join_muc(
        room="chaos@group.pain.agency",  # TODO: fix
        nick=login.get(
            'vanity_name',
            '‼️ "vanity_name" field missing from login.json'
        ),
        pfrom=login['xmpp']['jid'],
    )

    # Run Matrix (This blocks forever, keeping the script alive)
    # Since we are inside the same asyncio loop, XMPP will keep working
    # in the background while this line runs.
    try:
        # This will run until you hit Ctrl+C
        await asyncio.gather(
            nio_main(),
            webserver.serve(port=4567, log_level="info"),
        )
    except asyncio.CancelledError:
        # This handles the internal signal that asyncio uses to stop
        print("Tasks cancelled, shutting down...")
    finally:
        # --- FIX: Graceful Disconnect Here ---
        print("Disconnecting XMPP...")
        if xmpp_side:
            # .disconnect() returns a future, so we await it here
            # Note: Standard slixmpp disconnect() does not usually take a 'reason' argument
            await xmpp_side.disconnect()
            print("XMPP Disconnected.")

        await db.close()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Just catch the generic interrupt to suppress the ugly traceback
        pass
