"""
Koishi Bridge software
Copyright 2026 Joseph Winkie <jjj333.p.1325@gmail.com>
Licensed as AGPL 3.0
Distributed as-is and without warranty
"""
# python stdlib
import sys
import json
import logging
import asyncio
import uuid
import urllib

# xmpp library
from slixmpp import stanza, JID
from slixmpp.types import PresenceArgs

# temporary matrix library
from nio import AsyncClient, MatrixRoom, RoomMessageText, RoomMessageMedia, RoomMessageNotice, Receipt, ReceiptEvent, RedactionEvent

# database
from psycopg.errors import UniqueViolation

import util
from db import KoishiDB
from webserver import KoishiWebserver
from xmpp import KoishiComponent

# collect background tasks
background_tasks = set()

# get login details
with open('login.json', 'r', encoding='utf-8') as login_file:
    login = json.load(login_file)

mapping_by_room_id: dict[str, JID] = {}
mapping_by_muc_jid: dict[JID, str] = {}
for mapping in login['bridge-mapping']:
    jid = JID(mapping['xmpp'])
    if jid.resource:
        print(
            f"{mapping['xmpp']} is not a valid muc id, a muc id should have no resourcepart but this has \"{jid.resource}\"",
            file=sys.stderr
        )
        sys.exit(66)
    mapping_by_room_id[mapping['matrix']] = jid
    mapping_by_muc_jid[jid] = mapping['matrix']

db: KoishiDB = KoishiDB(
    conn_str=login['postgresql_conn'],
    min_connections=1,
    max_connections=3,
)

webserver: KoishiWebserver = KoishiWebserver(
    db, login['matrix']['domain'], login['http_domain'])

# Global variables to store instances
xmpp_side = None
matrix_side = None

# basic deduplication
bridged_jnics: set[str] = set((login['vanity_name'],),)
bridged_jids: set[str] = set((login['xmpp']['jid'],),)
bridged_mx_eventid: set[str] = set()

# lazy
cached_matrix_nick: dict[str, str] = {}

# dont join the same puppet many times
is_joining: dict[str, asyncio.Event] = {}


async def message_handler(room: MatrixRoom, event: RoomMessageText) -> None:

    # temporary until appservice is used
    if event.sender == login['matrix']['mxid']:
        return

    user_jid = f"{event.sender[1:].replace(':','_')}@{login['xmpp']['jid']}"

    muc_jid = mapping_by_room_id[room.room_id]
    if not muc_jid:
        return

    assert xmpp_side, "xmpp_side should be defined before matrix side is connected"

    # hold onto events until they can be bridged
    await xmpp_side.started.wait()

    new_bridged_muc_jid = util.escape_nickname(
        muc_jid,
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
    if is_joining.get(user_jid) is not None:
        await is_joining[user_jid].wait()

    this_cached_nick = cached_matrix_nick.get(event.sender)

    # puppet joining logic
    if new_bridged_nick != this_cached_nick or not user_jid in bridged_jids or event.body.startswith("!join"):

        if this_cached_nick:
            try:
                bridged_jnics.remove(this_cached_nick)
            except Exception as e:
                print("cannot remove nick from cache", e)

        # avoid duplicate joins
        asyncio_event = is_joining.get(user_jid)
        if asyncio_event is None:
            is_joining[user_jid] = asyncio_event = asyncio.Event()
        else:
            asyncio_event.clear()

        try:
            print("joining", new_bridged_nick)
            await xmpp_side.plugin['xep_0045'].join_muc_wait(
                room=muc_jid,
                nick=new_bridged_nick,
                presence_options=PresenceArgs(pfrom=user_jid)
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

        bridged_jids.add(user_jid)
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
            reply_jid or f"{muc_jid}/",
            stanza_id or "",
            fallback=content or "",
            mto=muc_jid,
            mbody=event.body,
            mtype='groupchat',
            mfrom=user_jid,
        )

    else:

        message: stanza.Message = xmpp_side.make_message(
            mto=muc_jid,
            mbody=event.body,
            mtype='groupchat',
            mfrom=user_jid,

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
    await xmpp_side.started.wait()

    user_jid = f"{event.sender[1:].replace(':','_')}@{login['xmpp']['jid']}"

    muc_jid = mapping_by_room_id[room.room_id]
    if not muc_jid:
        return

    media_id: str = str(uuid.uuid4())
    raw_mx_filename = event.source.get('content', {}).get('filename')
    filename: str = urllib.parse.quote_plus(
        (raw_mx_filename or event.body).split('/')[-1]
    )

    new_bridged_muc_jid = util.escape_nickname(
        muc_jid,
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
    if is_joining.get(user_jid) is not None:
        await is_joining[user_jid].wait()

    # join puppet logic
    if new_bridged_nick != cached_matrix_nick.get(event.sender) or not user_jid in bridged_jids:

        # avoid duplicate joins
        asyncio_event = is_joining.get(user_jid)
        if asyncio_event is None:
            is_joining[user_jid] = asyncio_event = asyncio.Event()
        else:
            asyncio_event.clear()

        try:
            await xmpp_side.plugin['xep_0045'].join_muc_wait(
                room=muc_jid,
                nick=new_bridged_nick,
                presence_options=PresenceArgs(pfrom=user_jid)
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

        bridged_jids.add(user_jid)
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
            reply_jid or f"{muc_jid}/",
            stanza_id or "",
            fallback=content or "",
            mto=muc_jid,
            mbody=url if raw_mx_filename is None else f"{event.body}\n{url}",
            mtype='groupchat',
            mfrom=user_jid,
        )

    else:

        message: stanza.Message = xmpp_side.make_message(
            mto=muc_jid,
            mbody=url if raw_mx_filename is None else f"{event.body}\n{url}",
            mtype='groupchat',
            mfrom=user_jid,

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


async def receipt_handler(room: MatrixRoom, event: Receipt):

    assert xmpp_side, "xmpp_side should be defined before matrix side is connected"

    # hold onto events until they can be bridged
    await xmpp_side.started.wait()

    user_jid = f"{event.user_id[1:].replace(':','_')}@{login['xmpp']['jid']}"

    muc_jid = mapping_by_room_id[room.room_id]
    if not muc_jid:
        return

    # dont proceede if a join is in progress
    if is_joining.get(user_jid) is not None:
        await is_joining[user_jid].wait()

    # join puppet logic
    if not user_jid in bridged_jids:
        return

    stanza_id = None

    result = await db.get_xmpp_reply_data(event.event_id)

    if result:
        stanza_id = result[0]
    else:
        return

    xmpp_side.plugin['xep_0333'].send_marker(
        mto=muc_jid,
        id=stanza_id,
        mtype="groupchat",
        marker="displayed",
        mfrom=user_jid
    )


async def receipt_callback(room: MatrixRoom, events: ReceiptEvent):
    for event in events.receipts:

        # only receipt type we care about
        if event.receipt_type != "m.read":
            return

        # temporary until appservice is used
        if event.user_id == login['matrix']['mxid']:
            return

        task: asyncio.Task = asyncio.create_task(receipt_handler(room, event))
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)


async def redaction_handler(room: MatrixRoom, event: RedactionEvent):
    # Skip if the redaction was sent by the bridge itself
    if event.sender == login['matrix']['mxid']:
        return

    muc_jid = mapping_by_room_id[room.room_id]
    if not muc_jid:
        return

    # delete record from db so that bridged media link is broken
    try:
        result = await db.delete_media(event_id=event.redacts)
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
                "body": f"Could not bridge your redaction because of database error of type {type(e)}. error:\n{e}",
            }
        )
        return

    # the message may not be bridged or may have already been removed
    if not result:
        return

    # data from the db we care about
    stanza_id = result.get('stanza_id')
    cached_path = result.get('path')

    # Cleanup local files if this was a media message
    del_task = None
    if cached_path:
        del_task = asyncio.create_task(
            asyncio.to_thread(util.rm_file, cached_path))

    # XMPP Moderation (XEP-0425)
    if stanza_id:
        try:
            await xmpp_side['xep_0425'].moderate(
                room=muc_jid,
                id=stanza_id,
                reason=f"redacted by {event.sender}: {event.reason or '<No reason provided.>'}",
                ifrom=xmpp_side.jid
            )
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
                    "body": f"Could not bridge your redaction because of xmpp error of type {type(e)}. error:\n{e}",
                }
            )
            return

    # catch any fs error
    if del_task:
        try:
            await del_task
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
                    "body": f"Could not bridge your redaction because of filesystem error of type {type(e)}. error:\n{e}",
                }
            )
            return


async def redaction_callback(room: MatrixRoom, event: RedactionEvent):
    task: asyncio.Task = asyncio.create_task(redaction_handler(room, event))
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)


async def nio_main() -> None:
    global matrix_side  # <--- THIS IS MISSING

    matrix_side = AsyncClient(
        login['matrix']['domain'], login['matrix']['mxid'])
    webserver.set_matrix_side(matrix_side)
    xmpp_side.set_matrix_side(matrix_side)
    matrix_side.add_event_callback(
        message_callback,  # callback that throws it into a thread
        (RoomMessageText, RoomMessageNotice)  # types to handle
    )
    matrix_side.add_event_callback(
        media_callback,  # callback that throws it into a thread
        RoomMessageMedia  # type to handle
    )
    matrix_side.add_event_callback(
        redaction_callback,
        RedactionEvent
    )
    matrix_side.add_ephemeral_callback(
        receipt_callback,
        ReceiptEvent
    )

    await matrix_side.login(login['matrix']['password'])

    await matrix_side.sync(timeout=30000)

    # join rooms all at once
    try:
        async with asyncio.TaskGroup() as tg:
            for r in mapping_by_room_id:
                tg.create_task(matrix_side.join(r))
    except* ExceptionGroup as eg:
        # pylint: disable=not-an-iterable
        for e in eg.exceptions:
            print(f"Task failed: {e}", file=sys.stderr)
        sys.exit(1)

    # Sync forever acts as the "run_forever" for the Matrix side
    await matrix_side.sync_forever(timeout=30000)


async def main():
    global xmpp_side

    # Initialize XMPP
    xmpp_side = KoishiComponent(
        mapping_by_muc_jid,
        db,
        login['xmpp']['jid'],
        login['xmpp']['secret'],
        login['xmpp']['domain'],
        login['xmpp']['port'],
        login['vanity_name'],
        login['http_domain'],
        bridged_jnics,
        bridged_jids,
        bridged_mx_eventid,
    )

    # Setup Logging
    logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname)-8s %(message)s')

    # This attaches XMPP to the event loop in the background.
    await db.connect()

    # Run Matrix (This blocks forever, keeping the script alive)
    # Since we are inside the same asyncio loop, XMPP will keep working
    # in the background while this line runs.
    try:
        # This will run until you hit Ctrl+C
        await asyncio.gather(
            nio_main(),
            webserver.serve(port=4567, log_level="info"),
            xmpp_side.connect()
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
