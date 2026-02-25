import mimetypes
import uuid
import asyncio

import xml.etree.ElementTree as ET

# xmpp library
from slixmpp.componentxmpp import ComponentXMPP

# matrix library
from nio import RoomSendResponse

# for the typehint of the custom db class
from db import KoishiDB


class KoishiComponent(ComponentXMPP):
    def __init__(
        self,
        db: KoishiDB,
        jid,
        secret,
        server,
        http_domain: str,
        port,
        bridged_jnics: set[str],
        bridged_jids: set[str],
        bridged_mx_eventid: set[str]
    ):
        ComponentXMPP.__init__(self, jid, secret, server, port)

        self.db: KoishiDB = db
        self.http_domain: str = http_domain

        # TODO: change when matrix side is wraped up in a nice object
        self.bridged_jnics: set[str] = bridged_jnics
        self.bridged_jids: set[str] = bridged_jids
        self.bridged_mx_eventid: set[str] = bridged_mx_eventid

        self.bridged_stanzaid: set[str] = set()

        self.add_event_handler('session_start', self.start)
        self.add_event_handler("message", self.message)
        self.add_event_handler("message_error", self.message_error)
        self.add_event_handler("moderated_message", self.moderated_message)

        self.started: asyncio.Event = asyncio.Event()

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
        self.register_plugin('xep_0333')  # displayed indicator
        self.register_plugin('xep_0422')  # message fastening
        self.register_plugin('xep_0424')  # message retraction
        self.register_plugin('xep_0425')  # message moderation

        self.matrix_side = None

    def set_matrix_side(self, matrix_side) -> None:
        self.matrix_side = matrix_side

    async def start(self, _):
        self['xep_0030'].add_identity(
            category='gateway',
            itype='matrix',      # This triggers the Matrix icon in clients
            name='Koishi Matrix Bridge'
        )

        # Always register disco#info and disco#items
        self['xep_0030'].add_feature('http://jabber.org/protocol/disco#info')
        self['xep_0030'].add_feature('http://jabber.org/protocol/disco#items')

        # Perform initial sends
        self.send_presence()

        # TODO: remove
        self.send_message(
            mto="jjj333@pain.agency",
            mbody="fart",
            mfrom=self.jid,
        )

        await self.plugin['xep_0045'].join_muc(
            room="chaos@group.pain.agency",  # TODO: fix
            nick="Koishi Bridge",  # TODO: this should be a param
            pfrom=self.jid,
        )

        # set started flag for async loop
        self.started.set()

        print("XMPP Component Joined")

    async def message_error(self, msg):
        # TODO: look up from, should be muc jid corresponding to matrix id
        ET.indent(msg.xml, space="  ")
        await self.matrix_side.room_send(
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

    async def moderated_message(self, msg):

        if self.matrix_side is None:
            print("Not sending to matrix because offline")
            return

        # blank sender
        msg_from = msg.get('from', '')
        if msg_from == '':
            return

        if msg.get('to') != "koishi.pain.agency":  # TODO dont hardcode
            return

        # TODO check for server support and ignore if none

        retract = msg.get('retract', {})

        moderated = retract.get('moderated')
        if not moderated:
            return

        stanza_id = retract.get('id')
        if not stanza_id:
            return

        by_readable = retract['by']  # TODO if none, look up occupant id
        by_id = moderated.get('occupant-id', {}).get('id')
        reason = retract['reason']

        result = None
        event_id = None
        try:
            result = await self.db.get_matrix_reply_data(stanza_id)
        except Exception as e:
            print(e)

        if result:
            event_id = result[0]

        if not event_id:
            return

        await self.matrix_side.room_redact(
            room_id='!odwJFwanVTgIblSUtg:matrix.org',  # TODO
            event_id=event_id,
            reason=f"Moderated by {by_readable} ({by_id}) with reason: {reason}",
        )

    async def message(self, msg):
        # TODO figure out how to hold until it comes online, a la mutex or js promise

        if self.matrix_side is None:
            print("Not sending to matrix because offline")
            return

        # blank sender
        msg_from = msg.get('from', '')
        if msg_from == '':
            return

        if msg.get('to') != "koishi.pain.agency":  # TODO dont hardcode
            return

        match(msg.get_type()):
            case ('groupchat'):

                # server-assigned XEP-0359 Stanza ID used for deduplication and archiving)
                stanzaid = msg.get('stanza_id', {}).get('id')
                if stanzaid is None or stanzaid in self.bridged_stanzaid:
                    return
                self.bridged_stanzaid.add(stanzaid)

                if msg.get('id', '') in self.bridged_mx_eventid:
                    try:
                        await self.db.set_xmpp_stanzaid(stanzaid, msg.get('id', ''))
                    except Exception as e:
                        print(e)
                    finally:
                        self.bridged_mx_eventid.remove(msg.get('id', ''))
                        try:
                            await self.matrix_side.room_read_markers(
                                "!odwJFwanVTgIblSUtg:matrix.org", msg.get('id', ''), msg.get('id', ''))
                        except Exception as _:
                            pass
                        finally:
                            # if its in the map can short circut regardless of the return
                            return  # pylint: disable=return-in-finally, lost-exception

                # ignore all puppets
                # TODO clean this up
                if msg_from.resource in self.bridged_jnics or msg_from.bare in self.bridged_jids:
                    return

                xmpp_replyto_id = msg.get('reply', {}).get('id')
                matrix_replyto_id = None
                replyto_mxid = None
                result = None
                if xmpp_replyto_id:
                    try:
                        result = await self.db.get_matrix_reply_data(xmpp_replyto_id)
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
                        resp: RoomSendResponse = await self.matrix_side.room_send(
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

                    self.plugin['xep_0333'].send_marker(
                        mto="chaos@group.pain.agency",  # TODO
                        id=stanzaid,
                        mtype="groupchat",
                        marker="displayed",
                        mfrom="koishi.pain.agency"  # TODO
                    )

                    try:
                        await self.db.insert_message_mapping(
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
                        resp: RoomSendResponse = await self.matrix_side.room_send(
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
                        await self.db.insert_xmpp_media_message_mapping(
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
                        resp: RoomSendResponse = await self.matrix_side.room_send(
                            room_id="!odwJFwanVTgIblSUtg:matrix.org",
                            message_type="m.room.message",
                            content={
                                "msgtype": f"m.{main_type if main_type in ['image', 'video', 'audio'] else 'file'}",
                                "body": body,
                                "url": f"mxc://{self.http_domain}/{file_id}",
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

                    self.plugin['xep_0333'].send_marker(
                        mto="chaos@group.pain.agency",  # TODO
                        id=stanzaid,
                        mtype="groupchat",
                        marker="displayed",
                        mfrom="koishi.pain.agency"  # TODO
                    )

                    try:
                        await self.db.set_mtrx_eventid(resp.event_id, stanzaid)
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
