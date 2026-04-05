"""
Koishi Bridge software
Copyright 2026 Joseph Winkie <jjj333.p.1325@gmail.com>
Licensed as AGPL 3.0
Distributed as-is and without warranty
"""
import sys
import mimetypes
import uuid
import asyncio

import xml.etree.ElementTree as ET

# xmpp library
from slixmpp.componentxmpp import ComponentXMPP
from slixmpp import JID
from slixmpp.types import PresenceArgs

# matrix library
from nio import RoomSendResponse

# import a psycopg error
from psycopg.errors import UniqueViolation

# for the typehint of the custom db class
from db import KoishiDB

import util

REMOVE_FOR = {"urn:xmpp:reply:0", "jabber:x:oob"}


class KoishiComponent(ComponentXMPP):
    def __init__(
        self,
        mapping_by_muc_jid: dict[JID, str],
        db: KoishiDB,
        jid,
        secret,
        server,
        port,
        display_name: str,
        http_domain: str,
        bridged_jnics: set[str],
        bridged_jids: set[str],
        bridged_mx_eventid: set[str],
    ):
        ComponentXMPP.__init__(self, jid, secret, server, port)

        self.jid = jid
        self.display_name = display_name

        self.mapping_by_muc: dict[JID, str] = mapping_by_muc_jid

        self.db: KoishiDB = db
        self.http_domain: str = http_domain

        # TODO: change when matrix side is wraped up in a nice object
        self.bridged_jnics: set[str] = bridged_jnics
        self.bridged_jids: set[str] = bridged_jids
        self.bridged_mx_eventid: set[str] = bridged_mx_eventid

        self.bridged_stanzaid: set[str] = set()

        self.add_event_handler('session_start', self.start)
        self.add_event_handler("groupchat_message", self.groupchat_message)
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

        try:
            async with asyncio.TaskGroup() as tg:
                for muc_jid in self.mapping_by_muc:
                    tg.create_task(
                        self.plugin['xep_0045'].join_muc_wait(
                            room=muc_jid,
                            nick=self.display_name,
                            presence_options=PresenceArgs(pfrom=self.jid)
                        )
                    )

        except ExceptionGroup as eg:
            # pylint: disable=not-an-iterable
            for e in eg.exceptions:
                print(f"Failed to join a MUC: {e}", file=sys.stderr)
            sys.exit(1)

        # set started flag for async loop
        self.started.set()

        print("XMPP Component Joined")

    async def message_error(self, msg):
        ET.indent(msg.xml, space="  ")
        await self.matrix_side.room_send(
            room_id=self.mapping_by_muc[msg['from'].bare],
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

        msg_from = msg.get('from', '')
        if msg_from == '':
            return

        if msg.get('to') != self.jid:
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
        by_id = moderated.get('occupant-id', {}).get('id', 'unknown')
        reason = retract['reason']

        # delete record in db
        result = None
        try:
            # We search by stanza_id here because the moderation came from XMPP
            result = await self.db.delete_media(stanza_id=stanza_id)
        except Exception as e:
            # If the DB fails, we still try to inform XMPP that bridging failed
            self['xep_0461'].make_reply(
                msg['from'],
                stanza_id,
                "",  # body
                mto=msg_from.bare,
                mbody=f"Bridge DB error during moderation: {type(e).__name__}: {e}",
                mtype='groupchat',
                mfrom=self.jid,
            ).send()
            return

        if not result:
            # Message wasn't in our DB, might have already been deleted or never bridged
            return

        event_id = result.get('event_id')
        cached_path = result.get('path')

        # Cleanup local files if this was a media message
        del_task = None
        if cached_path:
            del_task = asyncio.create_task(
                asyncio.to_thread(util.rm_file, cached_path))

        # Perform Matrix Redaction
        if event_id:
            try:
                await self.matrix_side.room_redact(
                    room_id=self.mapping_by_muc[msg['from'].bare],
                    event_id=event_id,
                    reason=f"Moderated by {by_readable} ({by_id}): {reason or '<No reason provided.>'}",
                )
            except Exception as e:
                # Log error or notify XMPP that redaction failed
                self['xep_0461'].make_reply(
                    msg['from'],
                    stanza_id,
                    "",
                    mto=msg_from.bare,
                    mbody=f"Bridge fs error during moderation: {type(e).__name__}: {e}",
                    mtype='groupchat',
                    mfrom=self.jid,
                ).send()

        # catch any fs error
        if del_task:
            try:
                await del_task
            except Exception as e:
                self['xep_0461'].make_reply(
                    msg['from'],
                    stanza_id,
                    "",
                    mto=msg_from.bare,
                    mbody=f"Bridge fs error during moderation: {type(e).__name__}: {e}",
                    mtype='groupchat',
                    mfrom=self.jid,
                ).send()

    async def groupchat_message(self, msg):
        # TODO figure out how to hold until it comes online, a la mutex or js promise

        if self.matrix_side is None:
            print("Not sending to matrix because offline")
            return

        # blank sender
        msg_from = msg.get('from', '')
        if msg_from == '':
            return

        if msg.get('to') != self.jid:
            return

        # server-assigned XEP-0359 Stanza ID used for deduplication and archiving)
        stanzaid = msg.get('stanza_id', {}).get('id')
        if stanzaid is None or stanzaid in self.bridged_stanzaid:
            return
        self.bridged_stanzaid.add(stanzaid)

        bridge_from = msg['from'].bare
        bridge_to = self.mapping_by_muc[bridge_from]

        if msg.get('id', '') in self.bridged_mx_eventid:
            try:
                await self.db.set_xmpp_stanzaid(stanzaid, msg.get('id', ''))
            except Exception as e:
                print(e)
            finally:
                self.bridged_mx_eventid.remove(msg.get('id', ''))
                try:
                    await self.matrix_side.room_read_markers(
                        bridge_to,
                        msg.get('id', ''),
                        msg.get('id', '')
                    )
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

        # get out fallback ranges for features we support
        ranges_to_remove = []
        for fallback in msg['fallbacks']:
            if fallback["for"] in REMOVE_FOR:
                start = int(fallback["body"]["start"])
                end = int(fallback["body"]["end"])
                ranges_to_remove.append((start, end))

        # Remove in reverse order so indices stay valid
        ranges_to_remove.sort(reverse=True)
        result = list(msg['body'])
        for start, end in ranges_to_remove:
            if end > len(result):
                continue
            if start < 0:
                continue
            del result[start:end]

        body = "".join(result).strip()

        url: str | None = msg.get('oob', {}).get('url')
        if not url:
            try:
                # You must 'await' this, otherwise the message is never sent!
                resp: RoomSendResponse = await self.matrix_side.room_send(
                    room_id=bridge_to,
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
                mto=bridge_from,
                id=stanzaid,
                mtype="groupchat",
                marker="displayed",
                mfrom=self.jid
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
                    mto=bridge_from,
                    mbody=f"could not bridge message because of database error of type {type(e)}\n{e}",
                    mtype='groupchat',
                    mfrom=self.jid,
                ).send()
                return

        else:

            # xmpp clients just get this information from the url so we have to add it
            filename: str = url.split('/')[-1]
            mime_type, _ = mimetypes.guess_type(filename)
            if mime_type is None:
                mime_type = "application/octet-stream"
            main_type, _ = mime_type.split('/')

            file_id = str(uuid.uuid4())

            try:
                await self.db.insert_xmpp_media_message_mapping(
                    stanzaid, url, file_id,
                    body, msg['from']
                ),

            # using errors to prevent duplicate message
            except UniqueViolation as _:
                return

            except Exception as e:
                self['xep_0461'].make_reply(
                    msg['from'],
                    stanzaid,
                    body,
                    mto=bridge_from,
                    mbody=f"could not bridge message because of database error of type {type(e)}\n{e}",
                    mtype='groupchat',
                    mfrom=self.jid,
                ).send()
                return

            caption = f"\n{body}" if body != url else ""

            try:
                resp: RoomSendResponse = await self.matrix_side.room_send(
                    room_id=bridge_to,
                    message_type="m.room.message",
                    content={
                        "msgtype": f"m.{main_type if main_type in ['image', 'video', 'audio'] else 'file'}",
                        "body": f"{msg['from'].resource} sent a(n) {mime_type}{caption}",
                        "url": f"mxc://{self.http_domain}/{file_id}",
                        "info": {"mimetype": mime_type},
                        "filename": filename,
                        "external_url": url,
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
                self['xep_0461'].make_reply(
                    msg['from'],
                    stanzaid,
                    body,
                    mto=bridge_from,
                    mbody=f"could not bridge message because of matrix error of type {type(e)}\n{e}",
                    mtype='groupchat',
                    mfrom=self.jid,
                ).send()
                return

            self.plugin['xep_0333'].send_marker(
                mto=bridge_from,
                id=stanzaid,
                mtype="groupchat",
                marker="displayed",
                mfrom=self.jid
            )

            try:
                await self.db.set_mtrx_eventid(resp.event_id, stanzaid)
            except Exception as e:
                self['xep_0461'].make_reply(
                    msg['from'],
                    stanzaid,
                    body,
                    mto=bridge_from,
                    mbody=f"could not bridge message because of database error of type {type(e)}\n{e}",
                    mtype='groupchat',
                    mfrom=self.jid,
                ).send()
                return

        print(resp)
