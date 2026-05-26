"""
Koishi Bridge software
Copyright 2026 Joseph Winkie <jjj333.p.1325@gmail.com>
Licensed as AGPL 3.0
Distributed as-is and without warranty
"""
import asyncio
import uuid
import urllib
import mimetypes
import html

# matrix library
from nio import MatrixRoom, RoomMessageText, RoomMessageMedia, Receipt, RedactionEvent, RoomSendResponse
# import a psycopg error
from slixmpp import stanza, JID
from slixmpp.types import PresenceArgs
from slixmpp.plugins.xep_0428.stanza import Fallback
from psycopg.errors import UniqueViolation

# koishi objects
from xmpp import KoishiComponent
from db import KoishiDB
import util
from formatting import matrix_html_to_xep0393, xep0393_to_matrix_html

# TODO: copy over control character sanitization


class KoishiRoom:
    def __init__(self, http_domain: str, mappingJSON: dict[str, str], db: KoishiDB, xmpp_side: KoishiComponent, matrix_side,):

        self.muc_jid_str: str = mappingJSON.get("xmpp")
        self.muc_jid: JID = JID(self.muc_jid_str)

        self.http_domain = http_domain

        if db is None:
            raise ValueError("DB must not be None")
        self.db = db

        if xmpp_side is None:
            raise ValueError("xmpp_side must not be None")

        self.xmpp = xmpp_side

        self.mx_rid = mappingJSON.get("matrix")
        self.matrix = matrix_side

        self.xmpp_queue_lock = asyncio.Lock()
        self.matrix_queue_lock = asyncio.Lock()

        self.bridged_stanzaid: set[str] = set()
        self.bridged_mx_eventid: set[str] = set()

        self.bridged_jnics: set[str] = set()
        self.bridged_jids: set[str] = set()
        self.bridged_mx_eventid: set[str] = set()
        self.cached_bridged_jnics: dict[str, str] = {}

        # Moderation sync is always attempted. If the bridge lacks Matrix power
        # level or XMPP MUC affiliation for an operation, the server will reject
        # it and Koishi will log the failure.
        self.pending_moderation_ttl = 60
        self.xmpp_banlist_poll_interval = 300
        self._xmpp_known_outcasts: set[str] | None = None
        self._xmpp_banlist_task: asyncio.Task | None = None

    def connect_matrix(self, matrix_side):
        pass

    async def connect(self):
        """Connect room event handlers and join both MUC and Matrix room"""

        # slixmpp doesnt seem to support the muc:: style handler for moderations
        self.xmpp.moderated_message_handlers[self.muc_jid] = self.handle_xmpp_moderation

        # Register XMPP event handlers
        async def _handle_xmpp_message(msg):
            await self.handle_xmpp_message(msg)

        async def _handle_xmpp_message_error(msg):
            await self.handle_xmpp_message_error(msg)

        async def _handle_xmpp_presence(presence):
            await self.handle_xmpp_presence(presence)

        self.xmpp.add_event_handler(
            f"muc::{self.muc_jid_str}::message", _handle_xmpp_message)

        self.xmpp.add_event_handler(
            f"muc::{self.muc_jid_str}::message_error", _handle_xmpp_message_error)

        self.xmpp.add_event_handler(
            f"muc::{self.muc_jid_str}::presence", _handle_xmpp_presence)

        # Wait for XMPP component to be ready
        await self.xmpp.started.wait()

        # Join the MUC as the bridge component
        print(
            f"Joining XMPP MUC: {self.muc_jid_str} as {self.xmpp.display_name}")
        try:
            await self.xmpp.plugin['xep_0045'].join_muc_wait(
                room=self.muc_jid,
                nick=self.xmpp.display_name,
                presence_options=PresenceArgs(pfrom=self.xmpp.boundjid.bare),
                maxchars=0,
                timeout=30
            )
            print(f"Successfully joined XMPP MUC: {self.muc_jid_str}")
        except Exception as e:
            print(f"Failed to join XMPP MUC {self.muc_jid_str}: {e}")
            raise

        # Wait for Matrix client to be ready
        await self.matrix.connected.wait()

        # Join the Matrix room
        print(f"Joining Matrix room: {self.mx_rid}")
        try:
            await self.matrix.join(self.mx_rid)
            print(f"Successfully joined Matrix room: {self.mx_rid}")
        except Exception as e:
            print(f"Failed to join Matrix room {self.mx_rid}: {e}")
            raise

        self._xmpp_banlist_task = asyncio.create_task(self._xmpp_banlist_worker())

    async def handle_xmpp_message_error(self, msg):
        if self.matrix is None:
            raise RuntimeError("Matrix side not initialized")

        import xml.etree.ElementTree as ET

        ET.indent(msg.xml, space="  ")

        try:
            await self.matrix.room_send(
                room_id=self.mx_rid,
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
        except Exception as e:
            print(
                f"Could not send error message to Matrix: {type(e).__name__}: {e}")

    async def handle_xmpp_message(self, msg):

        if self.matrix is None:
            raise RuntimeError("Matrix side not initialized")

        msg_from = msg.get('from', '')
        if msg_from == '':
            return

        if msg.get('to') != self.xmpp.boundjid.bare:
            return

        stanza_id = msg.get('stanza_id', {}).get('id')

        # server-assigned XEP-0359 Stanza ID used for deduplication and archiving)
        if stanza_id is None:
            return

        if msg.get('id', '') in self.bridged_mx_eventid:
            try:
                await self.db.set_xmpp_stanzaid(stanza_id, msg.get('id', ''))
            except Exception as e:
                print(e)
            finally:
                self.bridged_mx_eventid.remove(msg.get('id', ''))
                try:
                    await self.matrix.room_read_markers(
                        self.mx_rid,
                        msg.get('id', ''),
                        msg.get('id', '')
                    )
                except Exception as e:
                    print(e)

            # if its in the map can short circut regardless of the return
            return

        # closure for consistent error handling
        async def send_error(error_str: str, fallback_txt: str = '') -> None:
            self.xmpp['xep_0461'].make_reply(
                msg_from,
                stanza_id,
                fallback_txt,
                mto=msg_from.bare,
                mbody=error_str,
                mtype='groupchat',
                mfrom=self.xmpp.boundjid.bare,
            ).send()

        async with self.xmpp_queue_lock:
            # server-assigned XEP-0359 Stanza ID used for deduplication and archiving)
            if stanza_id in self.bridged_stanzaid:
                return
            self.bridged_stanzaid.add(stanza_id)

            # ignore all puppets
            # TODO clean this up
            if msg_from.resource in self.bridged_jnics or msg_from.bare in self.bridged_jids:
                return

            # get reply mapping from db
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

            # get attachment url
            attachment_url: str | None = msg.get('oob', {}).get('url')

            # fallbacks for found and supported features
            REMOVE_FOR = []
            if matrix_replyto_id:
                REMOVE_FOR.append("urn:xmpp:reply:0")
            if attachment_url:
                REMOVE_FOR.append("jabber:x:oob")

            body = util.remove_fallbacks(
                msg['body'],
                REMOVE_FOR,
                msg['fallbacks']
            )

            matrix_displayname = html.escape(msg['from'].resource, quote=False)
            matrix_body = f"{msg['from'].resource}: {body}"
            matrix_formatted_body = (
                f"<strong data-mx-profile-fallback>{matrix_displayname}: </strong> "
                f"{xep0393_to_matrix_html(body)}"
            )

            # wait for connection
            await self.matrix.connected.wait()

            if not attachment_url:
                try:
                    # You must 'await' this, otherwise the message is never sent!
                    resp: RoomSendResponse = await self.matrix.room_send(
                        room_id=self.mx_rid,
                        message_type="m.room.message",
                        content={
                            "msgtype": "m.text",
                            "body": matrix_body,
                            "format": "org.matrix.custom.html",
                            "formatted_body": matrix_formatted_body,
                            "com.beeper.per_message_profile": {
                                "id": str(msg['from']),
                                "displayname": msg['from'].resource,
                                "has_fallback": True,
                            },
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
                    await send_error(
                        f"Could not bridge message because of Matrix error of type {type(e)}\n{e}",
                        body
                    )
                    return

                self.xmpp['xep_0333'].send_marker(
                    mto=self.muc_jid_str,
                    id=stanza_id,
                    mtype="groupchat",
                    marker="displayed",
                    mfrom=self.xmpp.boundjid.bare
                )

                try:
                    await self.db.insert_message_mapping(
                        stanza_id,
                        resp.event_id,
                        body,
                        str(msg['from'])
                    )
                except UniqueViolation as _:
                    # this is working as intended as to not duplicate messages
                    return
                except Exception as e:
                    await send_error(
                        f"could not bridge message because of database error of type {type(e)}\n{e}",
                        body
                    )
                    return

            else:

                # xmpp clients just get this information from the url so we have to add it
                filename: str = attachment_url.split('/')[-1]
                mime_type, _ = mimetypes.guess_type(filename)
                if mime_type is None:
                    mime_type = "application/octet-stream"
                main_type, _ = mime_type.split('/')

                file_id = str(uuid.uuid4())

                try:
                    await self.db.insert_xmpp_media_message_mapping(
                        stanza_id, attachment_url, file_id,
                        body, msg['from']
                    )

                # using errors to prevent duplicate message
                except UniqueViolation as _:
                    return

                except Exception as e:
                    await send_error(
                        f"could not bridge message because of database error of type {type(e)}\n{e}",
                        body
                    )
                    return

                caption = f"\n{body}" if body != attachment_url else ""
                formatted_caption = (
                    f"<br>{xep0393_to_matrix_html(body)}"
                    if body != attachment_url
                    else ""
                )

                try:
                    resp: RoomSendResponse = await self.matrix.room_send(
                        room_id=self.mx_rid,
                        message_type="m.room.message",
                        content={
                            "msgtype": f"m.{main_type if main_type in ['image', 'video', 'audio'] else 'file'}",
                            "body": f"{msg['from'].resource}: sent a(n) {mime_type}{caption}",
                            "format": "org.matrix.custom.html",
                            "formatted_body": (
                                f"<strong data-mx-profile-fallback>{matrix_displayname}: </strong> sent a(n) "
                                f"{html.escape(mime_type, quote=False)}"
                                f"{formatted_caption}"
                            ),
                            "url": f"mxc://{self.http_domain}/{file_id}",
                            "info": {"mimetype": mime_type},
                            "filename": filename,
                            "external_url": attachment_url,
                            "com.beeper.per_message_profile": {
                                "id": str(msg['from']),
                                "displayname": msg['from'].resource,
                                "has_fallback": True,
                            },
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
                    await send_error(
                        f"Could not bridge message because of Matrix error of type {type(e)}\n{e}",
                        body
                    )
                    return

                self.xmpp['xep_0333'].send_marker(
                    mto=self.muc_jid_str,
                    id=stanza_id,
                    mtype="groupchat",
                    marker="displayed",
                    mfrom=self.xmpp.boundjid.bare
                )

                try:
                    await self.db.set_mtrx_eventid(resp.event_id, stanza_id)
                except Exception as e:
                    await send_error(
                        f"could not bridge message because of database error of type {type(e)}\n{e}",
                        body
                    )
                    return

    async def handle_xmpp_moderation(self, msg):
        if self.matrix is None:
            raise RuntimeError("Matrix side not initialized")

        msg_from = msg.get('from', '')
        if msg_from == '':
            return

        if msg.get('to') != self.xmpp.boundjid.bare:
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

        # closure for consistent error handling
        async def send_error(error_str: str, fallback_txt: str = '') -> None:
            self.xmpp['xep_0461'].make_reply(
                msg_from,
                stanza_id,
                fallback_txt,
                mto=msg_from.bare,
                mbody=error_str,
                mtype='groupchat',
                mfrom=self.xmpp.boundjid.bare,
            ).send()

        async with self.xmpp_queue_lock:
            # delete record in db
            result = None
            try:
                # We search by stanza_id here because the moderation came from XMPP
                result = await self.db.delete_media(stanza_id=stanza_id)
            except Exception as e:
                # If the DB fails, we still try to inform XMPP that bridging failed
                await send_error(
                    f"Bridge DB error during moderation: {type(e).__name__}: {e}")
                return

            if not result:
                # Message wasn't in our DB, might have already been deleted or never bridged
                # we in theory with the lock shouldnt recieve the moderation before a message
                return

            event_id = result.get('event_id')
            cached_path = result.get('path')

            # Cleanup local files if this was a media message
            del_task = None
            if cached_path:
                del_task = asyncio.create_task(
                    asyncio.to_thread(util.rm_file, cached_path))

            # wait for connection
            await self.matrix.connected.wait()

            # Perform Matrix Redaction
            if event_id:
                try:
                    await self.matrix.room_redact(
                        room_id=self.mx_rid,
                        event_id=event_id,
                        reason=f"Moderated by {by_readable or '<unknown>'} ({by_id}): {reason or '<No reason provided.>'}",
                    )
                except Exception as e:
                    # Log error or notify XMPP that redaction failed
                    await send_error(
                        f"Matrix error during moderation: {type(e).__name__}: {e}"
                    )

            # catch any fs error
            if del_task:
                try:
                    await del_task
                except Exception as e:
                    await send_error(
                        f"Bridge fs error during moderation: {type(e).__name__}: {e}"
                    )

    def _matrix_user_to_puppet_jid(self, mxid: str) -> str:
        return f"{mxid[1:].replace(':','_')}@{self.xmpp.boundjid.bare}"

    @staticmethod
    def _matrix_membership(event) -> tuple[str | None, str | None, str | None]:
        content = event.source.get("content", {}) if hasattr(event, "source") else {}
        prev_content = event.source.get("unsigned", {}).get("prev_content", {}) \
            if hasattr(event, "source") else {}
        membership = content.get("membership", getattr(event, "membership", None))
        previous = prev_content.get("membership")
        reason = content.get("reason", getattr(event, "reason", None))
        return membership, previous, reason

    @staticmethod
    def _xmpp_muc_status_codes(presence) -> set[str]:
        """Return XEP-0045 status codes using Slixmpp's MUC stanza plugin."""
        muc = presence.get_plugin("muc", check=True)
        if muc is None:
            return set()

        return {str(code) for code in muc["status_codes"]}

    @staticmethod
    def _xmpp_muc_item_value(muc, key: str) -> str | None:
        """Read values from Slixmpp's XEP-0045 MUC stanza plugins.

        The MUC user stanza exposes affiliation, role, jid and nick directly
        through the ``muc`` plugin. The optional reason and actor fields belong
        to the nested muc#user <item/> plugin, so they must be read through
        ``muc['item']`` instead of as direct MUC user interfaces.
        """
        if muc is None:
            return None

        if key in {"affiliation", "jid", "nick", "role"}:
            value = muc[key]
            return str(value) if value else None

        item = muc.get_plugin("item", check=True)
        if item is None:
            return None

        if key == "reason":
            value = item["reason"]
            return str(value) if value else None

        if key == "actor":
            actor = item.get_plugin("actor", check=True)
            if actor is None:
                return None

            value = actor["jid"] or actor["nick"]
            return str(value) if value else None

        return None

    async def _remember_mapping(
        self, matrix_room_id: str, matrix_user_id: str, puppet_jid: str, puppet_nick: str | None
    ) -> None:
        try:
            await self.db.upsert_user_mapping(
                matrix_room_id=matrix_room_id,
                xmpp_room_jid=self.muc_jid_str,
                matrix_user_id=matrix_user_id,
                xmpp_puppet_jid=str(JID(puppet_jid).bare),
                xmpp_puppet_nick=puppet_nick,
            )
        except Exception as e:
            print(f"Could not update moderation mapping: {type(e).__name__}: {e}")

    async def _remember_pending_moderation(
        self,
        direction: str,
        action: str,
        matrix_user_id: str | None = None,
        xmpp_puppet_jid: str | None = None,
        xmpp_puppet_nick: str | None = None,
    ) -> None:
        try:
            await self.db.insert_moderation_action(
                matrix_room_id=self.mx_rid,
                xmpp_room_jid=self.muc_jid_str,
                direction=direction,
                action=action,
                ttl_seconds=self.pending_moderation_ttl,
                matrix_user_id=matrix_user_id,
                xmpp_puppet_jid=str(JID(xmpp_puppet_jid).bare) if xmpp_puppet_jid else None,
                xmpp_puppet_nick=xmpp_puppet_nick,
            )
        except Exception as e:
            print(f"Could not store pending moderation action: {type(e).__name__}: {e}")

    async def _consume_pending_moderation(
        self,
        direction: str,
        action: str,
        matrix_user_id: str | None = None,
        xmpp_puppet_jid: str | None = None,
        xmpp_puppet_nick: str | None = None,
    ) -> bool:
        try:
            return await self.db.consume_moderation_action(
                matrix_room_id=self.mx_rid,
                xmpp_room_jid=self.muc_jid_str,
                direction=direction,
                action=action,
                matrix_user_id=matrix_user_id,
                xmpp_puppet_jid=str(JID(xmpp_puppet_jid).bare) if xmpp_puppet_jid else None,
                xmpp_puppet_nick=xmpp_puppet_nick,
            )
        except Exception as e:
            print(f"Could not read pending moderation action: {type(e).__name__}: {e}")
            return False

    async def handle_matrix_member_event(self, room: MatrixRoom, event) -> None:
        """Bridge Matrix kick/ban events to the matching XMPP puppet."""
        if not self.matrix or not self.xmpp:
            raise RuntimeError("Bridge sides not initialized")
        if event.sender == self.matrix.mxid:
            return

        target_mxid = getattr(event, "state_key", None)
        if not target_mxid:
            return

        membership, previous_membership, reason = self._matrix_membership(event)
        action = None
        if membership == "ban":
            action = "ban"
        elif membership == "leave" and previous_membership == "ban":
            action = "unban"
        elif (
            membership == "leave"
            and previous_membership in ("join", "invite")
            and event.sender != target_mxid
        ):
            action = "kick"

        if action is None:
            return

        if await self._consume_pending_moderation(
            "xmpp_to_matrix", action, matrix_user_id=target_mxid
        ):
            return

        try:
            mapping = await self.db.get_mapping_by_matrix_user(
                self.mx_rid, self.muc_jid_str, target_mxid
            )
        except Exception as e:
            print(f"Could not look up Matrix moderation mapping: {type(e).__name__}: {e}")
            mapping = None

        puppet_jid = mapping[0] if mapping else self._matrix_user_to_puppet_jid(target_mxid)
        puppet_nick = mapping[1] if mapping else self.cached_bridged_jnics.get(target_mxid)
        readable_reason = reason or "<No reason provided.>"
        xmpp_reason = f"Bridged Matrix {action} by {event.sender}: {readable_reason}"

        await self.xmpp.started.wait()
        try:
            if action in ("ban", "unban"):
                affiliation = "outcast" if action == "ban" else "none"
                await self._remember_pending_moderation(
                    "matrix_to_xmpp", action, target_mxid, puppet_jid, puppet_nick
                )
                await self.xmpp.plugin["xep_0045"].set_affiliation(
                    room=self.muc_jid,
                    jid=JID(puppet_jid),
                    affiliation=affiliation,
                    reason=xmpp_reason,
                    ifrom=self.xmpp.boundjid.bare,
                )
            elif action == "kick":
                if not puppet_nick:
                    print(
                        f"Cannot bridge Matrix kick for {target_mxid}: no XMPP puppet nick is known"
                    )
                    return
                await self._remember_pending_moderation(
                    "matrix_to_xmpp", "kick", target_mxid, puppet_jid, puppet_nick
                )
                await self.xmpp.plugin["xep_0045"].set_role(
                    room=self.muc_jid,
                    nick=puppet_nick,
                    role="none",
                    reason=xmpp_reason,
                    ifrom=self.xmpp.boundjid.bare,
                )
        except Exception as e:
            print(f"Could not bridge Matrix {action} to XMPP: {type(e).__name__}: {e}")

    async def handle_xmpp_presence(self, presence) -> None:
        """Bridge XMPP MUC kick/ban events for Matrix puppets to Matrix."""
        if not self.matrix or not self.xmpp:
            raise RuntimeError("Bridge sides not initialized")
        if presence.get("to") != self.xmpp.boundjid.bare:
            return

        muc_from = presence.get("from", "")
        if not muc_from or getattr(muc_from, "bare", None) != self.muc_jid_str:
            return

        nick = getattr(muc_from, "resource", None)
        if not nick or nick == self.xmpp.display_name:
            return

        codes = self._xmpp_muc_status_codes(presence)
        muc = presence.get_plugin("muc", check=True)
        if muc is None:
            return

        affiliation = self._xmpp_muc_item_value(muc, "affiliation")
        item_jid = self._xmpp_muc_item_value(muc, "jid")
        actor = self._xmpp_muc_item_value(muc, "actor") or "<unknown>"
        reason = self._xmpp_muc_item_value(muc, "reason") or "<No reason provided.>"

        action = None
        if "301" in codes or affiliation == "outcast":
            action = "ban"
        elif "307" in codes:
            action = "kick"

        if action is None:
            return

        try:
            mapping = await self.db.get_mapping_by_xmpp_user(
                self.mx_rid,
                self.muc_jid_str,
                xmpp_puppet_jid=str(JID(item_jid).bare) if item_jid else None,
                xmpp_puppet_nick=nick,
            )
        except Exception as e:
            print(f"Could not look up XMPP moderation mapping: {type(e).__name__}: {e}")
            mapping = None

        if not mapping:
            print(
                f"Ignoring XMPP {action} for {self.muc_jid_str}/{nick}: "
                "no Matrix puppet mapping is known"
            )
            return

        target_mxid, puppet_jid, puppet_nick = mapping
        if action == "ban" and self._xmpp_known_outcasts is not None:
            self._xmpp_known_outcasts.add(str(JID(puppet_jid).bare))

        await self._bridge_xmpp_membership_to_matrix(
            action, target_mxid, puppet_jid, puppet_nick or nick, actor, reason
        )


    async def _get_xmpp_outcast_jids(self) -> set[str]:
        """Return bare JIDs on the XMPP MUC outcast list.

        This requires the bridge to have enough MUC rights. If the server rejects
        the request, the caller logs and skips XMPP -> Matrix unban detection.
        """
        await self.xmpp.started.wait()
        result = await self.xmpp.plugin["xep_0045"].get_affiliation_list(
            room=self.muc_jid,
            affiliation="outcast",
            ifrom=self.xmpp.boundjid.bare,
        )

        return {str(JID(jid).bare) for jid in result if jid}

    async def _xmpp_banlist_worker(self) -> None:
        """Poll the MUC ban list to detect XMPP-side bans and unbans.

        XMPP MUC unbans of absent users usually do not create occupant presence
        events, so presence alone cannot make XMPP -> Matrix unban sync reliable.
        Polling the outcast list fills that gap whenever Koishi has admin rights.
        """
        while True:
            try:
                await self.sync_xmpp_banlist_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"Could not poll XMPP ban list for {self.muc_jid_str}: {type(e).__name__}: {e}")

            try:
                await asyncio.sleep(self.xmpp_banlist_poll_interval)
            except asyncio.CancelledError:
                raise

    async def sync_xmpp_banlist_once(self) -> None:
        """Compare the XMPP outcast list with known Matrix puppets."""
        current_outcasts = await self._get_xmpp_outcast_jids()
        current_puppet_outcasts = set()

        try:
            mappings = await self.db.get_mappings_for_room(self.mx_rid, self.muc_jid_str)
        except Exception as e:
            print(f"Could not read moderation mappings for {self.muc_jid_str}: {type(e).__name__}: {e}")
            return

        by_jid = {str(JID(puppet_jid).bare): (mxid, str(JID(puppet_jid).bare), nick)
                  for mxid, puppet_jid, nick in mappings}
        current_puppet_outcasts = set(by_jid).intersection(current_outcasts)

        if self._xmpp_known_outcasts is None:
            self._xmpp_known_outcasts = current_puppet_outcasts
            return

        newly_banned = current_puppet_outcasts - self._xmpp_known_outcasts
        newly_unbanned = self._xmpp_known_outcasts - current_puppet_outcasts
        self._xmpp_known_outcasts = current_puppet_outcasts

        for puppet_jid in newly_banned:
            mxid, stored_jid, nick = by_jid[puppet_jid]
            await self._bridge_xmpp_membership_to_matrix(
                "ban",
                mxid,
                stored_jid,
                nick,
                actor="<XMPP MUC ban list>",
                reason="Detected in XMPP MUC outcast list",
            )

        for puppet_jid in newly_unbanned:
            mxid, stored_jid, nick = by_jid[puppet_jid]
            await self._bridge_xmpp_membership_to_matrix(
                "unban",
                mxid,
                stored_jid,
                nick,
                actor="<XMPP MUC ban list>",
                reason="No longer present in XMPP MUC outcast list",
            )

    async def _bridge_xmpp_membership_to_matrix(
        self,
        action: str,
        target_mxid: str,
        puppet_jid: str | None,
        puppet_nick: str | None,
        actor: str,
        reason: str,
    ) -> None:
        """Apply an XMPP-side membership action to Matrix with loop protection."""
        if await self._consume_pending_moderation(
            "matrix_to_xmpp", action, target_mxid, puppet_jid, puppet_nick
        ):
            return

        matrix_reason = f"Bridged XMPP {action} by {actor}: {reason}"
        await self.matrix.connected.wait()
        try:
            await self._remember_pending_moderation(
                "xmpp_to_matrix", action, target_mxid, puppet_jid, puppet_nick
            )
            if action == "ban":
                await self.matrix.room_ban(
                    room_id=self.mx_rid,
                    user_id=target_mxid,
                    reason=matrix_reason,
                )
            elif action == "unban":
                await self.matrix.room_unban(
                    room_id=self.mx_rid,
                    user_id=target_mxid,
                    reason=matrix_reason,
                )
            elif action == "kick":
                await self.matrix.room_kick(
                    room_id=self.mx_rid,
                    user_id=target_mxid,
                    reason=matrix_reason,
                )
        except Exception as e:
            print(f"Could not bridge XMPP {action} to Matrix: {type(e).__name__}: {e}")

    async def join_xmpp_puppet_for_matrix(self, room: MatrixRoom, mxid: str, jid: str, nick: str, join_anyways: bool):
        cached_nick = self.cached_bridged_jnics.get(mxid)

        if join_anyways or nick != cached_nick or not jid in self.bridged_jids:

            if cached_nick:
                try:
                    self.bridged_jnics.remove(cached_nick)
                except Exception as e:
                    print("cannot remove nick from cache", e)

            print("joining", nick)
            await self.xmpp.plugin['xep_0045'].join_muc_wait(
                room=self.muc_jid,
                nick=nick,
                presence_options=PresenceArgs(pfrom=jid),
                maxchars=0,
                timeout=15
            )
            print("joined", nick)

            self.cached_bridged_jnics[mxid] = nick
            self.bridged_jnics.add(nick)
            self.bridged_jids.add(jid)
            await self._remember_mapping(room.room_id, mxid, jid, nick)

    async def handle_matrix_text_message(self, room: MatrixRoom, event: RoomMessageText):

        if not self.matrix:
            raise RuntimeError("Matrix not yet initialized")
        if not self.xmpp:
            raise RuntimeError("XMPP not yet initialized")

        # temporary until appservice is used
        if event.sender == self.matrix.mxid:
            return

        user_jid = f"{event.sender[1:].replace(':','_')}@{self.xmpp.boundjid.bare}"

        new_bridged_muc_jid = util.escape_nickname(
            self.muc_jid_str,
            room.user_name(event.sender) or event.sender
        )
        new_bridged_nick = new_bridged_muc_jid.resource

        matrix_content = event.source.get('content', {})
        formatted_body = matrix_content.get('formatted_body')
        body_for_xmpp = (
            matrix_html_to_xep0393(formatted_body)
            if matrix_content.get('format') == "org.matrix.custom.html" and formatted_body
            else event.body
        )
        sanitized_body = util.illegal_xml_chars_regex.sub('', body_for_xmpp)

        async def send_error(error_str: str):
            await self.matrix.room_send(
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
                    "body": error_str,
                }
            )

        async with self.matrix_queue_lock:
            # hold onto events until they can be bridged
            await self.xmpp.started.wait()

            try:
                await self.join_xmpp_puppet_for_matrix(
                    room, event.sender, user_jid, f"{new_bridged_nick} [Matrix]",
                    event.body.startswith("!join")
                )
            except TimeoutError as _:
                await send_error(
                    "Timed out trying to join puppet to MUC. "
                    "This is most often caused by your nickname being already in use. "
                    "Please change your displayname and try again."
                )
                return
            except Exception as e:
                await send_error(
                    f"Could not join puppet because of {type(e)} error:\n{e}")
                return

            create_row_task: asyncio.Task = asyncio.create_task(
                self.db.insert_msg_from_mtrx(
                    event.event_id,
                    sanitized_body,
                    new_bridged_muc_jid,
                    event.sender
                )
            )

            self.bridged_mx_eventid.add(event.event_id)

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
                    result = await self.db.get_xmpp_reply_data(mx_reply_to_id)
                except Exception as e:
                    print(e)

            if result:
                if len(result) > 2:
                    stanza_id, reply_jid, content, *_ = result
                else:
                    stanza_id = result[0]

            if stanza_id:

                message: stanza.Message = self.xmpp['xep_0461'].make_reply(
                    reply_jid or f"{self.muc_jid_str}/",
                    stanza_id or "",
                    fallback=content or "",
                    mto=self.muc_jid,
                    mbody=sanitized_body,
                    mtype='groupchat',
                    mfrom=user_jid,
                )

            else:

                message: stanza.Message = self.xmpp.make_message(
                    mto=self.muc_jid,
                    mbody=sanitized_body,
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
                await send_error(
                    f"Could not bridge your message because of database error of type {type(e)}. error:\n{e}")
                return

            try:
                message.send()
            except Exception as e:
                await send_error(
                    f"Could not bridge your message because matrix error of type {type(e)} error:\n{e}"
                )

    async def handle_matrix_media_message(self, room: MatrixRoom, event: RoomMessageMedia) -> None:
        if not self.matrix:
            raise RuntimeError("Matrix not yet initialized")
        if not self.xmpp:
            raise RuntimeError("XMPP not yet initialized")

        # temporary until appservice is used
        if event.sender == self.matrix.mxid:
            return

        user_jid = f"{event.sender[1:].replace(':','_')}@{self.xmpp.boundjid.bare}"

        new_bridged_muc_jid = util.escape_nickname(
            self.muc_jid_str,
            room.user_name(event.sender) or event.sender
        )
        new_bridged_nick = new_bridged_muc_jid.resource

        matrix_content = event.source.get('content', {})
        formatted_body = matrix_content.get('formatted_body')
        body_for_xmpp = (
            matrix_html_to_xep0393(formatted_body)
            if matrix_content.get('format') == "org.matrix.custom.html" and formatted_body
            else event.body
        )
        sanitized_body = util.illegal_xml_chars_regex.sub('', body_for_xmpp)

        media_id: str = str(uuid.uuid4())
        raw_mx_filename = event.source.get('content', {}).get('filename')
        filename: str = urllib.parse.quote_plus(
            (raw_mx_filename or sanitized_body).split('/')[-1]
        )

        async def send_error(error_str: str):
            await self.matrix.room_send(
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
                    "body": error_str,
                }
            )

        async with self.matrix_queue_lock:
            # hold onto events until they can be bridged
            await self.xmpp.started.wait()

            try:
                await self.join_xmpp_puppet_for_matrix(
                    room, event.sender, user_jid, f"{new_bridged_nick} [Matrix]",
                    event.body.startswith("!join")
                )
            except TimeoutError as _:
                await send_error(
                    "Timed out trying to join puppet to MUC. \
                        This is most often caused by your nickname being already in use. \
                            Please change your displayname and try again."
                )
                return
            except Exception as e:
                await send_error(
                    f"Could not join puppet because of {type(e)} error:\n{e}")
                return

            create_row_task: asyncio.Task = asyncio.create_task(
                self.db.insert_media_msg_from_mtrx(
                    event.event_id,
                    event.url,
                    media_id,
                    sanitized_body,
                    filename,
                    new_bridged_muc_jid,
                    event.sender
                )
            )

            self.bridged_mx_eventid.add(event.event_id)

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
                    result = await self.db.get_xmpp_reply_data(mx_reply_to_id)
                except Exception as e:
                    print(e)

            if result:
                if len(result) > 2:
                    stanza_id, reply_jid, content, *_ = result
                else:
                    stanza_id = result[0]

            # dont send across until we have recorded it in the database
            try:
                await create_row_task
            except UniqueViolation as _:
                # this is working as intended as to not duplicate messages
                return
            except Exception as e:
                await send_error(
                    f"Could not bridge your message because of database error of type {type(e)}. error:\n{e}"
                )
                return

            # format of https url to send to xmpp side
            url: str = f"https://{self.http_domain}/matrix-proxy/{media_id}/{filename}"

            # if theres an id to reply to for the xmpp side
            if stanza_id:

                has_caption = raw_mx_filename is not None and raw_mx_filename != sanitized_body

                message: stanza.Message = self.xmpp['xep_0461'].make_reply(
                    reply_jid or f"{self.muc_jid_str}/",
                    stanza_id or "",
                    fallback=content or "",
                    mto=self.muc_jid,
                    mbody=f"{sanitized_body}\n{url}" if has_caption else url,
                    mtype='groupchat',
                    mfrom=user_jid,
                )

            # we cant reply to anything, form a message normally
            else:

                # if it doesnt qualify for the caption display spec dont bridge it
                if raw_mx_filename is None or raw_mx_filename == sanitized_body:
                    message: stanza.Message = self.xmpp.make_message(
                        mto=self.muc_jid,
                        mbody=url,
                        mtype='groupchat',
                        mfrom=user_jid,
                    )

                else:

                    # create base message
                    message: stanza.Message = self.xmpp.make_message(
                        mto=self.muc_jid,
                        mbody=f"{sanitized_body}\n{url}",
                        mtype='groupchat',
                        mfrom=user_jid,
                    )

                    # create fallback for the oob element
                    fallback = Fallback()
                    fallback['for'] = "jabber:x:oob"

                    # get start offset, +1 for the \n
                    start = len(sanitized_body) + 1

                    # add the range
                    # pylint: disable=invalid-sequence-index
                    fallback["body"]["start"] = start
                    # pylint: disable=invalid-sequence-index
                    fallback["body"]["end"] = start + len(url)

                    message.append(fallback)

            message.set_id(event.event_id)

            # attach media tag
            # pylint: disable=invalid-sequence-index
            message['oob']['url'] = url

            try:
                message.send()
            except Exception as e:
                await send_error(
                    f"Could not bridge your message because of XMPP error of type {type(e)}. error:\n{e}"
                )

    async def handle_matrix_receipt(self, room: MatrixRoom, event: Receipt):
        if not self.matrix:
            raise RuntimeError("Matrix not yet initialized")
        if not self.xmpp:
            raise RuntimeError("XMPP not yet initialized")

        # temporary until appservice is used
        if event.user_id == self.matrix.mxid:
            return

        # hold onto events until they can be bridged
        await self.xmpp.started.wait()

        user_jid = f"{event.user_id[1:].replace(':','_')}@{self.xmpp.boundjid.bare}"

        # only send receipts for joined puppets
        if not user_jid in self.bridged_jids:
            return

        stanza_id = None

        try:
            result = await self.db.get_xmpp_reply_data(event.event_id)
        except Exception as e:
            print(e)
            return

        if result:
            stanza_id = result[0]
        else:
            return

        try:
            self.xmpp['xep_0333'].send_marker(
                mto=self.muc_jid,
                id=stanza_id,
                mtype="groupchat",
                marker="displayed",
                mfrom=user_jid
            )
        except Exception as e:
            print(f"Could not send read receipt: {type(e).__name__}: {e}")

    async def handle_matrix_redaction(self, room: MatrixRoom, event: RedactionEvent):
        if not self.matrix:
            raise RuntimeError("Matrix not yet initialized")
        if not self.xmpp:
            raise RuntimeError("XMPP not yet initialized")

        # Skip if the redaction was sent by the bridge itself
        if event.sender == self.matrix.mxid:
            return

        async def send_error(error_str: str):
            await self.matrix.room_send(
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
                    "body": error_str,
                }
            )

        async with self.matrix_queue_lock:
            # delete record from db so that bridged media link is broken
            try:
                result = await self.db.delete_media(event_id=event.redacts)
            except Exception as e:
                await send_error(
                    f"Could not bridge your redaction because of database error of type {type(e)}. error:\n{e}"
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

                if event.reason:
                    mx_reason = util.illegal_xml_chars_regex.sub(
                        '', event.reason)
                else:
                    mx_reason = '<No reason provided.>'

                try:
                    await self.xmpp['xep_0425'].moderate(
                        room=self.muc_jid,
                        id=stanza_id,
                        reason=f"redacted by {event.sender}: {mx_reason}",
                        ifrom=self.xmpp.boundjid.bare
                    )
                except Exception as e:
                    await send_error(
                        f"Could not bridge your redaction because of xmpp error of type {type(e)}. error:\n{e}"
                    )
                    return

            # catch any fs error
            if del_task:
                try:
                    await del_task
                except Exception as e:
                    await send_error(
                        f"Could not bridge your redaction because of filesystem error of type {type(e)}. error:\n{e}"
                    )
                    return
