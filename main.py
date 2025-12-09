#!/usr/bin/env python3
import json
import logging
import asyncio
from slixmpp import stanza
from slixmpp.componentxmpp import ComponentXMPP
from nio import AsyncClient, MatrixRoom, RoomMessageText, RoomMessageMedia

# get login details
with open('login.json', 'r', encoding='utf-8') as login_file:
    login = json.load(login_file)

# Global variables to store instances
xmpp_side = None
matrix_side = None

# basic deduplication
bridged_jnics: set[str] = set()
bridged_jids: set[str] = set()
bridged_stanzaid: set[str] = set()

tmp_muc_id = "chaos@group.pain.agency"  # TODO


async def message_callback(room: MatrixRoom, event: RoomMessageText) -> None:

    if event.sender == login['matrix']['mxid']:
        return

    assert xmpp_side, "xmpp_side should be defined before matrix side is connected"

    # hold onto events until they can be bridged
    xmpp_side.session_bind_event.wait()

    jid = f"{event.sender[1:].replace(':','_')}@{login['xmpp']['jid']}"

    bridged_jids.add(jid)
    bridged_jnics.add(event.sender)

    # Fix: Ensure this awaits correctly
    await xmpp_side.plugin['xep_0045'].join_muc(
        room=tmp_muc_id,
        nick=event.sender,
        pfrom=jid,
    )

    xmpp_side.send_message(
        mto=tmp_muc_id,
        mbody=event.body,
        mtype='groupchat',
        mfrom=jid
    )


async def media_callback(room: MatrixRoom, event: RoomMessageMedia) -> None:
    assert xmpp_side, "xmpp_side should be defined before matrix side is connected"

    # hold onto events until they can be bridged
    xmpp_side.session_bind_event.wait()

    jid = f"{event.sender[1:].replace(':','_')}@{login['xmpp']['jid']}"

    bridged_jids.add(jid)
    bridged_jnics.add(event.sender)

    url: str = f"{login['matrix']['domain']}/_matrix/media/v3/download/{event.url[6:]}/{event.body}"

    # boilerplate message obj
    message: stanza.Message = xmpp_side.make_message(
        mto=tmp_muc_id,
        mbody=url,
        mtype='groupchat',
        mfrom=jid
    )

    # attach media tag
    message['oob']['url'] = url
    message.send()


async def nio_main() -> None:
    global matrix_side  # <--- THIS IS MISSING

    matrix_side = AsyncClient(
        login['matrix']['domain'], login['matrix']['mxid'])
    matrix_side.add_event_callback(message_callback, RoomMessageText)
    matrix_side.add_event_callback(media_callback, RoomMessageMedia)

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
        self.add_event_handler("message", self.message)

        # Register plugins
        self.register_plugin('xep_0030')
        self.register_plugin('xep_0004')
        self.register_plugin('xep_0060')
        self.register_plugin('xep_0199')
        self.register_plugin('xep_0045')
        self.register_plugin('xep_0461')
        self.register_plugin('xep_0363')
        # (Unique and Stable Stanza IDs)
        self.register_plugin('xep_0359')
        self.register_plugin('xep_0066')  # media

    async def start(self, event):
        self.send_presence()
        print("XMPP Component Joined")

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

        # ignore all puppets
        if msg_from.resource in bridged_jnics or msg_from.bare in bridged_jids:
            return

        # server-assigned XEP-0359 Stanza ID used for deduplication and archiving)
        stanzaid = msg.get('stanza_id', {}).get('id')
        if stanzaid is None or stanzaid in bridged_stanzaid:
            return
        bridged_stanzaid.add(stanzaid)

        # You must 'await' this, otherwise the message is never sent!
        await matrix_side.room_send(
            room_id="!odwJFwanVTgIblSUtg:matrix.org",
            message_type="m.room.message",
            content={"msgtype": "m.text",
                     "body": f"{msg['from']}:\n{msg.get('body', 'No body found !?')}"},
        )


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
        await nio_main()
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

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Just catch the generic interrupt to suppress the ugly traceback
        pass
