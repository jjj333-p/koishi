#!/usr/bin/env python3
import json
import logging
import asyncio
from slixmpp import stanza
from slixmpp.componentxmpp import ComponentXMPP
from nio import AsyncClient, MatrixRoom, RoomMessageText, RoomMessageMedia

import mimetypes
import uuid
import secrets

import uvicorn
from fastapi import FastAPI, Response, HTTPException

import httpx
from fastapi.responses import StreamingResponse

# Initialize a client for the proxy to use
proxy_client = httpx.AsyncClient()

app = FastAPI()

xmpp_to_matrix_media_lookup: dict[str, str] = {}
matrix_to_xmpp_media_lookup: dict[str, tuple[str, str, str]] = {}


@app.get("/.well-known/matrix/server")
async def well_known_server():
    """
    Tells other Matrix servers where to send federation traffic.
    This is required if your server name differs from your DNS A record
    or if you are running on a non-standard port.
    """
    return {
        # CHANGE THIS: "hostname:port" of where your federation listener is reachable.
        # If this script is behind a reverse proxy (like Nginx) handling HTTPS on port 8448:
        "m.server": "koishi.pain.agency:443"
    }


@app.get("/_matrix/federation/v1/media/download/{media_id}")
async def federation_media_download(media_id: str):
    """
    Implements the Authenticated Media 'Location' redirect flow
    """

    # Lookup the arbitrary URL in the dictionary
    redirect_url = xmpp_to_matrix_media_lookup.get(media_id)

    # Handle 404 if media ID doesn't exist
    if not redirect_url:
        # The spec suggests falling back to existing endpoints or
        # treating it as non-existent.
        return Response(status_code=404, content="Media not found")

    # Construct the multipart/mixed response manually
    # We need a unique boundary string
    boundary = f"boundary-{secrets.token_hex(16)}"
    CRLF = "\r\n"

    # Part 1: Metadata (Currently empty JSON object according to spec)
    # Format:
    # --boundary
    # Content-Type: application/json
    # [blank line]
    # {}
    part_1 = (
        f"--{boundary}{CRLF}"
        f"Content-Type: application/json{CRLF}"
        f"{CRLF}"
        f"{{}}{CRLF}"
    )

    # Part 2: The Redirect Location
    # This part has no body, only the Location header.
    # Format:
    # --boundary
    # Location: <url>
    # [blank line]
    part_2 = (
        f"--{boundary}{CRLF}"
        f"Location: {redirect_url}{CRLF}"
        f"{CRLF}"
    )

    # Closing boundary
    end = f"--{boundary}--{CRLF}"

    # Combine all parts
    full_body = part_1 + part_2 + end

    # Return the raw Response
    # Note: We must explicitly set the media_type to multipart/mixed with the boundary
    return Response(
        content=full_body,
        status_code=200,
        media_type=f"multipart/mixed; boundary={boundary}"
    )


async def stream_generator(access_token: str, upstream_url: str):
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    print("‼️‼️‼️‼️ upstream url:", upstream_url)

    async with proxy_client.stream("GET", upstream_url, headers=headers) as resp:
        if resp.status_code != 200:
            # If we can't get it, yield the error so the user sees something went wrong
            yield f"Error from Matrix Server: {resp.status_code}".encode()
            return

        # Stream the file chunks
        async for chunk in resp.aiter_bytes():
            yield chunk


@app.get("/matrix-proxy/{media_id}/{file_name}")
async def matrix_proxy(media_id: str, file_name: str):
    # 1. Get the Home Server URL and Access Token from your running bot
    if not matrix_side or not matrix_side.access_token:
        return Response(status_code=503, content="Bridge not logged in yet")

    # 2. Construct the upstream URL to the actual Matrix media repo
    #    (Using the homeserver URL from your login config)
    homeserver_url = login['matrix']['domain']
    if not homeserver_url.startswith("http"):
        homeserver_url = f"https://{homeserver_url}"

    mxc: tuple[str, str] = matrix_to_xmpp_media_lookup.get(media_id)
    if mxc is None:
        return Response(status_code=404, content="Media not found")

    server_name, mxc_id, mime_type = mxc
    upstream_url: str = f"{homeserver_url}/_matrix/client/v1/media/download/{server_name}/{mxc_id}"

    return StreamingResponse(
        stream_generator(matrix_side.access_token, upstream_url),
        media_type=mime_type
    )

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

    media_id: str = str(uuid.uuid4())
    server, mxc_id = event.url.split('/')[-2:]
    filename: str = event.body.split('/')[-1]
    mime_type: str = mimetypes.guess_type(filename)[0]
    matrix_to_xmpp_media_lookup[media_id] = (server, mxc_id, mime_type)

    url: str = f"https://{login['http_domain']}/matrix-proxy/{media_id}/{filename}"

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

        url: str | None = msg.get('oob', {}).get('url')
        if url:

            # xmpp clients just get this information from the url so we have to add it
            filename: str = url.split('/')[-1]
            mime_type, _ = mimetypes.guess_type(filename)
            main_type, _ = mime_type.split('/')

            await matrix_side.room_send(
                room_id="!odwJFwanVTgIblSUtg:matrix.org",
                message_type="m.room.message",
                content={"msgtype": "m.text",
                         "body": f"{msg['from']} sent a(n) {mime_type}"},
            )

            file_id = str(uuid.uuid4())

            xmpp_to_matrix_media_lookup[file_id] = url

            await matrix_side.room_send(
                room_id="!odwJFwanVTgIblSUtg:matrix.org",
                message_type="m.room.message",
                content={
                    "msgtype": f"m.{main_type if main_type in ['image', 'video', 'audio'] else 'file'}",
                    "body": url,
                    "url": f"mxc://{login['http_domain']}/{file_id}",
                    "info": {"mimetype": mime_type},
                    "filename:": filename
                },
            )

            return

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

    # Configure Uvicorn manually
    config = uvicorn.Config(app, host="0.0.0.0", port=4567, log_level="info")
    server = uvicorn.Server(config)

    # Run Matrix (This blocks forever, keeping the script alive)
    # Since we are inside the same asyncio loop, XMPP will keep working
    # in the background while this line runs.
    try:
        # This will run until you hit Ctrl+C
        await asyncio.gather(
            nio_main(),
            server.serve()
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

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Just catch the generic interrupt to suppress the ugly traceback
        pass
