#!/usr/bin/env python3
# python stdlib
import json
import logging
import asyncio
import mimetypes
import uuid
import secrets

# xml parsing
import xml.etree.ElementTree as ET

# xmpp library
from slixmpp import stanza
from slixmpp.componentxmpp import ComponentXMPP

# temporary matrix library
from nio import AsyncClient, MatrixRoom, RoomMessageText, RoomMessageMedia, RoomSendResponse

# make fetching shit work
import httpx

# webserver
import uvicorn
from fastapi import FastAPI, Response
from fastapi.responses import StreamingResponse

# database
import psycopg_pool

# connect=5.0: Wait max 5s to establish connection
# read=300.0: Wait max 5 mins for data to arrive (or set to None for infinite)
# write=5.0: Wait max 5s to send request
# pool=5.0: Wait max 5s to get a connection from the pool
timeout_config = httpx.Timeout(300.0, connect=5.0)

# get login details
with open('login.json', 'r', encoding='utf-8') as login_file:
    login = json.load(login_file)

# create db pool, do not connect now because it has to be inside the async loop (what is python)
db_pool = psycopg_pool.AsyncConnectionPool(
    conninfo=login['postgresql_conn'],
    min_size=1,
    max_size=3,
    open=False,
)

# Initialize a client for the proxy to use
proxy_client = httpx.AsyncClient()
app = FastAPI()


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
    async with db_pool.connection() as conn:
        async with conn.cursor() as cursor:
            # curr.
            await cursor.execute(
                "select original_xmpp_media_url from media_mappings where bridged_matrix_media_id = %s",
                (media_id,),
            )
            record = await cursor.fetchone()
            redirect_url = record[0]

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


# Only exclude headers that are strictly hop-by-hop connection details.
# We KEEL Content-Length and Content-Encoding now.
EXCLUDED_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",  # We let FastAPI manage chunking if needed
    "upgrade",
    "host",
    "server",
    "report-to",
    "set-cookie"
}


@app.get("/matrix-proxy/{media_id}/{file_name}")
async def matrix_proxy(media_id: str, file_name: str):
    # Check if the bridge is logged in
    if not matrix_side or not matrix_side.access_token:
        return Response(status_code=503, content="Bridge not logged in yet")

    # Get the homeserver base URL
    homeserver_url = login['matrix']['domain']
    if not homeserver_url.startswith("http"):
        homeserver_url = f"https://{homeserver_url}"

    # Lookup the real Matrix media ID
    async with db_pool.connection() as conn:
        async with conn.cursor() as cursor:
            # curr.
            await cursor.execute(
                "select original_matrix_media_id from media_mappings where bridged_xmpp_media_id = %s",
                (media_id,),
            )
            record = await cursor.fetchone()
            mxc = record[0]
    if mxc is None:
        return Response(status_code=404, content="Media not found")

    _, _, server_name, mxc_id = mxc.split('/')
    upstream_url = f"{homeserver_url}/_matrix/client/v1/media/download/{server_name}/{mxc_id}"

    # Build the request
    req = proxy_client.build_request(
        "GET",
        upstream_url,
        headers={"Authorization": f"Bearer {matrix_side.access_token}"},
        timeout=timeout_config
    )

    # Send request with stream=True
    resp = await proxy_client.send(req, stream=True)

    if resp.status_code in {301, 302, 307, 308}:
        # no good way to test without hosting a server so hopefully this works
        return Response(status_code=resp.status_code, content=resp.content)
    elif resp.status_code != 200:
        await resp.aclose()
        return Response(status_code=resp.status_code, content="Upstream Error")

    # Filter headers, but KEEP Content-Length
    forwarded_headers = {
        k: v for k, v in resp.headers.items()
        if k.lower() not in EXCLUDED_HEADERS
    }

    # aiter_raw() yields the exact bytes from the socket (compressed or not).
    # This guarantees the data matches the upstream Content-Length header.
    return StreamingResponse(
        resp.aiter_raw(),
        status_code=resp.status_code,
        headers=forwarded_headers,
        media_type=resp.headers.get("content-type"),
        background=resp.aclose
    )

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

tmp_muc_id = "chaos@group.pain.agency"  # TODO


async def message_callback(room: MatrixRoom, event: RoomMessageText) -> None:

    # temporary until appservice is used
    if event.sender == login['matrix']['mxid']:
        return

    try:
        async with db_pool.connection() as conn:
            await conn.execute(
                "insert into media_mappings (matrix_message_id) values (%s)",
                (event.event_id,),
            )
        bridged_mx_eventid.add(event.event_id)
    except Exception as e:
        print(e)
        return

    assert xmpp_side, "xmpp_side should be defined before matrix side is connected"

    # hold onto events until they can be bridged
    await xmpp_side_started.wait()

    jid = f"{event.sender[1:].replace(':','_')}@{login['xmpp']['jid']}"

    new_matrix_nick = room.user_name(event.sender)

    if new_matrix_nick != cached_matrix_nick.get(event.sender) or not jid in bridged_jids or event.body.startswith("!join"):
        try:
            await xmpp_side.plugin['xep_0045'].join_muc(
                room=tmp_muc_id,
                nick=new_matrix_nick,
                pfrom=jid,
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
                    "body": f"Could not join puppet because of {type(e)} error:\n{e}",
                }
            )

        bridged_jids.add(jid)
        bridged_jnics.add(new_matrix_nick)

    mx_reply_to_id = event.source \
        .get('content', {}) \
        .get("m.relates_to", {}) \
        .get("m.in_reply_to", {}) \
        .get("event_id", None)

    result = None
    stanza_id = None
    content = None
    if mx_reply_to_id:
        try:
            async with db_pool.connection() as conn:
                cursor = await conn.execute(
                    "SELECT (xmpp_message_id, body) FROM media_mappings WHERE matrix_message_id = %s",
                    (mx_reply_to_id,)
                )

                result = (await cursor.fetchone())[0]

        except Exception as e:
            print(e)

    if result:
        if len(result) > 1:
            stanza_id, content, *_ = result
        else:
            stanza_id = result[0]

    if stanza_id:

        message: stanza.Message = xmpp_side['xep_0461'].make_reply(
            "chaos@group.pain.agency/asdf",
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


async def media_callback(room: MatrixRoom, event: RoomMessageMedia) -> None:

    # temporary until appservice is used
    if event.sender == login['matrix']['mxid']:
        return

    assert xmpp_side, "xmpp_side should be defined before matrix side is connected"

    # hold onto events until they can be bridged
    await xmpp_side_started.wait()

    jid = f"{event.sender[1:].replace(':','_')}@{login['xmpp']['jid']}"

    new_matrix_nick = room.user_name(event.sender)

    if new_matrix_nick != cached_matrix_nick.get(event.sender) or not jid in bridged_jids:
        try:
            await xmpp_side.plugin['xep_0045'].join_muc(
                room=tmp_muc_id,
                nick=new_matrix_nick,
                pfrom=jid,
            )
        except Exception as e:

            matrix_side.room_send(
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

        bridged_jids.add(jid)
        bridged_jnics.add(new_matrix_nick)
    media_id: str = str(uuid.uuid4())
    filename: str = event.body.split('/')[-1]
    try:
        async with db_pool.connection() as conn:
            await conn.execute(
                "insert into media_mappings (matrix_message_id, original_matrix_media_id, bridged_xmpp_media_id) values (%s, %s, %s)",
                (event.event_id, event.url, media_id),
            )
        bridged_mx_eventid.add(event.event_id)
    except Exception as e:
        print(e)
        return

    url: str = f"https://{login['http_domain']}/matrix-proxy/{media_id}/{filename}"

    mx_reply_to_id = event.source \
        .get('content', {}) \
        .get("m.relates_to", {}) \
        .get("m.in_reply_to", {}) \
        .get("event_id", None)

    result = None
    stanza_id = None
    content = None
    if mx_reply_to_id:
        try:
            async with db_pool.connection() as conn:
                cursor = await conn.execute(
                    "SELECT (xmpp_message_id, body) FROM media_mappings WHERE matrix_message_id = %s",
                    (mx_reply_to_id,)
                )

                result = (await cursor.fetchone())[0]

        except Exception as e:
            print(e)

    if result:
        if len(result) > 1:
            stanza_id, content, *_ = result
        else:
            stanza_id = result[0]

    if stanza_id:

        message: stanza.Message = xmpp_side['xep_0461'].make_reply(
            "chaos@group.pain.agency/asdf",
            stanza_id or "",
            fallback=content or "",
            mto=tmp_muc_id,
            mbody=url,
            mtype='groupchat',
            mfrom=jid,
        )

    else:

        message: stanza.Message = xmpp_side.make_message(
            mto=tmp_muc_id,
            mbody=url,
            mtype='groupchat',
            mfrom=jid,

        )

    message.set_id(event.event_id)

    # attach media tag
    # pylint: disable=invalid-sequence-index
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

        self.add_event_handler('session_start', self.start)
        self.add_event_handler("message", self.message)
        self.add_event_handler("message_error", self.message_error)

        # Register plugins
        self.register_plugin('xep_0030')  # disco
        self.register_plugin('xep_0004')
        self.register_plugin('xep_0060')
        self.register_plugin('xep_0199')
        self.register_plugin('xep_0045')
        self.register_plugin('xep_0461')
        self.register_plugin('xep_0428')
        self.register_plugin('xep_0363')
        # (Unique and Stable Stanza IDs)
        self.register_plugin('xep_0359')
        self.register_plugin('xep_0066')  # media

    async def start(self, _):
        self.send_presence()
        # pylint: disable=no-member # it is apart of slixmpp
        # await self.get_roster()
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
                        async with db_pool.connection() as conn:
                            await conn.execute(
                                "UPDATE media_mappings SET xmpp_message_id = %s WHERE matrix_message_id = %s",
                                (stanzaid, msg.get('id'))
                            )
                        bridged_mx_eventid.remove(msg.get('id', ''))
                    except Exception as e:
                        print(e)
                        return

                # ignore all puppets
                if msg_from.resource in bridged_jnics or msg_from.bare in bridged_jids:
                    try:
                        matrix_side.room_read_markers(
                            "!odwJFwanVTgIblSUtg:matrix.org", msg.get('id', ''), msg.get('id', ''))
                        return
                    except Exception as _:
                        return

                xmpp_replyto_id = msg.get('reply', {}).get('id')
                matrix_replyto_id = None
                result = None
                if xmpp_replyto_id:
                    try:
                        async with db_pool.connection() as conn:
                            cursor = await conn.execute(
                                "SELECT (matrix_message_id) FROM media_mappings WHERE xmpp_message_id = %s",
                                (xmpp_replyto_id,)
                            )

                            result = await cursor.fetchone()

                    except Exception as e:
                        print(e)

                if result:
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
                                "body": f"{msg['from']}:\n{body}",
                                "m.relates_to": {
                                    "m.in_reply_to": {
                                        "event_id": matrix_replyto_id
                                    },
                                },
                            }
                        )
                    except Exception as e:
                        print(e)
                        return

                    try:
                        async with db_pool.connection() as conn:
                            await conn.execute(
                                "insert into media_mappings (xmpp_message_id, matrix_message_id) values (%s, %s)",
                                (stanzaid, resp.event_id),
                            )
                    except Exception as e:
                        print(e)
                        return

                else:

                    # xmpp clients just get this information from the url so we have to add it
                    filename: str = url.split('/')[-1]
                    mime_type, _ = mimetypes.guess_type(filename)
                    main_type, _ = mime_type.split('/')

                    try:
                        resp: RoomSendResponse = await matrix_side.room_send(
                            room_id="!odwJFwanVTgIblSUtg:matrix.org",
                            message_type="m.room.message",
                            content={
                                "msgtype": "m.text",
                                "body": f"{msg['from']} sent a(n) {mime_type}",
                                "m.relates_to": {
                                    "m.in_reply_to": {
                                        "event_id": matrix_replyto_id
                                    },
                                },
                            },
                        )
                    except Exception as e:
                        print(e)
                        return

                    file_id = str(uuid.uuid4())

                    try:
                        async with db_pool.connection() as conn:
                            await conn.execute(
                                "insert into media_mappings (xmpp_message_id, original_xmpp_media_url, bridged_matrix_media_id) values (%s, %s, %s)",
                                (stanzaid, url, file_id),
                            )
                    except Exception as e:
                        print(e)
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
                        print(e)
                        return

                    try:
                        async with db_pool.connection() as conn:
                            await conn.execute(
                                "UPDATE media_mappings SET matrix_message_id = %s WHERE xmpp_message_id = %s",
                                (resp.event_id, stanzaid)
                            )
                    except Exception as e:
                        print(e)
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

    await db_pool.open()

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
            server.serve(),
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

        await db_pool.close()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Just catch the generic interrupt to suppress the ugly traceback
        pass
