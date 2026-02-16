# python stdlib
import asyncio
import mimetypes
import uuid
import secrets
import aiofiles

# make fetching shit work
import httpx

# webserver
import uvicorn
from fastapi import FastAPI, Response
from fastapi.responses import FileResponse, JSONResponse

# custom db class
from db import KoishiDB


class KoishiWebserver:
    def __init__(self, db: KoishiDB, domain: str):

        self.matrix_side = None  # TODO: fix this when its cleaned up into a nice class

        self.db: KoishiDB = db

        # Initialize a client for the proxy to use
        self.http_fetch_client = httpx.AsyncClient()
        app = FastAPI()
        self.app = app

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

            record = await db.get_original_xmpp_url(media_id)
            redirect_url = record[0] if record else None  # safe fallback

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
                f"{CRLF}{CRLF}"
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

        @app.head("/matrix-proxy/{media_id}/{file_name}")
        async def matrix_proxy_head(media_id: str, file_name: str):
            mxc, filepath, size = await db.get_matrix_mediapath(media_id)

            # if we dont know what original file this points to we have no way of
            if mxc is None:
                print(
                    f"Failed to get head request for id {media_id} as no record was found in database")
                return Response(status_code=404, content="No record of this file was found in our database")

            mime_type, _ = mimetypes.guess_type(file_name)

            # attempt to fetch missing data
            if not size:
                originating_server, matrix_media_id = mxc.split("/")[-2:]
                upstream_url = f"{domain}/_matrix/client/v1/media/download/{originating_server}/{matrix_media_id}"
                headers = {
                    "Authorization": f"Bearer {self.matrix_side.access_token}"}
                try:
                    upstream_response = await self.http_fetch_client.head(
                        upstream_url,
                        headers=headers,
                        follow_redirects=True
                    )

                    size = upstream_response.headers.get('Content-Length')

                    if not filepath:
                        await db.set_mtrx_media_size(size, mxc)

                except httpx.HTTPError as e:
                    print(
                        f"Failed to get head request for id {media_id} ({mxc}) due to upstream error\n{e}")
                    return Response(
                        content=None,
                        status_code=502,
                        headers={'X-Error': str(e)}
                    )

            return Response(
                content=None,
                status_code=200,
                headers={
                    'Content-Type': mime_type or "application/octet-stream",
                    **({'Content-Length': str(size)} if size is not None else {}),
                    'Cache-Control': 'public, max-age=31536000',
                }
            )

        await_file_download: dict[str, asyncio.Task] = {}

        @app.get("/matrix-proxy/{media_id}/{file_name}")
        async def matrix_proxy(media_id: str, file_name: str):
            # Check if the bridge is logged in
            if not self.matrix_side or not self.matrix_side.access_token:
                return Response(status_code=503, content="Bridge not logged in yet")

            # TODO: update last fetched date
            mxc, filepath, _ = await db.get_matrix_mediapath(media_id)

            # if we dont know what original file this points to we have no way of
            if mxc is None:
                print(
                    f"Failed to fullfill get request for id {media_id} ({mxc}) because no record was found")
                return Response(status_code=404, content="No record of this file was found in our database")

            # if theres a filepath already recorded in the db, we can just serve that and move on
            if not filepath:

                # Request Coalescing or In-Flight Deduping
                download_task: asyncio.Task | None = await_file_download.get(
                    media_id)
                if download_task is None:

                    download_task = asyncio.create_task(
                        self.download_matrix_media(
                            domain,
                            self.matrix_side.access_token,
                            mxc,
                            "./cache"  # TODO user defined cache path, in case they want to put it on second tier storage
                        )
                    )

                    await_file_download[media_id] = download_task

                    download_task.add_done_callback(
                        lambda _: await_file_download.pop(media_id, None)
                    )

                try:
                    filepath, _ = await download_task
                except httpx.HTTPStatusError as e:
                    e_str = f"Failed to fullfill get request for id {media_id} ({mxc}) due to upstream error \n {e.response.reason_phrase}"
                    print(e_str)
                    return JSONResponse(
                        status_code=e.response.status_code,
                        content={
                            "error": e_str,
                            "upstream_status": e.response.status_code
                        }
                    )

                except httpx.RequestError as e:
                    e_str = f"Failed to fullfill get request for id {media_id} ({mxc}) due to upstream error \n {e}"
                    print(e_str)
                    return JSONResponse(
                        status_code=523,
                        content={
                            "error": e_str
                        }
                    )

                except ValueError as e:
                    # File size errors become 413 Payload Too Large
                    e_str = f"Failed to fullfill get request for id {media_id} ({mxc}) due to too large of a response \n {e}"
                    print(e_str)
                    return JSONResponse(
                        status_code=413,
                        content={
                            "error": e_str
                        }
                    )
                except Exception as e:
                    e_str = str(e.with_traceback(None))
                    print(e_str)
                    return JSONResponse(
                        status_code=500,
                        content={
                            "error": e_str
                        }
                    )

            mime_type, _ = mimetypes.guess_type(file_name)

            return FileResponse(
                path=filepath,
                filename=file_name,
                media_type=mime_type or "application/octet-stream",
                headers={
                    # CORS: Allows browsers to access the resource from specific origins
                    # Use '*' so that webclients can access it
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'GET, OPTIONS',

                    # This line tells the browser: "It's okay to let JS see these headers"
                    'Access-Control-Expose-Headers': 'Content-Disposition, Content-Length',

                    # Caching: Long-term immutable cache
                    'Cache-Control': 'public, max-age=31536000, immutable',

                    # inline vs attachment:
                    # 'inline' attempts to display in browser; 'attachment' forces download.
                    "Content-Disposition": f'inline; filename="{file_name}"',

                    # Security: Prevents the browser from "sniffing" the MIME type
                    'X-Content-Type-Options': 'nosniff',
                }
            )

    def set_matrix_side(self, matrix_side) -> None:
        self.matrix_side = matrix_side

    async def download_matrix_media(self, matrix_host: str, token: str, mxc: str, cache_dir: str) -> tuple[str, int]:
        """
        Downloads matrix media to the local cache folder
        Args:
            matrix_host: base url for our matrix server to download media from
            Token: authentication token for downloading matrix media
            mxc: the mxc uri for media
            dir: directory to download media to
        Returns:
            tuple containing the filepath to the downloaded file and its size
        """

        originating_server, matrix_media_id = mxc.split("/")[-2:]
        upstream_url = f"{matrix_host}/_matrix/client/v1/media/download/{originating_server}/{matrix_media_id}"
        headers = {"Authorization": f"Bearer {token}"}

        filename = str(uuid.uuid4())
        filepath = f"{ cache_dir }{ '' if cache_dir.endswith('/') else '/' }{ filename }"

        downloaded_bytes = 0

        async with self.http_fetch_client.stream("GET", upstream_url, headers=headers) as response:
            response.raise_for_status()
            _ = int(response.headers["content-length"])
            async with aiofiles.open(filepath, "wb") as f:
                async for chunk in response.aiter_bytes(chunk_size=8192):
                    downloaded_bytes += len(chunk)
                    await f.write(chunk)

        await self.db.set_mtrx_media_cache_path(filepath, downloaded_bytes, mxc)

        return (filepath, downloaded_bytes)

    async def serve(self, port, log_level):
        # Configure Uvicorn manually
        config = uvicorn.Config(self.app, host="0.0.0.0",
                                port=port, log_level=log_level)
        server = uvicorn.Server(config)

        # run in this event loop
        await server.serve()
