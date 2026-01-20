# database
import psycopg_pool

# jid type for stringifying
from slixmpp import JID


class KoishiDB:
    def __init__(
        self,
        conn_str: str,
        min_connections: int = 1,
        max_connections: int = 3
    ):
        # create db pool, do not connect now because it has to be inside the async loop (what is python)
        self.db_pool = psycopg_pool.AsyncConnectionPool(
            conninfo=conn_str,
            min_size=min_connections,
            max_size=max_connections,
            open=False,
        )

    async def get_original_xmpp_url(self, media_id: str) -> tuple[str | None] | None:
        """
        Lookup the opaque bridged mxc uri's media id, and return the original \
        http url to redirect to
        Args:
            media_id: opaque UUID assigned to the bridged media to look up in the db
        Returns:
            An optional tuple containing the original media url if a record is found. \
            This media url may be None.
        """
        async with self.db_pool.connection() as conn:
            async with conn.cursor() as cursor:
                # curr.
                await cursor.execute(
                    "select original_xmpp_media_url from media_mappings where bridged_matrix_media_id = %s",
                    (media_id,),
                )
                return await cursor.fetchone()

    async def connect(self) -> None:
        await self.db_pool.open()

    async def close(self) -> None:
        await self.db_pool.close()

    async def get_matrix_mediapath(
        self, xmpp_media_id: str
    ) -> tuple[str | None, str | None, int | None]:
        """
        Gets the original MXC URI and the local cache path for a given ID.
        Args:
            xmpp_media_id: UUID assigned to the bridged media.
        Returns:
            A tuple of (mxc_uri, local_path, file_size). All can be None if not found.
        """
        async with self.db_pool.connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                UPDATE media_mappings
                SET last_fetched_at = CURRENT_TIMESTAMP
                WHERE bridged_xmpp_media_id = %s
                RETURNING original_matrix_media_id, path, size
                """,
                    (xmpp_media_id,),
                )
                # there should only be one result available
                fetch = await cursor.fetchone()

        if len(fetch) != 3:
            raise ValueError(
                f"expected response from database containing 3 values, got length {len(fetch)}: {str(fetch)}")
        return fetch

    async def set_mtrx_media_cache_path(self, filepath: str, downloaded_bytes: int, mxc: str):
        async with self.db_pool.connection() as conn:
            await conn.execute(
                "UPDATE media_mappings SET path = %s, size = %s WHERE original_matrix_media_id = %s",
                (filepath, downloaded_bytes, mxc),
            )

    async def set_mtrx_media_size(self, downloaded_bytes: int, mxc: str):
        async with self.db_pool.connection() as conn:
            await conn.execute(
                "UPDATE media_mappings SET size = %s WHERE original_matrix_media_id = %s",
                (downloaded_bytes, mxc),
            )

    async def insert_msg_from_mtrx(self, event_id: str, body: str, jid: JID, mxid: str) -> None:
        async with self.db_pool.connection() as conn:
            await conn.execute(
                "insert into media_mappings (matrix_message_id, body, user_jid, user_mxid) values (%s, %s, %s, %s)",
                (event_id, body, str(jid), mxid),
            )

    async def insert_media_msg_from_mtrx(self, event_id: str, mxc: str, xmpp_media_id: str, body: str, filename: str, jid: JID, mxid: str) -> None:
        async with self.db_pool.connection() as conn:
            await conn.execute(
                """
                insert into media_mappings (
                    matrix_message_id,
                    original_matrix_media_id,
                    bridged_xmpp_media_id,
                    body,
                    filename,
                    user_jid,
                    user_mxid
                ) values (%s, %s, %s, %s, %s, %s, %s)
                """,
                (event_id, mxc, xmpp_media_id,
                 body, filename, str(jid), mxid),
            )

    async def get_xmpp_reply_data(
        self, mx_reply_to_id
    ) -> tuple[str | None, str | None, str | None] | None:
        async with self.db_pool.connection() as conn:
            cursor = await conn.execute(
                "SELECT xmpp_message_id, user_jid, body FROM media_mappings WHERE matrix_message_id = %s",
                (mx_reply_to_id,)
            )

            return await cursor.fetchone()

    async def get_matrix_reply_data(
        self, xmpp_stanza_id: str
    ) -> tuple[str | None, str | None] | None:
        async with self.db_pool.connection() as conn:
            cursor = await conn.execute(
                "SELECT matrix_message_id, user_mxid FROM media_mappings WHERE xmpp_message_id = %s",
                (xmpp_stanza_id,)
            )

            return await cursor.fetchone()

    async def set_xmpp_stanzaid(self, stanzaid: str, mtrx_id: str) -> None:
        async with self.db_pool.connection() as conn:
            await conn.execute(
                "UPDATE media_mappings SET xmpp_message_id = %s WHERE matrix_message_id = %s",
                (stanzaid, mtrx_id)
            )

    async def set_mtrx_eventid(self, event_id: str, stanzaid: str) -> None:
        async with self.db_pool.connection() as conn:
            await conn.execute(
                "UPDATE media_mappings SET matrix_message_id = %s WHERE xmpp_message_id = %s",
                (event_id, stanzaid)
            )

    async def insert_message_mapping(self, stanzaid: str, event_id: str, body: str, jid: JID) -> None:
        async with self.db_pool.connection() as conn:
            await conn.execute(
                "insert into media_mappings (xmpp_message_id, matrix_message_id, body, user_jid) values (%s, %s, %s, %s)",
                (stanzaid, event_id, body, str(jid)),
            )

    async def insert_xmpp_media_message_mapping(
        self,
        stanzaid: str,
        url: str,
        file_id: str,
        body: str,
        jid: JID
    ) -> None:
        async with self.db_pool.connection() as conn:
            await conn.execute(
                "insert into media_mappings (xmpp_message_id, original_xmpp_media_url, bridged_matrix_media_id, body, user_jid) values (%s, %s, %s, %s, %s)",
                (stanzaid, url, file_id, body, str(jid)),
            )
