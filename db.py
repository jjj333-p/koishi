"""
Koishi Bridge software
Copyright 2026 Joseph Winkie <jjj333.p.1325@gmail.com>
Licensed as AGPL 3.0
Distributed as-is and without warranty

This file contains the KoishiDB class which basically just tucks away the actual sql statements
n such that need to be executed by the connection pool. Only one table at this time, despite 
being called "media mappings" its basically the mapping of messages and all the relevant metadata.
It looks scary but not much else will be added. could be split to several tables but not much need
to at this time.

koishi=# \dt
               List of tables
 Schema |      Name      | Type  |  Owner   
--------+----------------+-------+----------
 public | media_mappings | table | postgres
(1 row)

koishi=# \d media_mappings
                                 Table "public.media_mappings"
          Column          |           Type           | Collation | Nullable |      Default      
--------------------------+--------------------------+-----------+----------+-------------------
 xmpp_message_id          | text                     |           |          | 
 matrix_message_id        | text                     |           |          | 
 bridged_matrix_media_id  | text                     |           |          | 
 original_matrix_media_id | text                     |           |          | 
 bridged_xmpp_media_id    | text                     |           |          | 
 original_xmpp_media_url  | text                     |           |          | 
 body                     | text                     |           |          | 
 path                     | text                     |           |          | 
 size                     | bigint                   |           |          | 
 filename                 | text                     |           |          | ''::text
 last_fetched_at          | timestamp with time zone |           |          | CURRENT_TIMESTAMP
 user_jid                 | text                     |           |          | 
 user_mxid                | text                     |           |          | 
Indexes:
    "idx_media_mappings_bridged_matrix_id" btree (bridged_matrix_media_id)
    "idx_media_mappings_bridged_xmpp_id" btree (bridged_xmpp_media_id)
    "idx_media_mappings_last_fetched" btree (last_fetched_at)
    "idx_media_mappings_matrix_id" btree (matrix_message_id)
    "idx_media_mappings_path_exists" btree (path) WHERE path IS NOT NULL
    "idx_media_mappings_xmpp_id" btree (xmpp_message_id)
    "idx_unique_matrix_id" UNIQUE, btree (matrix_message_id)
    "idx_unique_xmpp_id" UNIQUE, btree (xmpp_message_id)
Check constraints:
    "check_at_least_one_id" CHECK (xmpp_message_id IS NOT NULL OR matrix_message_id IS NOT NULL)
"""

# database
import psycopg_pool

# jid type for stringifying
from slixmpp import JID


class KoishiDB:
    """
    KoishiDB class basically just tucks away the actual sql statements n such
    that need to be executed by the connection pool. I have attempted to make function
    names obvious to what they contain.
    """

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

    async def connect(self) -> None:
        """
        tell the db pool to connect
        Takes no arguments, returns nothing
        """
        await self.db_pool.open()

    async def close(self) -> None:
        """
        tell the db pool to un-connect
        Takes no arguments, returns nothing
        """
        await self.db_pool.close()

    async def init_moderation_tables(self) -> None:
        """Create the tables used by moderation sync if they do not exist."""
        async with self.db_pool.connection() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS user_mappings (
                    matrix_room_id TEXT NOT NULL,
                    xmpp_room_jid TEXT NOT NULL,
                    matrix_user_id TEXT NOT NULL,
                    xmpp_puppet_jid TEXT NOT NULL,
                    xmpp_puppet_nick TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (matrix_room_id, xmpp_room_jid, matrix_user_id)
                )
                """
            )
            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_user_mappings_xmpp_puppet_jid
                ON user_mappings (xmpp_room_jid, xmpp_puppet_jid)
                """
            )
            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_user_mappings_xmpp_puppet_nick
                ON user_mappings (xmpp_room_jid, LOWER(xmpp_puppet_nick))
                WHERE xmpp_puppet_nick IS NOT NULL
                """
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS moderation_actions (
                    id BIGSERIAL PRIMARY KEY,
                    matrix_room_id TEXT NOT NULL,
                    xmpp_room_jid TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    action TEXT NOT NULL,
                    matrix_user_id TEXT,
                    xmpp_puppet_jid TEXT,
                    xmpp_puppet_nick TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMPTZ NOT NULL
                )
                """
            )
            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_moderation_actions_lookup
                ON moderation_actions (matrix_room_id, xmpp_room_jid, direction, action, matrix_user_id)
                """
            )
            await conn.execute(
                "DELETE FROM moderation_actions WHERE expires_at <= CURRENT_TIMESTAMP"
            )

    async def upsert_user_mapping(
        self,
        matrix_room_id: str,
        xmpp_room_jid: str,
        matrix_user_id: str,
        xmpp_puppet_jid: str,
        xmpp_puppet_nick: str | None,
    ) -> None:
        """Store or refresh a Matrix user <-> XMPP puppet mapping."""
        async with self.db_pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO user_mappings (
                    matrix_room_id, xmpp_room_jid, matrix_user_id,
                    xmpp_puppet_jid, xmpp_puppet_nick
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (matrix_room_id, xmpp_room_jid, matrix_user_id)
                DO UPDATE SET
                    xmpp_puppet_jid = EXCLUDED.xmpp_puppet_jid,
                    xmpp_puppet_nick = EXCLUDED.xmpp_puppet_nick,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (matrix_room_id, xmpp_room_jid, matrix_user_id, xmpp_puppet_jid, xmpp_puppet_nick),
            )

    async def get_mapping_by_matrix_user(
        self, matrix_room_id: str, xmpp_room_jid: str, matrix_user_id: str
    ) -> tuple[str, str | None] | None:
        """Return (xmpp_puppet_jid, xmpp_puppet_nick) for a Matrix user."""
        async with self.db_pool.connection() as conn:
            cursor = await conn.execute(
                """
                SELECT xmpp_puppet_jid, xmpp_puppet_nick
                FROM user_mappings
                WHERE matrix_room_id = %s
                  AND xmpp_room_jid = %s
                  AND matrix_user_id = %s
                """,
                (matrix_room_id, xmpp_room_jid, matrix_user_id),
            )
            return await cursor.fetchone()

    async def get_mapping_by_xmpp_user(
        self,
        matrix_room_id: str,
        xmpp_room_jid: str,
        xmpp_puppet_jid: str | None = None,
        xmpp_puppet_nick: str | None = None,
    ) -> tuple[str, str, str | None] | None:
        """Return (matrix_user_id, xmpp_puppet_jid, xmpp_puppet_nick) for a puppet JID or nick."""
        if not xmpp_puppet_jid and not xmpp_puppet_nick:
            return None

        clauses = []
        params = [matrix_room_id, xmpp_room_jid]
        if xmpp_puppet_jid:
            clauses.append("xmpp_puppet_jid = %s")
            params.append(xmpp_puppet_jid)
        if xmpp_puppet_nick:
            clauses.append("LOWER(xmpp_puppet_nick) = LOWER(%s)")
            params.append(xmpp_puppet_nick)

        async with self.db_pool.connection() as conn:
            cursor = await conn.execute(
                f"""
                SELECT matrix_user_id, xmpp_puppet_jid, xmpp_puppet_nick
                FROM user_mappings
                WHERE matrix_room_id = %s
                  AND xmpp_room_jid = %s
                  AND ({' OR '.join(clauses)})
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                tuple(params),
            )
            return await cursor.fetchone()


    async def get_mappings_for_room(
        self, matrix_room_id: str, xmpp_room_jid: str
    ) -> list[tuple[str, str, str | None]]:
        """Return all known Matrix user <-> XMPP puppet mappings for a room."""
        async with self.db_pool.connection() as conn:
            cursor = await conn.execute(
                """
                SELECT matrix_user_id, xmpp_puppet_jid, xmpp_puppet_nick
                FROM user_mappings
                WHERE matrix_room_id = %s
                  AND xmpp_room_jid = %s
                """,
                (matrix_room_id, xmpp_room_jid),
            )
            return await cursor.fetchall()

    async def insert_moderation_action(
        self,
        matrix_room_id: str,
        xmpp_room_jid: str,
        direction: str,
        action: str,
        ttl_seconds: int,
        matrix_user_id: str | None = None,
        xmpp_puppet_jid: str | None = None,
        xmpp_puppet_nick: str | None = None,
    ) -> None:
        """Remember a moderation action briefly so the bridged echo can be ignored."""
        async with self.db_pool.connection() as conn:
            await conn.execute(
                "DELETE FROM moderation_actions WHERE expires_at <= CURRENT_TIMESTAMP"
            )
            await conn.execute(
                """
                INSERT INTO moderation_actions (
                    matrix_room_id, xmpp_room_jid, direction, action,
                    matrix_user_id, xmpp_puppet_jid, xmpp_puppet_nick, expires_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP + (%s * INTERVAL '1 second'))
                """,
                (
                    matrix_room_id, xmpp_room_jid, direction, action,
                    matrix_user_id, xmpp_puppet_jid, xmpp_puppet_nick, ttl_seconds,
                ),
            )

    async def consume_moderation_action(
        self,
        matrix_room_id: str,
        xmpp_room_jid: str,
        direction: str,
        action: str,
        matrix_user_id: str | None = None,
        xmpp_puppet_jid: str | None = None,
        xmpp_puppet_nick: str | None = None,
    ) -> bool:
        """Return True and delete a pending moderation action if it matches."""
        clauses = []
        params = [matrix_room_id, xmpp_room_jid, direction, action]

        if matrix_user_id:
            clauses.append("matrix_user_id = %s")
            params.append(matrix_user_id)
        if xmpp_puppet_jid:
            clauses.append("xmpp_puppet_jid = %s")
            params.append(xmpp_puppet_jid)
        if xmpp_puppet_nick:
            clauses.append("LOWER(xmpp_puppet_nick) = LOWER(%s)")
            params.append(xmpp_puppet_nick)

        if not clauses:
            return False

        async with self.db_pool.connection() as conn:
            await conn.execute(
                "DELETE FROM moderation_actions WHERE expires_at <= CURRENT_TIMESTAMP"
            )
            cursor = await conn.execute(
                f"""
                DELETE FROM moderation_actions
                WHERE id IN (
                    SELECT id
                    FROM moderation_actions
                    WHERE matrix_room_id = %s
                      AND xmpp_room_jid = %s
                      AND direction = %s
                      AND action = %s
                      AND expires_at > CURRENT_TIMESTAMP
                      AND ({' OR '.join(clauses)})
                    ORDER BY created_at DESC
                    LIMIT 1
                )
                RETURNING id
                """,
                tuple(params),
            )
            return await cursor.fetchone() is not None

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

    async def get_matrix_mediapath(
        self, xmpp_media_id: str
    ) -> tuple[str | None, str | None, int | None]:
        """
        Gets the original MXC URI and the local cache path for a given ID.
        Args:
            xmpp_media_id: UUID assigned to the bridged media.
        Returns:
            A tuple of (mxc_uri, local_path, file_size). All can be None if not found. Guaranteed to be at least 3 values
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

        if not fetch:
            return (None, None, None)

        if len(fetch) != 3:
            raise ValueError(
                f"expected response from database containing 3 values, got length {len(fetch)}: {str(fetch)}")
        return fetch

    async def set_mtrx_media_cache_path(self, filepath: str, downloaded_bytes: int, mxc: str):
        """
        UPDATE media_mappings SET path = filepath, size = downloaded_bytes WHERE original_matrix_media_id = mxc
        """
        async with self.db_pool.connection() as conn:
            await conn.execute(
                "UPDATE media_mappings SET path = %s, size = %s WHERE original_matrix_media_id = %s",
                (filepath, downloaded_bytes, mxc),
            )

    async def set_mtrx_media_size(self, downloaded_bytes: int, mxc: str):
        """
        UPDATE media_mappings SET size = downlaoded_bytes WHERE original_matrix_media_id = mxc
        """
        async with self.db_pool.connection() as conn:
            await conn.execute(
                "UPDATE media_mappings SET size = %s WHERE original_matrix_media_id = %s",
                (downloaded_bytes, mxc),
            )

    async def insert_msg_from_mtrx(self, event_id: str, body: str, jid: JID, mxid: str) -> None:
        """
        insert into media_mappings (matrix_message_id, body, user_jid, user_mxid) values (event_id, body, jid, mxid)
        """
        async with self.db_pool.connection() as conn:
            await conn.execute(
                "insert into media_mappings (matrix_message_id, body, user_jid, user_mxid) values (%s, %s, %s, %s)",
                (event_id, body, str(jid), mxid),
            )

    async def insert_media_msg_from_mtrx(self, event_id: str, mxc: str, xmpp_media_id: str, body: str, filename: str, jid: JID, mxid: str) -> None:
        """
        insert into media_mappings (
            matrix_message_id,
            original_matrix_media_id,
            bridged_xmpp_media_id,
            body,
            filename,
            user_jid,
            user_mxid
        ) values (event_id, mxc, xmpp_media_id,
                 body, filename, jid, mxid)
        """
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
        """
        SELECT xmpp_message_id, user_jid, body FROM media_mappings 
        WHERE matrix_message_id = mx_reply_to_id
        """
        async with self.db_pool.connection() as conn:
            cursor = await conn.execute(
                """
                SELECT xmpp_message_id, user_jid, body FROM media_mappings 
                WHERE matrix_message_id = %s
                """,
                (mx_reply_to_id,)
            )

            return await cursor.fetchone()

    async def get_matrix_reply_data(
        self, xmpp_stanza_id: str
    ) -> tuple[str | None, str | None] | None:
        """
        "SELECT matrix_message_id, user_mxid FROM media_mappings WHERE xmpp_message_id = xmpp_stanza_id"
        """
        async with self.db_pool.connection() as conn:
            cursor = await conn.execute(
                "SELECT matrix_message_id, user_mxid FROM media_mappings WHERE xmpp_message_id = %s",
                (xmpp_stanza_id,)
            )

            return await cursor.fetchone()

    async def set_xmpp_stanzaid(self, stanzaid: str, mtrx_id: str) -> None:
        """
        "UPDATE media_mappings SET xmpp_message_id = stanzaid WHERE matrix_message_id = mtrx_id"
        """
        async with self.db_pool.connection() as conn:
            await conn.execute(
                "UPDATE media_mappings SET xmpp_message_id = %s WHERE matrix_message_id = %s",
                (stanzaid, mtrx_id)
            )

    async def set_mtrx_eventid(self, event_id: str, stanzaid: str) -> None:
        """
        UPDATE media_mappings SET matrix_message_id = event_id WHERE xmpp_message_id = stanzaid
        """
        async with self.db_pool.connection() as conn:
            await conn.execute(
                "UPDATE media_mappings SET matrix_message_id = %s WHERE xmpp_message_id = %s",
                (event_id, stanzaid)
            )

    async def insert_message_mapping(self, stanzaid: str, event_id: str, body: str, jid: JID) -> None:
        """
        insert into media_mappings (xmpp_message_id, matrix_message_id, body, user_jid)
        values (stanzaid, event_id, body, jid)
        """
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
        """
        insert into media_mappings 
        (xmpp_message_id, original_xmpp_media_url, bridged_matrix_media_id, body, user_jid)
        values (stanzaid, url, file_id, body, jid)
        """
        async with self.db_pool.connection() as conn:
            await conn.execute(
                "insert into media_mappings (xmpp_message_id, original_xmpp_media_url, bridged_matrix_media_id, body, user_jid) values (%s, %s, %s, %s, %s)",
                (stanzaid, url, file_id, body, str(jid)),
            )

    async def delete_media(self, stanza_id: str = None, event_id: str = None) -> dict | None:
        """
        Deletes a record based on xmpp_message_id or matrix_message_id.
        Returns metadata for cleanup, or None if not found.
        """
        if not stanza_id and not event_id:
            raise ValueError(
                "You must provide either a stanza_id or an event_id.")

        # Determine search criteria based on your schema
        search_col = "xmpp_message_id" if stanza_id else "matrix_message_id"
        search_val = stanza_id if stanza_id else event_id

        async with self.db_pool.connection() as conn:
            async with conn.cursor() as cursor:
                # Fetch the data before deleting (Postgres allows RETURNING,
                # but we fetch first to ensure we have the 'other' ID)
                query = f"""
                    SELECT xmpp_message_id, matrix_message_id, path 
                    FROM media_mappings 
                    WHERE {search_col} = %s
                """
                await cursor.execute(query, (search_val,))
                row = await cursor.fetchone()

                if not row:
                    return None

                # Perform the deletion
                await cursor.execute(
                    f"DELETE FROM media_mappings WHERE {search_col} = %s",
                    (search_val,)
                )

                # Return the metadata
                # TODO: tuple may be more efficient 🤔
                return {
                    "stanza_id": row[0],
                    "event_id": row[1],
                    "path": row[2]
                }
