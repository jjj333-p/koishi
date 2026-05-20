"""
Koishi Bridge software
Copyright 2026 Joseph Winkie <jjj333.p.1325@gmail.com>
Licensed as AGPL 3.0
Distributed as-is and without warranty
"""
import sys
import json
import logging
import asyncio

from slixmpp import JID

from db import KoishiDB
from webserver import KoishiWebserver
from xmpp import KoishiComponent
from matrix import KoishiMatrixClient
from chat import KoishiRoom

# get login details
with open('login.json', 'r', encoding='utf-8') as login_file:
    login = json.load(login_file)

# Parse mappings
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


async def main():
    # Setup Logging
    logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname)-8s %(message)s')

    # Initialize components inside async context
    db: KoishiDB = KoishiDB(
        conn_str=login['postgresql_conn'],
        min_connections=1,
        max_connections=3,
    )

    webserver: KoishiWebserver = KoishiWebserver(
        db, login['matrix']['domain'], login['http_domain'])

    xmpp_side: KoishiComponent = KoishiComponent(
        jid=login['xmpp']['jid'],
        secret=login['xmpp']['secret'],
        server=login['xmpp']['domain'],
        port=login['xmpp']['port'],
        display_name=login['vanity_name'],
    )

    matrix_side: KoishiMatrixClient = KoishiMatrixClient(
        homeserver=login['matrix']['domain'],
        mxid=login['matrix']['mxid'],
        password=login['matrix']['password']
    )

    # Set cross-references
    webserver.set_matrix_side(matrix_side)
    xmpp_side.set_matrix_side(matrix_side)

    # Create room handlers
    rooms: list[KoishiRoom] = []
    for mapping in login['bridge-mapping']:
        room = KoishiRoom(
            http_domain=login['http_domain'],
            mappingJSON=mapping,
            db=db,
            xmpp_side=xmpp_side,
            matrix_side=matrix_side
        )
        rooms.append(room)

        # Register with Matrix client for event routing
        matrix_side.register_room(mapping['matrix'], room)

    # Connect to database
    await db.connect()

    # Connect rooms (register event handlers and join rooms on both sides)
    async def connect_rooms():
        try:
            for r in rooms:
                await r.connect()
        except Exception as e:
            print(f"Failed to connect rooms: {e}")
            raise

    # Run all services concurrently
    try:
        await asyncio.gather(
            xmpp_side.connect(),
            matrix_side.connect_and_sync(),
            connect_rooms(),
            webserver.serve(port=4567, log_level="info"),
        )
    except Exception as e:
        print(f"Error in main: {e}")
        raise
    except asyncio.CancelledError:
        print("Tasks cancelled, shutting down...")
    finally:
        print("Disconnecting XMPP...")
        if xmpp_side:
            await xmpp_side.disconnect()
            print("XMPP Disconnected.")

        print("Disconnecting Matrix...")
        if matrix_side:
            await matrix_side.disconnect()
            print("Matrix Disconnected.")

        await db.close()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutdown requested...")
