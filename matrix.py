"""
Koishi Bridge software
Copyright 2026 Joseph Winkie <jjj333.p.1325@gmail.com>
Licensed as AGPL 3.0
Distributed as-is and without warranty
"""
import asyncio
from typing import Optional

# matrix library
from nio import AsyncClient, MatrixRoom, RoomMessageText, RoomMessageMedia, RoomMessageNotice, Receipt, ReceiptEvent, \
    RedactionEvent


class KoishiMatrixClient:
    def __init__(self, homeserver: str, mxid: str, password: str):
        self.homeserver = homeserver
        self.mxid = mxid
        self.password = password

        self.client: Optional[AsyncClient] = None
        self.connected = asyncio.Event()

        # Store background tasks
        self.background_tasks = set()

        # Room handlers
        self.room_handlers: dict[str, 'KoishiRoom'] = {}

    def register_room(self, room_id: str, room_handler):
        """Register a KoishiRoom handler for a specific Matrix room"""
        self.room_handlers[room_id] = room_handler

    async def _text_message_callback(self, room: MatrixRoom, event):
        """Route text messages to appropriate room handler"""
        handler = self.room_handlers.get(room.room_id)
        if handler:
            task = asyncio.create_task(handler.handle_matrix_text_message(room, event))
            self.background_tasks.add(task)
            task.add_done_callback(self.background_tasks.discard)

    async def _media_callback(self, room: MatrixRoom, event):
        """Route media messages to appropriate room handler"""
        handler = self.room_handlers.get(room.room_id)
        if handler:
            task = asyncio.create_task(handler.handle_matrix_media_message(room, event))
            self.background_tasks.add(task)
            task.add_done_callback(self.background_tasks.discard)

    async def _redaction_callback(self, room: MatrixRoom, event: RedactionEvent):
        """Route redactions to appropriate room handler"""
        handler = self.room_handlers.get(room.room_id)
        if handler:
            task = asyncio.create_task(handler.handle_matrix_redaction(room, event))
            self.background_tasks.add(task)
            task.add_done_callback(self.background_tasks.discard)

    async def _receipt_callback(self, room: MatrixRoom, events: ReceiptEvent):
        """Route receipts to appropriate room handler"""
        handler = self.room_handlers.get(room.room_id)
        if not handler:
            return

        for event in events.receipts:
            # only receipt type we care about
            if event.receipt_type != "m.read":
                continue

            # Skip bridge's own receipts
            if event.user_id == self.mxid:
                continue

            task = asyncio.create_task(handler.handle_matrix_receipt(room, event))
            self.background_tasks.add(task)
            task.add_done_callback(self.background_tasks.discard)

    async def connect_and_sync(self):
        """Connect to Matrix and sync forever"""
        self.client = AsyncClient(self.homeserver, self.mxid)

        # Register callbacks
        self.client.add_event_callback(
            self._text_message_callback,
            (RoomMessageText, RoomMessageNotice)
        )
        self.client.add_event_callback(
            self._media_callback,
            RoomMessageMedia
        )
        self.client.add_event_callback(
            self._redaction_callback,
            RedactionEvent
        )
        self.client.add_ephemeral_callback(
            self._receipt_callback,
            ReceiptEvent
        )

        # Login
        await self.client.login(self.password)

        # Initial sync
        await self.client.sync(timeout=30000)

        # Set connected flag so rooms can begin joining
        self.connected.set()

        # Sync forever
        await self.client.sync_forever(timeout=30000)

    async def disconnect(self):
        """Disconnect from Matrix"""
        if self.client:
            await self.client.close()

    # Proxy commonly used methods
    async def room_send(self, *args, **kwargs):
        if not self.client:
            raise RuntimeError("Matrix client not connected")
        return await self.client.room_send(*args, **kwargs)

    async def room_redact(self, *args, **kwargs):
        if not self.client:
            raise RuntimeError("Matrix client not connected")
        return await self.client.room_redact(*args, **kwargs)

    async def room_read_markers(self, *args, **kwargs):
        if not self.client:
            raise RuntimeError("Matrix client not connected")
        return await self.client.room_read_markers(*args, **kwargs)
    
    async def join(self, room_id: str):
        """Join a Matrix room"""
        if not self.client:
            raise RuntimeError("Matrix client not connected")
        return await self.client.join(room_id)

    def user_name(self, room_id: str, user_id: str) -> str:
        """Get display name for a user in a room"""
        if not self.client or not self.client.rooms.get(room_id):
            return user_id
        room = self.client.rooms[room_id]
        return room.user_name(user_id)