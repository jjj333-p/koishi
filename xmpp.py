"""
Koishi Bridge software
Copyright 2026 Joseph Winkie <jjj333.p.1325@gmail.com>
Licensed as AGPL 3.0
Distributed as-is and without warranty
"""
import asyncio

# xmpp library
from slixmpp.componentxmpp import ComponentXMPP


class KoishiComponent(ComponentXMPP):
    def __init__(
        self,
        jid: str,
        secret: str,
        server: str,
        port: int,
        display_name: str,
    ):
        # Get the current event loop before initializing ComponentXMPP
        loop = asyncio.get_event_loop()
        
        ComponentXMPP.__init__(self, jid, secret, server, port)
        
        # Ensure slixmpp uses the current event loop
        self.loop = loop

        self.display_name = display_name

        self.started: asyncio.Event = asyncio.Event()

        # Register event handlers
        self.add_event_handler('session_start', self.start)

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
        self.register_plugin('xep_0333')  # Chat Markers (displayed indicator)
        self.register_plugin('xep_0422')  # Message Fastening
        self.register_plugin('xep_0424')  # Message Retraction
        self.register_plugin('xep_0425')  # Message Moderation

        self.matrix_side = None

    def set_matrix_side(self, matrix_side) -> None:
        """Set reference to Matrix client"""
        self.matrix_side = matrix_side

    async def start(self, _):
        """Handle session start - set up disco and send presence"""
        # Set up service discovery
        self['xep_0030'].add_identity(
            category='gateway',
            itype='matrix',
            name='Koishi Matrix Bridge'
        )

        # Register disco features
        self['xep_0030'].add_feature('http://jabber.org/protocol/disco#info')
        self['xep_0030'].add_feature('http://jabber.org/protocol/disco#items')

        # Send initial presence
        self.send_presence()

        # Set started flag so rooms can begin joining
        self.started.set()

        print(f"XMPP Component started as {self.boundjid.bare}")
