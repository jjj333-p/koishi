"""
Koishi Bridge software
Copyright 2026 Joseph Winkie <jjj333.p.1325@gmail.com>
Licensed as AGPL 3.0
Distributed as-is and without warranty
"""
import asyncio
import os
import platform

from collections.abc import Callable
# xmpp library
from slixmpp.componentxmpp import ComponentXMPP

from util import get_git_info


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

        self.moderated_message_handlers: dict[str, Callable] = {}

        self.started: asyncio.Event = asyncio.Event()

        # Store the version so we only fetch it once
        # Dynamically fetch both the version and the fork's URL
        self.git_version, self.repo_url = get_git_info()

        # Register event handlers
        self.add_event_handler('session_start', self.start)

        self.add_event_handler("moderated_message",
                               self.dispatch_moderated_message)

        self.add_event_handler("vcard_get", self.handle_vcard_get)

        self.status_msg = f"Koishi Bridge v{self.git_version} | Source: {self.repo_url}"

        # Register plugins
        self.register_plugin('xep_0092')  # Software Version
        self.register_plugin('xep_0054')  # vCard-temp
        self.register_plugin('xep_0030')  # Service Discovery (Disco)
        self.register_plugin('xep_0004')  # Data Forms
        self.register_plugin('xep_0060')  # Publish-Subscribe
        self.register_plugin('xep_0199')  # XMPP Ping
        self.register_plugin('xep_0045')  # Multi-User Chat (MUC)
        self.register_plugin('xep_0461')  # Message Replies
        self.register_plugin('xep_0428')  # Fallback Indication
        self.register_plugin('xep_0359')  # Unique and Stable Stanza IDs
        self.register_plugin('xep_0421')  # Occupant ID for MUC
        self.register_plugin('xep_0066')  # Out of Band Data
        self.register_plugin('xep_0333')  # Chat Markers (displayed indicator)
        self.register_plugin('xep_0422')  # Message Fastening
        self.register_plugin('xep_0424')  # Message Retraction
        self.register_plugin('xep_0425')  # Message Moderation
        self.register_plugin('xep_0308')  # Message corrections
        self.register_plugin('xep_0012')  # Last Activity / Uptime

        self.matrix_side = None

    def set_matrix_side(self, matrix_side) -> None:
        """Set reference to Matrix client"""
        self.matrix_side = matrix_side

    async def start(self, _):
        """Handle session start - set up disco and send presence"""

        # Set up software version response (XEP-0092)
        self['xep_0092'].software_name = f"Koishi Matrix Bridge (Source: {self.repo_url})"
        self['xep_0092'].version = self.git_version
        self['xep_0092'].os = f"{platform.system()} ({os.name})"

        # Set up service discovery
        self['xep_0030'].add_identity(
            category='gateway',
            itype='matrix',
            name='Koishi Matrix Bridge'
        )

        # Register disco features
        self['xep_0030'].add_feature('http://jabber.org/protocol/disco#info')
        self['xep_0030'].add_feature('http://jabber.org/protocol/disco#items')
        self['xep_0030'].add_feature('jabber:iq:last')  # Add this line!

        # Initialize uptime tracking
        self['xep_0012'].set_last_activity(
            jid=self.boundjid.bare,
            seconds=0,
            status=None
        )

        self.send_presence(pstatus=self.status_msg)

        # Set started flag so rooms can begin joining
        self.started.set()

        print(f"XMPP Component started as {self.boundjid.bare}")

    def handle_vcard_get(self, iq):
        """
        Dynamically generate vCards for any JID on this component.
        Injects the AGPL source link into the profile description.
        """
        reply = iq.reply()

        # Use the plugin's stanza interface to populate the vCard
        reply['vcard_temp']['DESC'] = f"Bridged by Koishi (v{self.git_version})\nSource: {self.repo_url}"
        reply['vcard_temp']['URL'] = self.repo_url

        reply.send()

    async def dispatch_moderated_message(self, msg):
        muc = msg["from"].bare
        if muc in self.moderated_message_handlers:
            await self.moderated_message_handlers[muc](msg)
