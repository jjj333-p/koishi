"""
Koishi Bridge software
Copyright 2026 Joseph Winkie <jjj333.p.1325@gmail.com>
Licensed as AGPL 3.0
Distributed as-is and without warranty
"""

import string
import os
from slixmpp import JID, InvalidJID


def escape_nickname(muc_jid: JID, nickname: str) -> JID:
    """
    Attempts to parse a nickname as the resourcepart of a jid, re-encoding if necessary
    Args:
        muc_jid: JID - Jid of the muc, used to ensure that the full jid type returned can be used directly
        nickname: str - string resourcepart that should be encoded
    Returns:
        JID - the complete jid after necessary validation and encoding
    """
    jid = JID(muc_jid)

    try:
        jid.resource = nickname
    except InvalidJID:
        nickname = nickname.encode("punycode").decode()
        try:
            jid.resource = nickname
        except InvalidJID:
            # at this point there still might be control chars
            jid.resource = "".join(
                x for x in nickname if x in string.printable
            ) + f"-koishi-{hash(nickname)}"

    return jid


def rm_file(filepath: str):
    """
    syncronous function to delete file, should be run in an executor
    Args:
        filepath: string typed path to the file
    """

    if os.path.exists(filepath):
        os.remove(filepath)
        print(f"Deleted cached media: {filepath}")
