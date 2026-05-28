"""
Koishi Bridge software
Copyright 2026 Joseph Winkie <jjj333.p.1325@gmail.com>
Licensed as AGPL 3.0
Distributed as-is and without warranty
"""
import html
import re
from html.parser import HTMLParser
from pipe import Pipe


def xep0393_to_matrix_html(text: str) -> str:
    """
    Convert a subset of XMPP XEP-0393 Message Styling to Matrix pseudo-HTML.

    Supported:
      *bold*           -> *<strong>bold</strong>*
      _italic_         -> _<em>italic</em>_
      ~strike~         -> ~<del>strike</del>~
      `code`           -> `<code>code</code>`
      ```code block``` -> <pre><code>code block</code></pre>
      > quote          -> <blockquote>quote</blockquote>

    The original inline styling directives are intentionally preserved.
    """

    def convert_inline(s: str) -> str:
        """
        Convert inline XEP-0393 spans within a single block.

        XEP-0393 span rules handled here:
          - spans do not cross block boundaries because this function receives one line/block
          - opening directive must be at block start, after whitespace, or after a different
            opening styling directive
          - opening directive must not be followed by whitespace
          - closing directive must not be preceded by whitespace
          - matched spans must contain text between directives
          - matching is lazy
          - invalid directive-looking characters are treated as literal text
        """
        markers = {
            "*": "strong",
            "_": "em",
            "~": "del",
            "`": "code",
        }
        source_open_positions = {}

        def is_valid_open(pos: int, marker: str) -> bool:
            if pos + 1 >= len(s):
                return False

            if s[pos + 1].isspace():
                return False

            if pos == 0:
                return True

            previous = s[pos - 1]

            if previous.isspace():
                return True

            previous_open_marker = source_open_positions.get(pos - 1)
            return previous_open_marker is not None and previous_open_marker != marker

        def is_valid_close(pos: int, start: int) -> bool:
            if pos <= start + 1:
                return False

            return not s[pos - 1].isspace()

        def find_closing(start: int, end: int, marker: str) -> int | None:
            pos = start + 1

            while pos < end:
                if s[pos] == marker and is_valid_close(pos, start):
                    return pos

                pos += 1

            return None

        def parse_range(
            start: int,
            end: int,
            disabled_markers: set[str] | None = None,
        ) -> str:
            if disabled_markers is None:
                disabled_markers = set()

            out = []
            i = start

            while i < end:
                ch = s[i]

                if ch in markers and ch not in disabled_markers and is_valid_open(i, ch):
                    close = find_closing(i, end, ch)

                    if close is not None:
                        tag = markers[ch]
                        source_open_positions[i] = ch

                        if ch == "`":
                            content = html.escape(s[i + 1:close], quote=False)
                        else:
                            content = parse_range(
                                i + 1,
                                close,
                                disabled_markers | {ch},
                            )

                        escaped_marker = html.escape(ch, quote=False)
                        out.append(
                            f"{escaped_marker}<{tag}>{content}</{tag}>{escaped_marker}"
                        )
                        i = close + 1
                        continue

                out.append(html.escape(ch, quote=False))
                i += 1

            return "".join(out)

        return parse_range(0, len(s))

    def render_plain(part: str) -> str:
        # Split out fenced code blocks after quote prefixes have already been removed.
        parts = re.split(r"(```(?:\n)?[\s\S]*?```)", part)
        output = []

        for subpart in parts:
            if subpart.startswith("```") and subpart.endswith("```"):
                code = subpart[3:-3]
                if code.startswith("\n"):
                    code = code[1:]
                if code.endswith("\n"):
                    code = code[:-1]

                output.append(
                    f"<pre><code>{html.escape(code, quote=False)}</code></pre>")
                continue

            lines = subpart.splitlines(keepends=False)

            for i, line in enumerate(lines):
                output.append(convert_inline(line))

                if i < len(lines) - 1:
                    output.append("<br>")

        return "".join(output)

    lines = text.splitlines(keepends=False)
    output = []
    i = 0

    while i < len(lines):
        line = lines[i]

        if line.startswith(">"):
            quote_lines = []

            while i < len(lines) and lines[i].startswith(">"):
                quote_line = lines[i][1:]
                if quote_line.startswith(" "):
                    quote_line = quote_line[1:]
                quote_lines.append(quote_line)
                i += 1

            output.append(
                "<blockquote>"
                + render_plain("\n".join(quote_lines))
                + "</blockquote>"
            )

            if i < len(lines):
                output.append("<br>")

                if lines[i] == "":
                    i += 1

            continue

        plain_lines = []

        while i < len(lines) and not lines[i].startswith(">"):
            plain_lines.append(lines[i])
            i += 1

        output.append(render_plain("\n".join(plain_lines)))

        if i < len(lines):
            output.append("<br>")

    return "".join(output)


@Pipe
def to_iter(msg: str):
    """
    pass 0: turn the message into an iterator over characters
    https://git.qwertydotpl.us/qwerty/patches/src/commit/a1c1b2cf10179bd9e80290d71ced429905573b30/convert-matrix-formatted_body-to-xmpp-xep0393.py#L11
    """
    yield from msg


@Pipe
def tokenize(f):
    """
    pass 1: turn characters into tokens
    this turns each tag into a token by searching for < followed by n chars followed by >.
    https://git.qwertydotpl.us/qwerty/patches/src/commit/a1c1b2cf10179bd9e80290d71ced429905573b30/convert-matrix-formatted_body-to-xmpp-xep0393.py#L15
    """
    tok = ''
    while True:
        try:
            c = next(f)
        except StopIteration:
            yield from tok
            break
        if c == '<':  # start of tag
            yield from tok  # what we thought was a single token was not, in fact, a single token
            tok = '<'
        else:
            tok += c
            if len(tok) == 1 or c == '>':  # either we aren't building a token right now (if we were, tok would start with <), or we just finished building a token (by appending a >)
                yield tok
                tok = ''


@Pipe
def mark_self_closing(f):
    """https://git.qwertydotpl.us/qwerty/patches/src/commit/a1c1b2cf10179bd9e80290d71ced429905573b30/convert-matrix-formatted_body-to-xmpp-xep0393.py#L34"""
    while True:
        try:
            tok = next(f)
        except StopIteration:
            break

        match tok:
            case '<br>':
                yield '<br/>'
            case _:
                yield tok


@Pipe
def fix_tag_matching(f):
    """
    pass 3: use stack to match tags and erase unmatched closing tags
    this makes sure tags match up, so e.g. <a><b>123</a></c> becomes <a><b>123</b></>
    https://git.qwertydotpl.us/qwerty/patches/src/commit/a1c1b2cf10179bd9e80290d71ced429905573b30/convert-matrix-formatted_body-to-xmpp-xep0393.py#L48
    """
    stack = []
    while True:
        try:
            tok = next(f)
        except StopIteration:
            while len(stack) > 0:
                yield f'</{stack.pop()}>'
            break
        if len(tok) == 1:  # char
            yield tok  # skip
        elif tok[-2] == '/':  # self-closing tag
            yield tok  # skip
        elif tok[1] == '/':  # closing tag
            if tok[2:-1] in stack:  # exists in stack, unravel until we hit it
                while True:
                    popped = stack.pop()
                    yield f'</{popped}>'
                    if popped == tok[2:-1]:
                        # we hit our tag, move on
                        break
            # else drop
        else:  # opening tag
            yield tok
            stack.append(tok[1:-1])  # push


@Pipe
def drop_redundant(f):
    """
    pass 4: drop redundant/irrelevant tags
    this strips out redundancies in e.g. <em><em>abc</em> <em>def</em></em> (which then becomes <em>abc def</em>)
    https://git.qwertydotpl.us/qwerty/patches/src/commit/a1c1b2cf10179bd9e80290d71ced429905573b30/convert-matrix-formatted_body-to-xmpp-xep0393.py#L76
    """
    stack = []
    while True:
        try:
            tok = next(f)
        except StopIteration:
            break
        if len(tok) == 1:  # char
            yield tok  # skip
        elif tok[-2] == '/':  # self-closing tag
            yield tok  # skip
        elif tok[1] == '/':  # closing tag
            if stack.pop() != '.':  # not irrelevant
                yield tok  # output
        else:  # opening tag
            match tok[1:-1]:
                case 'em' | 'strong' | 'del':  # cannot be applied more than once
                    if tok[1:-1] in stack:
                        stack.append('.')
                    else:
                        stack.append(tok[1:-1])
                        yield tok
                # we drop code in pre (and vice versa) because it's easier to treat code as inline and pre as meaning codeblock (does anybody even use <pre> for anything other than code, anyway?)
                case 'pre' | 'code':
                    if 'pre' in stack or 'code' in stack:
                        stack.append('.')
                    else:
                        stack.append(tok[1:-1])
                        yield tok
                case _:
                    stack.append(tok[1:-1])
                    yield tok


@Pipe
def drop_bad_blocks(f):
    """
    drop bad blocks (e.g. mx-reply)
    https://git.qwertydotpl.us/qwerty/patches/src/commit/a1c1b2cf10179bd9e80290d71ced429905573b30/convert-matrix-formatted_body-to-xmpp-xep0393.py#L110
    """
    while True:
        try:
            tok = next(f)
        except StopIteration:
            break
        if len(tok) == 1:  # char
            yield tok  # skip
        elif tok[-2] == '/':  # self-closing tag
            yield tok  # skip
        elif tok[1] == '/':  # closing tag
            yield tok  # output
        else:  # opening tag
            match tok[1:-1]:
                case 'mx-reply':  # drop block
                    while True:
                        try:
                            tok = next(f)
                        except StopIteration:
                            break
                        if tok == f'</{tok[1:-1]}>':
                            break
                case _:
                    yield tok


@Pipe
def naive_convert(f):
    """
    semi-naive conversion to xmpp format
    https://git.qwertydotpl.us/qwerty/patches/src/commit/a1c1b2cf10179bd9e80290d71ced429905573b30/convert-matrix-formatted_body-to-xmpp-xep0393.py#L136
    """

    quote_level = 0
    unformat = False
    while True:
        try:
            tok = next(f)
        except StopIteration:
            break
        match tok:
            case '\n':
                yield '\n' + '> ' * quote_level
            # this handles characters when inside a code block
            case _ if unformat and len(tok) == 1:
                yield tok
            # handle single special characters:
            case '*':
                yield r'\*'
            case '_':
                yield r'\_'
            case '~':
                yield r'\~'
            case '`':
                yield r'\`'
            case '>':
                yield r'\>'
            # handle formatting toggles (this only works if we fixTagMatching and dropRedundant):
            case '<em>' | '</em>':
                yield '_'
            case '<strong>' | '</strong>':
                yield '*'
            case '<del>' | '</del>':
                yield '~'
            case '<code>' | '</code>':
                yield '`'
                unformat = not unformat
            case '<pre>' | '</pre>':
                yield '\n' + '> ' * quote_level + '```\n' + '> ' * quote_level
                unformat = not unformat
            # handle blockquotes:
            case '<blockquote>':
                quote_level += 1
                yield '\n' + '> ' * quote_level
            case '</blockquote>':
                quote_level -= 1
                yield '\n' + '> ' * quote_level
            # skip paragraph and line break tags:
            case '<p>' | '</p>' | '<br/>':
                pass
            # if you want to drop unknown tags, uncomment the following:
            # case _ if len(tok) > 0:
            # pass
            # directly copy anything we didn't cover
            case _:
                yield tok


def matrix_html_to_xep0393(message: str) -> str:
    """
    https://git.qwertydotpl.us/qwerty/patches/src/branch/main/convert-matrix-formatted_body-to-xmpp-xep0393.py#L150
    """
    return re.sub(
        '\n((> )*\n)+', '\n',
        "".join(
            message
            | to_iter
            | tokenize
            | mark_self_closing
            | fix_tag_matching
            | drop_redundant
            | drop_bad_blocks
            | naive_convert
        )
    )


if __name__ == "__main__":
    message = "<p>test <em>italic</em> <strong>bold</strong> <del>strikethrough</del> <code>code</code></p>\n<pre><code>code\n</code></pre>\n<blockquote>\n<p>quote<br>\n<code>code</code></p>\n</blockquote>"
    print(
        re.sub(
            '\n((> )*\n)+', '\n',
            "".join(
                message
                | to_iter
                | tokenize
                | mark_self_closing
                | fix_tag_matching
                | drop_redundant
                | drop_bad_blocks
                | naive_convert
            )
        )
    )
