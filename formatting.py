"""
Koishi Bridge software
Copyright 2026 Joseph Winkie <jjj333.p.1325@gmail.com>
Licensed as AGPL 3.0
Distributed as-is and without warranty
"""
import html
import re
from html.parser import HTMLParser


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


class _MatrixToXEP0393Parser(HTMLParser):
    """
    Small Matrix pseudo-HTML to XEP-0393 converter.

    It preserves a safe/common subset of Matrix formatting. Inline formatting
    is buffered so the emitted XEP-0393 directives can be adjusted to satisfy
    span boundary rules.
    """

    INLINE_MARKERS = {
        "strong": "*",
        "b": "*",
        "em": "_",
        "i": "_",
        "del": "~",
        "s": "~",
        "strike": "~",
    }

    BLOCK_TAGS = {"blockquote", "p", "div", "pre"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.out = []
        self.inline_stack = []
        self.tag_stack = []
        self.pre_depth = 0
        self.skip_depth = 0

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()

        if tag == "mx-reply":
            self.skip_depth += 1
            return

        if self.skip_depth > 0:
            self.skip_depth += 1
            return

        attrs_dict = dict(attrs)

        if tag in self.INLINE_MARKERS:
            self._start_inline(tag, self.INLINE_MARKERS[tag])
            self.tag_stack.append((tag, ""))
        elif tag == "code":
            if self.pre_depth > 0:
                self.tag_stack.append((tag, ""))
            else:
                self._start_inline(tag, "`")
                self.tag_stack.append((tag, ""))
        elif tag == "a":
            href = attrs_dict.get("href", "")
            closer = f" ( {href} )" if href else ""
            self.tag_stack.append((tag, closer))
        elif tag == "pre":
            self.pre_depth += 1
            self._ensure_newline()
            self._append("```\n")
            self.tag_stack.append((tag, "\n```"))
        elif tag == "blockquote":
            self._ensure_newline()
            self._append("> ")
            self.tag_stack.append((tag, ""))
        elif tag == "br":
            self._append("\n")
            if self._inside("blockquote"):
                self._append("> ")
        elif tag in {"p", "div"}:
            if self._inside("blockquote"):
                self._ensure_blockquote_prefix()
            else:
                self._ensure_newline()
            self.tag_stack.append((tag, ""))
        else:
            self.tag_stack.append((tag, ""))

    def handle_endtag(self, tag):
        tag = tag.lower()

        if self.skip_depth > 0:
            self.skip_depth -= 1
            return

        if tag in self.INLINE_MARKERS:
            self._end_inline(tag)
            self._pop_closer_for_tag(tag)
            return

        if tag == "code" and self.pre_depth == 0:
            self._end_inline(tag)
            self._pop_closer_for_tag(tag)
            return

        if tag in {
            "a",
            "code",
            "pre",
            "blockquote",
            "p",
            "div",
        }:
            closing = self._pop_closer_for_tag(tag)
            self._append(closing)

            if tag == "pre" and self.pre_depth > 0:
                self.pre_depth -= 1

            if tag in self.BLOCK_TAGS:
                self._ensure_newline()

    def handle_data(self, data):
        if self.skip_depth > 0:
            return

        if self.pre_depth == 0 and data.strip() == "" and "\n" in data:
            return

        self._append(data)

    def handle_entityref(self, name):
        if self.skip_depth > 0:
            return

        self._append(html.unescape(f"&{name};"))

    def handle_charref(self, name):
        if self.skip_depth > 0:
            return

        self._append(html.unescape(f"&#{name};"))

    def get_text(self):
        while self.inline_stack:
            frame = self.inline_stack.pop()
            self._append("".join(frame["parts"]))

        result = "".join(self.out)

        # Clean up excessive blank lines while preserving intentional line breaks.
        result = re.sub(r"\n{3,}", "\n\n", result)

        return result.strip()

    def _append(self, text):
        if self.inline_stack:
            self.inline_stack[-1]["parts"].append(text)
        else:
            self.out.append(text)

    def _current_output_text(self):
        if self.inline_stack:
            return "".join(self.inline_stack[-1]["parts"])

        return "".join(self.out)

    def _start_inline(self, tag, marker):
        self.inline_stack.append({
            "tag": tag,
            "marker": marker,
            "parts": [],
        })

    def _end_inline(self, tag):
        for i in range(len(self.inline_stack) - 1, -1, -1):
            frame = self.inline_stack[i]

            if frame["tag"] != tag:
                continue

            del self.inline_stack[i]

            marker = frame["marker"]
            content = "".join(frame["parts"])
            rendered = self._render_inline(marker, content)
            self._append(rendered)
            return True

        return False

    def _render_inline(self, marker, content):
        if not content:
            return content

        leading_len = len(content) - len(content.lstrip())
        trailing_len = len(content) - len(content.rstrip())

        leading = content[:leading_len]
        trailing = content[len(content) -
                           trailing_len:] if trailing_len else ""
        inner_end = len(content) - \
            trailing_len if trailing_len else len(content)
        inner = content[leading_len:inner_end]

        if not inner:
            return content

        prefix = leading

        if self._needs_space_before_marker(marker):
            prefix += " "

        return f"{prefix}{marker}{inner}{marker}{trailing}"

    def _needs_space_before_marker(self, marker):
        current = self._current_output_text()

        if not current:
            return False

        previous = current[-1]

        if previous.isspace():
            return False

        if self.inline_stack:
            previous_marker = self.inline_stack[-1].get("marker")
            return previous_marker is None or previous_marker == marker

        return True

    def _inside(self, tag):
        tag = tag.lower()
        return any(open_tag.lower() == tag for open_tag, _ in self.tag_stack)

    def _ensure_blockquote_prefix(self):
        current = self._current_output_text()

        if not current:
            self._append("> ")
        elif current.endswith("> "):
            return
        elif current.endswith("\n"):
            self._append("> ")
        else:
            self._append("\n> ")

    def _ensure_newline(self):
        current = self._current_output_text()

        if current and not current.endswith("\n"):
            self._append("\n")

    def _pop_closer_for_tag(self, tag):
        tag = tag.lower()

        for i in range(len(self.tag_stack) - 1, -1, -1):
            open_tag, closer = self.tag_stack[i]

            if open_tag == tag:
                del self.tag_stack[i]
                return closer

        return ""


def matrix_html_to_xep0393(formatted_body: str) -> str:
    """
    Convert a subset of Matrix pseudo-HTML to XMPP XEP-0393 Message Styling.

    Supported:
      <strong>, <b>          -> *bold*
      <em>, <i>              -> _italic_
      <del>, <s>, <strike>   -> ~strike~
      <code>                 -> `code`
      <pre><code>...</code></pre> -> ```code```
      <blockquote>           -> > quote
      <br>                   -> newline
      <p>, <div>             -> paragraph-ish newlines

    Inline styling output is adjusted slightly to comply with XEP-0393 span
    rules. For example, <strong> bold </strong> becomes " *bold* ".
    """
    parser = _MatrixToXEP0393Parser()
    parser.feed(formatted_body)
    parser.close()
    return parser.get_text()
