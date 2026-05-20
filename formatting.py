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
      *bold*           -> <strong>bold</strong>
      _italic_         -> <em>italic</em>
      ~strike~         -> <del>strike</del>
      `code`           -> <code>code</code>
      ```code block``` -> <pre><code>code block</code></pre>
      > quote          -> <blockquote>quote</blockquote>

    This is intentionally conservative and does not try to implement every
    edge case of XEP-0393 parsing.
    """

    def convert_inline(s: str) -> str:
        # Escape first so user-provided HTML is not interpreted.
        s = html.escape(s, quote=False)

        # Inline code first, so styling inside code is not processed.
        code_parts = re.split(r"(`[^`\n]+`)", s)

        for i, part in enumerate(code_parts):
            if len(part) >= 2 and part.startswith("`") and part.endswith("`"):
                code_parts[i] = f"`<code>{part[1:-1]}</code>`"
            else:
                # Avoid matching inside words by requiring non-word-ish boundaries.

                part = re.sub(
                    r"(?<!\w)\*([^*\n]+)\*(?!\w)",
                    r"*<strong>\1</strong>*",
                    part,
                )
                part = re.sub(
                    r"(?<!\w)_([^_\n]+)_(?!\w)",
                    r"_<em>\1</em>_",
                    part,
                )
                part = re.sub(
                    r"(?<!\w)~([^~\n]+)~(?!\w)",
                    r"~<del>\1</del>~",
                    part,
                )

                code_parts[i] = part

        return "".join(code_parts)

    # Split out fenced code blocks.
    parts = re.split(r"(```(?:\n)?[\s\S]*?```)", text)
    output = []

    for part in parts:
        if part.startswith("```") and part.endswith("```"):
            code = part[3:-3]
            if code.startswith("\n"):
                code = code[1:]
            if code.endswith("\n"):
                code = code[:-1]

            output.append(f"<pre><code>{html.escape(code, quote=False)}</code></pre>")
            continue

        lines = part.splitlines(keepends=False)
        i = 0

        while i < len(lines):
            line = lines[i]

            if line.startswith(">"):
                quote_lines = []

                while i < len(lines) and lines[i].startswith(">"):
                    quote_line = lines[i][1:]
                    if quote_line.startswith(" "):
                        quote_line = quote_line[1:]
                    quote_lines.append(convert_inline(quote_line))
                    i += 1

                output.append(
                    "<blockquote>"
                    + "<br>".join(quote_lines)
                    + "</blockquote>"
                )
                continue

            output.append(convert_inline(line))
            i += 1

            if i < len(lines):
                output.append("<br>")

    return "".join(output)


class _MatrixToXEP0393Parser(HTMLParser):
    """
    Small Matrix pseudo-HTML to XEP-0393 converter.

    It only preserves a safe/common subset of Matrix formatting.
    Unknown tags are ignored while their text content is kept.
    """

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.out = []
        self.tag_stack = []
        self.open_tags = []

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()

        if tag in {"strong", "b"}:
            self.out.append("*")
            self.tag_stack.append("*")
        elif tag in {"em", "i"}:
            self.out.append("_")
            self.tag_stack.append("_")
        elif tag in {"del", "s", "strike"}:
            self.out.append("~")
            self.tag_stack.append("~")
        elif tag == "code":
            # If inside <pre>, do not add inline backticks.
            if not self._inside("pre"):
                self.out.append("`")
                self.tag_stack.append("`")
            else:
                self.tag_stack.append("")
        elif tag == "pre":
            self._ensure_newline()
            self.out.append("```\n")
            self.tag_stack.append("\n```")
        elif tag == "blockquote":
            self._ensure_newline()
            self.out.append("> ")
            self.tag_stack.append("")
        elif tag == "br":
            self.out.append("\n")
            if self._inside("blockquote"):
                self.out.append("> ")
        elif tag in {"p", "div"}:
            self._ensure_newline()
            self.tag_stack.append("")
        else:
            self.tag_stack.append("")

    def handle_endtag(self, tag):
        tag = tag.lower()

        if tag in {
            "strong",
            "b",
            "em",
            "i",
            "del",
            "s",
            "strike",
            "code",
            "pre",
            "blockquote",
            "p",
            "div",
        }:
            closing = self._pop_last_relevant_closer()
            self.out.append(closing)

            if tag in {"blockquote", "p", "div", "pre"}:
                self._ensure_newline()

    def handle_data(self, data):
        self.out.append(data)

    def handle_entityref(self, name):
        self.out.append(html.unescape(f"&{name};"))

    def handle_charref(self, name):
        self.out.append(html.unescape(f"&#{name};"))

    def get_text(self):
        result = "".join(self.out)

        # Clean up excessive blank lines while preserving intentional line breaks.
        result = re.sub(r"\n{3,}", "\n\n", result)

        return result.strip()

    def _inside(self, tag):
        return tag.lower() in [t.lower() for t in self._open_tags()]

    def _open_tags(self):
        # HTMLParser does not maintain a tag stack for us, so this method exists
        # mostly for readability. For our simple parser, infer from output context.
        # We separately check <pre> by scanning unclosed closers.
        return tag.lower() in self.open_tags

    def _pop_open_tag(self, tag):
        tag = tag.lower()

        for i in range(len(self.open_tags) - 1, -1, -1):
            if self.open_tags[i] == tag:
                del self.open_tags[i]
                break

    def _ensure_newline(self):
        if self.out and not self.out[-1].endswith("\n"):
            self.out.append("\n")

    def _pop_last_relevant_closer(self):
        while self.tag_stack:
            closer = self.tag_stack.pop()
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
    """
    parser = _MatrixToXEP0393Parser()
    parser.feed(formatted_body)
    parser.close()
    return parser.get_text()