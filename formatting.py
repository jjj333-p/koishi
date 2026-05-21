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
                    r"(?<!\w)~([^~\n]+)~(?!\\w)",
                    r"~<del>\1</del>~",
                    part,
                )

                code_parts[i] = part

        return "".join(code_parts)

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

                output.append(f"<pre><code>{html.escape(code, quote=False)}</code></pre>")
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

    It only preserves a safe/common subset of Matrix formatting.
    Unknown tags are ignored while their text content is kept.
    """

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.out = []
        self.tag_stack = []
        self.pre_depth = 0

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        attrs_dict = dict(attrs)

        if tag in {"strong", "b"}:
            self.out.append("*")
            self.tag_stack.append((tag, "*"))
        elif tag in {"em", "i"}:
            self.out.append("_")
            self.tag_stack.append((tag, "_"))
        elif tag in {"del", "s", "strike"}:
            self.out.append("~")
            self.tag_stack.append((tag, "~"))
        elif tag == "a":
            href = attrs_dict.get("href", "")
            closer = f" ( {href} )" if href else ""
            self.tag_stack.append((tag, closer))
        elif tag == "pre":
            self.pre_depth += 1
            self._ensure_newline()
            self.out.append("```\n")
            self.tag_stack.append((tag, "\n```"))
        elif tag == "code":
            if self.pre_depth > 0:
                self.tag_stack.append((tag, ""))
            else:
                self.out.append("`")
                self.tag_stack.append((tag, "`"))
        elif tag == "blockquote":
            self._ensure_newline()
            self.out.append("> ")
            self.tag_stack.append((tag, ""))
        elif tag == "br":
            self.out.append("\n")
            if self._inside("blockquote"):
                self.out.append("> ")
        elif tag in {"p", "div"}:
            self._ensure_newline()
            self.tag_stack.append((tag, ""))
        else:
            self.tag_stack.append((tag, ""))

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
            "a",
            "code",
            "pre",
            "blockquote",
            "p",
            "div",
        }:
            closing = self._pop_closer_for_tag(tag)
            self.out.append(closing)

            if tag == "pre" and self.pre_depth > 0:
                self.pre_depth -= 1

            if tag in {"blockquote", "p", "div", "pre"}:
                self._ensure_newline()

    def handle_data(self, data):
        if self.pre_depth == 0 and data.strip() == "" and "\n" in data:
            return

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
        tag = tag.lower()
        return any(open_tag.lower() == tag for open_tag, _ in self.tag_stack)

    def _ensure_newline(self):
        if self.out and not self.out[-1].endswith("\n"):
            self.out.append("\n")

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
    """
    parser = _MatrixToXEP0393Parser()
    parser.feed(formatted_body)
    parser.close()
    return parser.get_text()