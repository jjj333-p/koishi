"""
Koishi Bridge software
Copyright 2026 Joseph Winkie <jjj333.p.1325@gmail.com>
Licensed as AGPL 3.0
Distributed as-is and without warranty
"""
import html
import re
from pipe import Pipe

_MARKERS = {
    "*": "strong",
    "_": "em",
    "~": "del",
    "`": "code",
}


def _is_valid_open(s: str, pos: int, marker: str, open_positions: dict) -> bool:
    if pos + 1 >= len(s):
        return False

    if s[pos + 1].isspace():
        return False

    if pos == 0:
        return True

    previous = s[pos - 1]

    if previous.isspace():
        return True

    previous_open_marker = open_positions.get(pos - 1)
    return previous_open_marker is not None and previous_open_marker != marker


def _is_valid_close(s: str, pos: int, start: int) -> bool:
    if pos <= start + 1:
        return False

    return not s[pos - 1].isspace()


def _find_closing(s: str, start: int, end: int, marker: str) -> int | None:
    pos = start + 1

    while pos < end:
        if s[pos] == marker and _is_valid_close(s, pos, start):
            return pos

        pos += 1

    return None


def _parse_range(
    s: str,
    start: int,
    end: int,
    open_positions: dict,
    disabled_markers: set[str] | None = None,
) -> str:
    if disabled_markers is None:
        disabled_markers = set()

    out = []
    i = start

    while i < end:
        ch = s[i]

        if ch in _MARKERS \
            and ch not in disabled_markers \
                and _is_valid_open(s, i, ch, open_positions):

            close = _find_closing(s, i, end, ch)

            if close is not None:
                tag = _MARKERS[ch]
                open_positions[i] = ch

                if ch == "`":
                    content = html.escape(s[i + 1:close], quote=False)
                else:
                    content = _parse_range(
                        s,
                        i + 1,
                        close,
                        open_positions,
                        disabled_markers | {ch},
                    )

                escaped_marker = html.escape(ch, quote=False)
                out.append(
                    f"{escaped_marker}<{tag}>{content}</{tag}>{escaped_marker}")
                i = close + 1
                continue

        out.append(html.escape(ch, quote=False))
        i += 1

    return "".join(out)


def _convert_inline(s: str) -> str:
    return _parse_range(s, 0, len(s), open_positions={})


def _render_plain(part: str) -> str:
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
            output.append(_convert_inline(line))

            if i < len(lines) - 1:
                output.append("<br>")

    return "".join(output)


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
                + _render_plain("\n".join(quote_lines))
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

        output.append(_render_plain("\n".join(plain_lines)))

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
    turn characters into tokens
    2026-05-28 rewrite into state machine, fix pathological case of <a b=">"> by switching to a new "tag-quote" state
    """
    state = ('literal',)
    while True:
        try:
            c = next(f)
        except StopIteration:
            match state:
                case ('literal',):
                    pass
                case ('tag', tok):
                    yield '<'
                    yield from tok
                case ('tag-quote', tok, _):
                    yield '<'
                    yield from tok
                case ('escape', tok):
                    yield from tok
            break
        match state:
            case ('literal',):
                if c == '<':
                    state = ('tag', '')
                elif c == '&':
                    state = ('escape', '&')
                else:
                    yield c
            case ('tag', tok):
                if c == '>':  # end tag
                    yield tok + ' '
                    state = ('literal',)
                elif c == '<':  # wipe current tag, start new one
                    yield '<'
                    yield from tok
                    state = ('tag', '')
                elif c in '"\'':  # start processing quoted section
                    state = ('tag-quote', tok + c, c)
                else:  # add character to tag
                    state = ('tag', tok + c)
            case ('tag-quote', tok, quote):  # quotes inside tag, e.g. <a href=">">
                if c == quote:  # hit the end of the quotes
                    state = ('tag', tok + c)
                else:
                    state = ('tag-quote', tok + c, quote)
            case ('escape', tok):  # escape sequence &\w+;
                if c == ';':
                    yield tok + '/'  # act like escapes are special self closing tags
                    state = ('literal',)
                elif c == ' ':
                    yield from tok
                    yield c
                    state = ('literal',)
                else:
                    state = ('escape', tok + c)

@Pipe
def extract_tag_attributes(f):
    """
    extract attributes from tags and turn into tuple[string,dict]
    https://git.qwertydotpl.us/qwerty/patches/src/commit/be753f233ce105182c215125fb6bce254ca19b0c/0001-feat-add-attrs-links-to-matrix-xmpp-converter.patch#L23
    """

    while True:
        try:
            tok = next(f)
        except StopIteration:
            break
        if len(tok) <= 1:
            yield tok
            continue
        # print(f'extract_tag_attributes {repr(tok)}')
        try:
            # if there is no space, index will throw a ValueError
            idx = tok.index(' ')
        except ValueError:  # no attributes
            yield (tok, {})
            continue  # skip further parsing
        # print(f'extract_tag_attributes 2 {repr(tok)}')

        name = tok[0:idx]
        if tok[-1] == '/':  # carry over / from self closing tags
            name += '/'
            tok = tok[:-1]

        if tok[0] in '/&':  # closing tags and escapes shouldn't have attributes
            yield (name, {})
            continue  # skip further processing
        tok = tok[idx + 1:]

        tok += ' '  # makes the next step marginally easier

        # attributes can thankfully be parsed with regex:
        # /(\w+)(?:=('[^']+'|"[^"]+"|[^ ]+))? /
        #  ^       ^^                        ^ space (ends attribute) (the reason for tok += ' ')
        #  ^       ^^ value, possibly in quotes
        #  ^       ^ equals
        #  ^ key, composed of some number of word characters a-zA-Z0-9_

        def fixattr(x):  # parse escapes in attributes (because doing that in the tokenizer is difficult
            return (x.replace('&lt;','<')
                     .replace('&gt;','>')
                     .replace('&quot;','"')
                     .replace('&apos;',"'")
                     .replace('&amp;','&'))

        attrs = dict((m.group(1), fixattr(m.group(2))) for m in re.finditer(
            '(\\w+)(?:=(\'[^\']+\'|"[^"]+"|[^ ]+))? ', tok))

        yield (name, attrs)


@Pipe
def mark_self_closing(f):
    """
    https://git.qwertydotpl.us/qwerty/patches/src/commit/a1c1b2cf10179bd9e80290d71ced429905573b30/convert-matrix-formatted_body-to-xmpp-xep0393.py#L34
    """
    while True:
        try:
            tok = next(f)
        except StopIteration:
            break
        match tok:
            case ('br', x):
                yield ('br/', x)
            case _:
                yield tok


@Pipe
def fix_tag_matching(f):
    """
    pass 3: use stack to match tags and erase unmatched closing tags
    this makes sure tags match up, so e.g. <a><b>123</a></c> becomes <a><b>123</b></>
    https://git.qwertydotpl.us/qwerty/patches/src/commit/a1c1b2cf10179bd9e80290d71ced429905573b30/convert-matrix-formatted_body-to-xmpp-xep0393.py#L48

    qwertydotplus 2026-05-28: made it also copy attributes from opening tag to closing tag
    """
    stack = []
    halfstack = []  # version of stack with only tag names
    while True:
        try:
            tok = next(f)
        except StopIteration:
            while len(stack) > 0:
                tag = stack.pop()
                yield ('/' + tag[0], tag[1])
            break
        if isinstance(tok, str):  # char
            yield tok  # skip
            continue
        name = tok[0]
        if name[-1] == '/':  # self-closing tag
            yield tok  # skip
        elif name[0] == '/':  # closing tag
            if name[1:] in halfstack:  # exists in stack, unravel until we hit it
                while True:
                    popped = stack.pop()
                    halfstack.pop()
                    yield ('/' + popped[0], popped[1])
                    if popped[0] == name[1:]:
                        # we hit our tag, move on
                        break
            # else drop
        else:  # opening tag
            yield tok
            stack.append(tok)  # push
            halfstack.append(name)


@Pipe
def rename_equivalents(f):
    """
    https://git.qwertydotpl.us/qwerty/patches/src/commit/be753f233ce105182c215125fb6bce254ca19b0c/0001-feat-add-attrs-links-to-matrix-xmpp-converter.patch#L134
    """
    while True:
        try:
            tok = next(f)
        except StopIteration:
            break
        if isinstance(tok, str):
            yield tok
            continue
        # print(f'rename_equivalents {repr(tok)}')
        name = tok[0]
        cl = ''
        if name[0] == '/':
            name = name[1:]
            cl = '/'
        match name:
            case 'i':
                yield (cl + 'em', tok[1])
            # functionally bold and underline exist for the same reason, and xmpp only supports bold
            # (and honestly, who really uses underline anymore?)
            case 'b' | 'u':
                yield (cl + 'strong', tok[1])
            case 's':
                yield (cl + 'del', tok[1])
            case _:
                yield tok


@Pipe
def drop_redundant(f):
    """
    pass 4: drop redundant/irrelevant tags
    this strips out redundancies in e.g. <em><em>abc</em> <em>def</em></em>
    (which then becomes <em>abc def</em>)
    https://git.qwertydotpl.us/qwerty/patches/src/commit/a1c1b2cf10179bd9e80290d71ced429905573b30/convert-matrix-formatted_body-to-xmpp-xep0393.py#L76
    """
    stack = []
    while True:
        try:
            tok = next(f)
        except StopIteration:
            break
        if isinstance(tok, str):  # char
            yield tok  # skip
            continue
        # print(f'drop_redundant {repr(tok)}')
        name = tok[0]
        if name[-1] == '/':  # self-closing tag
            yield tok  # skip
        elif name[0] == '/':  # closing tag
            if stack.pop() != '.':  # not irrelevant
                yield tok  # output
        else:  # opening tag
            match name:
                case 'em' | 'strong' | 'del':  # cannot be applied more than once
                    if name in stack:
                        stack.append('.')
                    else:
                        stack.append(name)
                        yield tok
                # we drop code in pre (and vice versa) because it's easier to treat code as inline
                # and pre as meaning codeblock
                # (does anybody even use <pre> for anything other than code, anyway?)
                case 'pre' | 'code':
                    if 'pre' in stack or 'code' in stack:
                        stack.append('.')
                    else:
                        stack.append(name)
                        yield tok
                case _:
                    stack.append(name)
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
        if isinstance(tok, str):  # char
            yield tok  # skip
            continue
        # print(f'drop_bad_blocks {repr(tok)}')
        name = tok[0]
        if name[-1] == '/':  # self-closing tag
            yield tok  # skip
        elif name[0] == '/':  # closing tag
            yield tok  # output
        else:  # opening tag
            match name:
                case 'mx-reply':  # drop block
                    while True:
                        try:
                            tok = next(f)
                        except StopIteration:
                            break
                        if isinstance(tok, tuple) and tok[0] == '/' + name:
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

    def newline():
        return '\n' + '> ' * quote_level  # because we use this construct enough to bother making it a variable (computed value because used immediately after quote_level += 1)

    while True:
        try:
            tok = next(f)
            # print(f'naive_convert {repr(tok)}')
        except StopIteration:
            break
        match tok:
            case '\n':
                yield newline()
            # this handles characters when inside a code block:
            case _ if unformat and isinstance(tok, str):
                yield tok
            # handle single special characters:
            case '*':
                yield '\u200B*\u200B'
            case '_':
                yield '\u200B_\u200B'
            case '~':
                yield '\u200B~\u200B'
            case '`':
                yield '\u200B`\u200B'
            case '>':
                yield '\u200B>'
            # unescape <>"'&
            case ('&gt/', _):
                yield '\u200B>'
            case ('&lt/', _):
                yield '<'
            case ('&quot/', _):
                yield '"'
            case ('&apos/', _):
                yield "'"
            case ('&amp/', _):
                yield '&'
            # handle formatting toggles (this only works because we fixTagMatching and dropRedundant):
            case ('em' | '/em', _):
                yield '_'
            case ('strong' | '/strong', _):
                yield '*'
            case ('del' | '/del', _):
                yield '~'
            case ('code' | '/code', _):
                yield '`'
                unformat = not unformat
            case ('pre' | '/pre', _):
                yield newline() + '```' + newline()
                unformat = not unformat
            # handle blockquotes:
            case ('blockquote', _):
                quote_level += 1
                yield newline()
            case ('/blockquote', _):
                quote_level -= 1
                yield newline()
            case ('p' | '/p' | 'br/', _):
                yield newline()
            case ('/a', {'href': href}):
                # extract_tag_attributes doesn't strip out quotes, so we strip them here:
                sanitized_href = href.strip('\'"')
                if sanitized_href.startswith("https://matrix.to/"):
                    continue
                yield f' ( {sanitized_href} )'
            # drop unknown tags:
            case (_, _):
                print("dropping", tok)
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
            | extract_tag_attributes
            | mark_self_closing
            | fix_tag_matching
            | rename_equivalents
            | drop_redundant
            | drop_bad_blocks
            | naive_convert
        )
    )


if __name__ == "__main__":
    TEST_MESSAGE = "<test interrupted tag <i><i><b>bold&italic test<br>br <a href='pass&lt;'>test link</a> <a>test closing attr, pathological attr</a href='>fail'></i></b></i> <s>strikethrough</s><blockquote>1 quote<blockquote>2 quotes<pre><code>code in<br>quotes</code></pre></blockquote></blockquote><mx-reply>fail mx-reply</mx-reply><p>paragraph 1</p>out of paragraph 1<p>paragraph 2</p>test&lt;&gt;&amp;escape<endtag"
    VERIFY = matrix_html_to_xep0393(TEST_MESSAGE)
    print(VERIFY)
    assert VERIFY.strip() == '''
<test interrupted tag _*bold&italic test
br test link ( pass< ) test closing attr, pathological attr*_ ~strikethrough~
> 1 quote
> > 2 quotes
> > ```
> > code in
> > quotes
> > ```
paragraph 1
out of paragraph 1
paragraph 2
test<\u200b>&escape<endtag
'''.strip('\n')

