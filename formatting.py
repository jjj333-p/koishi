"""
Koishi Bridge software
Copyright 2026 Joseph Winkie <jjj333.p.1325@gmail.com>
Licensed as AGPL 3.0
Distributed as-is and without warranty
"""
import html
import re
from pipe import Pipe


def xep0393_to_matrix_html(msg: str) -> str:
    """
    Convert a subset of XMPP XEP-0393 Message Styling to Matrix pseudo-HTML.

    Supported:
      *bold*           -> *<strong>bold</strong>*
      _italic_         -> _<em>italic</em>_
      ~strike~         -> ~<del>strike</del>~
      `code`           -> `<code>code</code>`
      ```code block``` -> <pre><code>code block</code></pre>
      > quote          -> <blockquote>quote</blockquote>
      > ... > quote    -> correct number of blockquotes

    The original inline styling directives are intentionally preserved.

    how this works:

    1. split out and parse block level elements recursively, storing alternating unparsed and parsed chunks into `staging`
    2. if we find block level elements of a given type, jump to merging
        a. recursively call ourselves to parse the unparsed chunks
        b. copy the parsed chunks into the output
        c. return
    3. if there are no more block level elements, split our input by '\n', parse the lines as spans, and join by '<br>'

    span parsing substitutes directives for html by pairing directives and parsing the contents between the pairs as spans (recursive method)
    """

    # parse and mark code blocks:
    staging = []
    lastend = 0
    CODEBLOCK_REGEX = r'^```[ \t]*([^\n]*)\n([\s\S]*?)\n```$'
    for match in re.finditer(CODEBLOCK_REGEX, msg, flags=re.MULTILINE):
        # unparsed chunk
        staging.append((False, msg[lastend:match.start()]))

        # Extract language (Group 1) and code content (Group 2)
        lang = match.group(1).strip()
        code = match.group(2)

        # If a language was specified, add it as a CSS class (standard Markdown behavior)
        if lang:
            escaped_lang = html.escape(lang)

            # weird gajim special case
            if escaped_lang.startswith("python"):
                escaped_lang = "py"

            parsed_chunk = f'<pre><code class="language-{escaped_lang}">{html.escape(code)}</code></pre>'
        else:
            parsed_chunk = f'<pre><code>{html.escape(code)}</code></pre>'

        staging.append((True, parsed_chunk))
        lastend = match.end()  # advance index

    staging.append((False, msg[lastend:]))  # copy final unparsed chunk
    if len(staging) == 1:  # no code blocks; parse and mark block quotes:
        # strip "> " from quoteblock and send back through the parser
        def strip_parse_quoteblock(m):
            return xep0393_to_matrix_html(re.sub('^> ?', '', m, flags=re.MULTILINE))
        staging = []
        lastend = 0
        for match in re.finditer('^>.*(\n>.*)*$', msg, flags=re.MULTILINE):
            # unparsed section
            staging.append((False, msg[lastend:match.start()]))
            staging.append(
                (
                    True,
                    f'<blockquote>{strip_parse_quoteblock(match.group(0))}</blockquote>'
                )
            )
            lastend = match.end()
        staging.append((False, msg[lastend:]))
    if len(staging) == 1:  # no code blocks or block quotes; parse spans:
        def parse_span(text: str) -> str:
            def sub_tag(m: re.Match) -> str:
                match m.group(1):
                    case '*':
                        return f'*<strong>{parse_span(m.group(2))}</strong>*'
                    case '_':
                        return f'_<em>{parse_span(m.group(2))}</em>_'
                    case '~':
                        return f'~<del>{parse_span(m.group(2))}</del>~'
                    case '`':
                        return f'`<code>{m.group(2)}</code>`'
            return re.sub(r'([*_~`])(.+?)\1', sub_tag, text)
        return '<br>'.join(parse_span(html.escape(line)) for line in msg.split('\n'))
    for i, (parsed, message) in enumerate(staging):  # parse unparsed
        if len(message) == 0 or parsed:
            staging[i] = message
        else:
            staging[i] = xep0393_to_matrix_html(message)
    return ''.join(staging)  # join output together


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
    for c in f:
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
    # end:
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


@Pipe
def extract_tag_attributes(f):
    """
    extract attributes from tags and turn into tuple[string,dict]
    https://git.qwertydotpl.us/qwerty/patches/src/commit/be753f233ce105182c215125fb6bce254ca19b0c/0001-feat-add-attrs-links-to-matrix-xmpp-converter.patch#L23
    """

    for tok in f:
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
            if not isinstance(x, str):
                return None
            return (x.replace('&lt;', '<')
                     .replace('&gt;', '>')
                     .replace('&quot;', '"')
                     .replace('&apos;', "'")
                     .replace('&amp;', '&'))

        attrs = dict((m.group(1), fixattr(m.group(2))) for m in re.finditer(
            '(\\w+)(?:=(\'[^\']+\'|"[^"]+"|[^ ]+))? ', tok))

        yield (name, attrs)


@Pipe
def mark_self_closing(f):
    """
    https://git.qwertydotpl.us/qwerty/patches/src/commit/a1c1b2cf10179bd9e80290d71ced429905573b30/convert-matrix-formatted_body-to-xmpp-xep0393.py#L34
    """
    for tok in f:
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
    for tok in f:
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
    # exit: unwind stack
    while len(stack) > 0:
        tag = stack.pop()
        yield ('/' + tag[0], tag[1])


@Pipe
def rename_equivalents(f):
    """
    https://git.qwertydotpl.us/qwerty/patches/src/commit/be753f233ce105182c215125fb6bce254ca19b0c/0001-feat-add-attrs-links-to-matrix-xmpp-converter.patch#L134
    """
    for tok in f:
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
def merge_code_attrs(f):
    """
    merge attrs from <pre><code> onto <pre>
    allows syntax highlighting in naive convert
    """
    out = []
    parse_pre = 0  # this has to be an int, since we haven't dropped redundant tags yet
    for tok in f:
        match tok:
            case ('pre',_):
                parse_pre += 1
                out.append(tok)
            case _ if parse_pre <= 0:
                yield tok
            case ('/pre',_) if parse_pre > 1:  # not final </pre>
                parse_pre -= 1
                out.append(tok)
            case ('/pre',_) if parse_pre == 1:  # final </pre>, no <code>
                yield from out
                yield tok
                parse_pre = False
            case ('code', code_attrs) if parse_pre > 0:  # code in pre, the thing we're actually looking for
                (pre, pre_attrs) = out[0]
                out[0] = (pre, pre_attrs | code_attrs)
                yield from out  # dump our buffer
                yield tok  # emit code tag
                # reset state:
                out = []
                parse_pre = 0
            case _ if parse_pre > 0:
                out.append(tok)
            case _:  # we shouldn't ever get here, because parse_pre<=0 and parse_pre>0 should cover all values of parse_pre
                assert False

@Pipe
def drop_redundant(f):
    """
    pass 4: drop redundant/irrelevant tags
    this strips out redundancies in e.g. <em><em>abc</em> <em>def</em></em>
    (which then becomes <em>abc def</em>)
    https://git.qwertydotpl.us/qwerty/patches/src/commit/a1c1b2cf10179bd9e80290d71ced429905573b30/convert-matrix-formatted_body-to-xmpp-xep0393.py#L76
    """
    stack = []
    for tok in f:
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
    for tok in f:
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
        # because we use this construct enough to bother making it a variable (computed value because used immediately after quote_level += 1)
        return '\n' + '> ' * quote_level

    for tok in f:
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
            case (('code' | '/code') as name, _):
                yield '`'
                unformat = name[0] != '/'
            case ('pre', attrs):
                lang = ''
                if 'class' in attrs:
                    cls = attrs['class'].strip('\'"')
                    if cls.startswith('language-'):
                        lang = cls[9:]
                    if lang == 'py':
                        lang = 'python3'
                yield newline() + f'```{lang}' + newline()
                unformat = True
            case ('/pre', _):
                yield newline() + '```' + newline()
                unformat = False
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
            | merge_code_attrs
            | drop_redundant
            | drop_bad_blocks
            | naive_convert
        )
    )


if __name__ == "__main__":
    TEST_MESSAGE = "<test interrupted tag <i><i><b>bold&italic test<br>br <a href='pass&lt;'>test link</a> <a>test closing attr, pathological attr</a href='>fail'></i></b></i> <s>strikethrough</s><blockquote>1 quote<blockquote>2 quotes<pre><code class='language-py'>code in<br>quotes</code></pre></blockquote></blockquote><mx-reply>fail mx-reply</mx-reply><p>paragraph 1</p>out of paragraph 1<p>paragraph 2</p>test&lt;&gt;&amp;escape<test novalue><a href='https://matrix.to/'><endtag"
    VERIFY = matrix_html_to_xep0393(TEST_MESSAGE)
    print(VERIFY)
    assert VERIFY.strip() == '''
<test interrupted tag _*bold&italic test
br test link ( pass< ) test closing attr, pathological attr*_ ~strikethrough~
> 1 quote
> > 2 quotes
> > ```python3
> > code in
> > quotes
> > ```
paragraph 1
out of paragraph 1
paragraph 2
test<\u200b>&escape<endtag
'''.strip('\n')
