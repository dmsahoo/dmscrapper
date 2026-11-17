'''
Text extraction and chunking (pure functions, unit tested without Spark).

1. read_parser_output: turns the ai_parse_document result of one PDF into paragraphs.
2. html_paragraphs: extracts paragraphs from an HTML snapshot with the Python standard library.
3. to_text and pages_of: the full text and the text per page, for Silver.
4. make_chunks: groups paragraphs into chunks of about chunk_chars, with an overlap, and
   remembers the section title and the first and last page of each chunk.

A paragraph is a tuple (kind, text, page): kind is heading or text, page is the page number
(starting at 1) or None for HTML.
'''

import json
from html.parser import HTMLParser

SKIP_ELEMENTS = {'page_header', 'page_footer', 'page_number'}
HEADING_ELEMENTS = {'title', 'section_header'}
PARSER_PDF = 'ai_parse_document'
PARSER_HTML = 'html.parser'


def read_parser_output(parsed_json):
    '''Paragraphs, page count and error from the JSON text of one ai_parse_document result.'''
    try:
        doc = json.loads(parsed_json)
    except (TypeError, ValueError) as error:
        return [], 0, f'Could not read parser output: {error}'
    errors = doc.get('error_status') or []
    paragraphs = []
    for element in (doc.get('document') or {}).get('elements') or []:
        kind = (element.get('type') or '').lower()
        text = (element.get('content') or '').strip()
        if not text or kind in SKIP_ELEMENTS:
            continue
        bbox = element.get('bbox') or [{}]
        page = (bbox[0] or {}).get('page_id')
        page = page + 1 if isinstance(page, int) else None              # page_id starts at 0
        paragraphs.append(('heading' if kind in HEADING_ELEMENTS else 'text', text, page))
    page_count = len((doc.get('document') or {}).get('pages') or [])
    error = '; '.join(str(e) for e in errors) if errors and not paragraphs else None
    return paragraphs, page_count, error


class _HtmlText(HTMLParser):
    '''Collects headings, paragraphs, list items and table rows from an HTML snapshot.'''
    BLOCKS = {'p', 'div', 'li', 'tr', 'br', 'section', 'article'}
    HEADINGS = {'h1', 'h2', 'h3', 'h4', 'h5', 'h6'}
    SKIP = {'script', 'style', 'noscript', 'nav', 'footer', 'form'}

    def __init__(self):
        super().__init__()
        self.paragraphs, self.buffer, self.kind, self.skip = [], [], 'text', 0

    def flush(self):
        '''Save the text collected so far as one paragraph.'''
        text = ' '.join(''.join(self.buffer).split())
        if text:
            self.paragraphs.append((self.kind, text, None))
        self.buffer, self.kind = [], 'text'

    def handle_starttag(self, tag, attrs):
        if tag in self.SKIP:
            self.skip += 1
        elif tag in self.HEADINGS or tag in self.BLOCKS:
            self.flush()
            if tag in self.HEADINGS:
                self.kind = 'heading'
        elif tag in ('td', 'th'):
            self.buffer.append(' | ')

    def handle_endtag(self, tag):
        if tag in self.SKIP:
            self.skip = max(0, self.skip - 1)
        elif tag in self.HEADINGS or tag in self.BLOCKS:
            self.flush()

    def handle_data(self, data):
        if not self.skip:
            self.buffer.append(data)


def html_paragraphs(html_text):
    '''Paragraphs of an HTML snapshot (no page numbers).'''
    parser = _HtmlText()
    parser.feed(html_text or '')
    parser.close()
    parser.flush()
    return parser.paragraphs


def to_text(paragraphs):
    '''One text for Silver. Headings start with ## so the chunking can find sections.'''
    return '\n\n'.join(('## ' + t) if k == 'heading' else t for k, t, _ in paragraphs)


def pages_of(paragraphs):
    '''Text per page, as a list of (page_number, text), for page level citations. Empty for HTML.'''
    pages = {}
    for kind, text, page in paragraphs:
        if page is not None:
            pages.setdefault(page, []).append(('## ' + text) if kind == 'heading' else text)
    return [(page, '\n\n'.join(parts)) for page, parts in sorted(pages.items())]


def make_chunks(paragraphs, chunk_chars, overlap_chars):
    '''Groups paragraphs into chunks of about chunk_chars. A new heading starts a new chunk once the
    current chunk is at least half full. Each chunk keeps its section title and its first and last page.'''
    chunks, current, size, section = [], [], 0, ''
    first_page = last_page = None
    section_of_chunk = ''

    def emit():
        # Close the current chunk and start the next one with the overlap from its end
        nonlocal current, size, first_page, last_page
        if current:
            chunks.append({'section_title': section_of_chunk, 'page_from': first_page,
                           'page_to': last_page or first_page, 'chunk_text': '\n\n'.join(current)})
            tail = chunks[-1]['chunk_text'][-overlap_chars:] if overlap_chars else ''
            current, size = ([tail], len(tail)) if tail else ([], 0)
            first_page = last_page = None

    for kind, text, page in paragraphs:
        if kind == 'heading':
            if size >= chunk_chars / 2:
                emit()
            section = text
        while text:
            room = chunk_chars - size
            if room <= 0:
                emit()
                room = chunk_chars - size
            piece, text = text[:room], text[room:]
            if not current or (len(current) == 1 and first_page is None and size <= overlap_chars):
                section_of_chunk = section
            current.append(('## ' + piece) if kind == 'heading' else piece)
            size += len(piece)
            if page:
                first_page = first_page or page
                last_page = page
    if size > overlap_chars or len(chunks) == 0:
        emit()
    return chunks
