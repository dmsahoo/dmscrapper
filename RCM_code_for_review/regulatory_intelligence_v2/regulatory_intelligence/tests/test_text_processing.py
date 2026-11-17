'''Text extraction and chunking.'''
import json

from rcm_intelligence.text_processing import html_paragraphs, make_chunks, pages_of, read_parser_output, to_text


def test_parser_output_keeps_pages_and_skips_headers():
    parsed = json.dumps({'document': {'pages': [{}, {}], 'elements': [
        {'type': 'page_header', 'content': 'MAS', 'bbox': [{'page_id': 0}]},
        {'type': 'section_header', 'content': 'Part 1', 'bbox': [{'page_id': 0}]},
        {'type': 'text', 'content': 'First rule.', 'bbox': [{'page_id': 1}]}]}})
    paragraphs, page_count, error = read_parser_output(parsed)
    assert paragraphs == [('heading', 'Part 1', 1), ('text', 'First rule.', 2)]
    assert page_count == 2 and error is None
    assert pages_of(paragraphs) == [(1, '## Part 1'), (2, 'First rule.')]
    assert to_text(paragraphs) == '## Part 1\n\nFirst rule.'


def test_html_paragraphs_skip_menus():
    html = '<nav>Menu</nav><h1>Speech</h1><p>Good morning.</p><script>x()</script>'
    assert html_paragraphs(html) == [('heading', 'Speech', None), ('text', 'Good morning.', None)]


def test_chunks_have_page_range_and_overlap():
    paragraphs = [('heading', 'Section A', 1)] + [('text', 'x' * 400, p) for p in (1, 2, 3, 4)]
    chunks = make_chunks(paragraphs, chunk_chars=1000, overlap_chars=100)
    assert len(chunks) == 2
    assert chunks[0]['section_title'] == 'Section A'
    assert chunks[0]['page_from'] == 1 and chunks[0]['page_to'] == 3
    assert chunks[1]['chunk_text'].startswith(chunks[0]['chunk_text'][-100:])
    assert all(len(c['chunk_text']) <= 1000 + 10 for c in chunks)
