# Databricks notebook source
# MAGIC %md
# MAGIC # Parse and Chunk (C4, Internal ADB workspace)
# MAGIC
# MAGIC 1. Picks copied documents with ingest_status PENDING from `rcm_promotion_log`.
# MAGIC 2. PDF: extracts text with `ai_parse_document` (GA). HTML: extracts text with the Python standard library.
# MAGIC 3. Saves the text to `rcm_doc_content` (Silver, every version kept).
# MAGIC 4. Splits the text into chunks and saves them to `rcm_doc_chunk` (Gold, current version only).
# MAGIC 5. Marks each document INGESTED or FAILED in `rcm_promotion_log`.
# MAGIC
# MAGIC Compute: serverless (environment version 3 or above) or Databricks Runtime 17.3 or above.
# MAGIC Parameters: `env` (default dev) and `max_documents` (default 200).
# MAGIC Table names and chunk sizes come from config/<env>.yaml through common/config.py.

# COMMAND ----------

import json
import time
from datetime import datetime
from html.parser import HTMLParser
from zoneinfo import ZoneInfo

spark.conf.set('spark.sql.session.timeZone', 'Asia/Singapore')
SGT = ZoneInfo('Asia/Singapore')

# Load the settings for the chosen environment from config/<env>.yaml, so nothing is hardcoded here.
# The env widget (or job parameter) picks the file: dev, uat or prod.
import os
import sys
_repo_root = os.path.dirname(os.getcwd()) if os.path.basename(os.getcwd()) == 'notebooks' else os.getcwd()
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)
from common.config import load_config, table, external_table, volume_path

dbutils.widgets.text('env', 'dev')
CFG = load_config(dbutils.widgets.get('env'))
print('Settings from', CFG['_path'])

# Settings, all from the config file
PROMOTION_LOG = table(CFG, 'rcm_promotion_log')
CONTENT_TABLE = table(CFG, 'rcm_doc_content')
CHUNK_TABLE = table(CFG, 'rcm_doc_chunk')
CHUNK_CHARS = CFG['chunking']['chunk_chars']          # about 700 to 800 tokens per chunk
OVERLAP_CHARS = CFG['chunking']['overlap_chars']      # text repeated from the end of the previous chunk, so sentences are not cut off
MIN_TEXT_CHARS = CFG['chunking']['min_text_chars']    # less text than this is treated as a failed parse

dbutils.widgets.text('max_documents', '200')
MAX_DOCUMENTS = int(dbutils.widgets.get('max_documents').strip() or 200)


# Current time in Singapore, without time zone, as stored in the tables
def now_sgt():
    return datetime.now(SGT).replace(tzinfo=None)


# Run a SQL statement, retrying briefly if another job is writing the same table
def run_sql(sql, args=None, attempts=3):
    for i in range(1, attempts + 1):
        try:
            return spark.sql(sql, args=args or {})
        except Exception:
            if i == attempts:
                raise
            time.sleep(3 * i)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Work list

# COMMAND ----------

todo = spark.sql(f'''
    SELECT document_id, version, regulator_id, content_format, file_name, source_url, internal_file_path
    FROM {PROMOTION_LOG}
    WHERE copy_status = 'COPIED' AND ingest_status = 'PENDING'
    ORDER BY copied_at_sgt
    LIMIT {MAX_DOCUMENTS}
''').collect()
print(f'{len(todo)} documents to parse')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Text extraction

# COMMAND ----------

SKIP_ELEMENTS = {'page_header', 'page_footer', 'page_number'}
HEADING_ELEMENTS = {'title', 'section_header'}


def parse_pdfs(paths):
    '''Returns {path: (paragraphs, page_count, error)} using ai_parse_document.
    paragraphs is a list of (kind, text, page) where kind is heading or text.'''
    if not paths:
        return {}
    spark.read.format('binaryFile').load(paths).createOrReplaceTempView('pdf_files')
    rows = spark.sql('''
        SELECT path, to_json(ai_parse_document(content, map('version', '2.0'))) AS parsed
        FROM pdf_files
    ''').collect()
    result = {}
    for r in rows:
        path = r['path'].replace('dbfs:', '', 1)
        try:
            doc = json.loads(r['parsed'])
            errors = doc.get('error_status') or []
            paragraphs = []
            for el in (doc.get('document') or {}).get('elements') or []:
                kind = (el.get('type') or '').lower()
                text = (el.get('content') or '').strip()
                if not text or kind in SKIP_ELEMENTS:
                    continue
                bbox = el.get('bbox') or [{}]
                page = (bbox[0] or {}).get('page_id')
                page = page + 1 if isinstance(page, int) else None    # page_id starts at 0
                paragraphs.append(('heading' if kind in HEADING_ELEMENTS else 'text', text, page))
            page_count = len((doc.get('document') or {}).get('pages') or [])
            error = '; '.join(str(e) for e in errors) if errors and not paragraphs else None
            result[path] = (paragraphs, page_count, error)
        except Exception as e:
            result[path] = ([], 0, f'Could not read parser output: {e}')
    return result


class _HtmlText(HTMLParser):
    '''Collects headings, paragraphs, list items and table rows from an HTML snapshot.'''
    BLOCKS = {'p', 'div', 'li', 'tr', 'br', 'section', 'article'}
    HEADINGS = {'h1', 'h2', 'h3', 'h4', 'h5', 'h6'}
    SKIP = {'script', 'style', 'noscript', 'nav', 'footer', 'form'}

    # Start with no text collected
    def __init__(self):
        super().__init__()
        self.paragraphs, self.buffer, self.kind, self.skip = [], [], 'text', 0

    # Save the text collected so far as one paragraph (heading or normal text)
    def _flush(self):
        text = ' '.join(''.join(self.buffer).split())
        if text:
            self.paragraphs.append((self.kind, text, None))
        self.buffer, self.kind = [], 'text'

    # An opening tag: skip scripts and menus, and start a new paragraph for headings and blocks
    def handle_starttag(self, tag, attrs):
        if tag in self.SKIP:
            self.skip += 1
        elif tag in self.HEADINGS or tag in self.BLOCKS:
            self._flush()
            if tag in self.HEADINGS:
                self.kind = 'heading'
        elif tag in ('td', 'th'):
            self.buffer.append(' | ')

    # A closing tag: end the skipped part, or end the current paragraph
    def handle_endtag(self, tag):
        if tag in self.SKIP:
            self.skip = max(0, self.skip - 1)
        elif tag in self.HEADINGS or tag in self.BLOCKS:
            self._flush()

    # Text between tags: keep it unless it is inside a skipped part
    def handle_data(self, data):
        if not self.skip:
            self.buffer.append(data)


# Extract the paragraphs of an HTML snapshot with the Python standard library
def parse_html(path):
    with open(path, 'r', encoding='utf-8', errors='replace') as f:
        parser = _HtmlText()
        parser.feed(f.read())
        parser.close()
    parser._flush()
    return parser.paragraphs, 0, None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Chunking

# COMMAND ----------

# Join the paragraphs into one text. Headings start with ## so chunking can find sections
def to_text(paragraphs):
    return '\n\n'.join(('## ' + t) if k == 'heading' else t for k, t, _ in paragraphs)


def make_chunks(paragraphs):
    '''Groups paragraphs into chunks of about CHUNK_CHARS. A new heading starts a new chunk
    once the current chunk is at least half full. Each chunk remembers its section title and first page.'''
    chunks, current, size, section, first_page = [], [], 0, '', None

    # Close the current chunk and start a new one, repeating the overlap from the end of the previous chunk
    def emit():
        nonlocal current, size, first_page
        if current:
            chunks.append({'section_title': section_of_chunk, 'page_number': first_page,
                           'chunk_text': '\n\n'.join(current)})
            tail = chunks[-1]['chunk_text'][-OVERLAP_CHARS:]
            current, size, first_page = ([tail], len(tail), None) if OVERLAP_CHARS else ([], 0, None)

    section_of_chunk = ''
    for kind, text, page in paragraphs:
        if kind == 'heading':
            if size >= CHUNK_CHARS / 2:
                emit()
            section = text
        while text:
            room = CHUNK_CHARS - size
            if room <= 0:
                emit()
                room = CHUNK_CHARS - size
            piece, text = text[:room], text[room:]
            if not current or (len(current) == 1 and first_page is None and size <= OVERLAP_CHARS):
                section_of_chunk = section
            current.append(('## ' + piece) if kind == 'heading' else piece)
            size += len(piece)
            if first_page is None and page:
                first_page = page
    if size > OVERLAP_CHARS or len(chunks) == 0:
        emit()
    return chunks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parse, save and mark

# COMMAND ----------

pdf_paths = [r['internal_file_path'] for r in todo if r['content_format'] == 'PDF']
pdf_results = parse_pdfs(pdf_paths)

content_rows, chunk_rows, done, failed = [], [], [], []
for r in todo:
    key = {'document_id': r['document_id'], 'version': r['version']}
    try:
        if r['content_format'] == 'PDF':
            paragraphs, pages, error = pdf_results.get(r['internal_file_path'], ([], 0, 'Not returned by parser'))
        else:
            paragraphs, pages, error = parse_html(r['internal_file_path'])
        text = to_text(paragraphs)
        if error or len(text) < MIN_TEXT_CHARS:
            raise ValueError(error or f'Only {len(text)} characters extracted')
        ts = now_sgt()
        content_rows.append((r['document_id'], r['version'], r['regulator_id'], r['content_format'],
                             r['file_name'], r['source_url'], r['internal_file_path'], text,
                             pages or None, 'PARSED', None, ts))
        for i, c in enumerate(make_chunks(paragraphs)):
            chunk_rows.append((f"{r['document_id']}_v{r['version']}_{i:04d}", r['document_id'], r['version'],
                               r['regulator_id'], i, c['section_title'] or None, c['page_number'],
                               c['chunk_text'], len(c['chunk_text']), r['file_name'], r['source_url'], ts))
        done.append(key)
        print(f"PARSED {r['file_name']}")
    except Exception as e:
        failed.append(dict(key, error=f'{type(e).__name__}: {e}'[:4000]))
        print(f"FAILED {r['file_name']}: {e}")

# COMMAND ----------

content_cols = ['document_id', 'version', 'regulator_id', 'content_format', 'file_name', 'source_url',
                'internal_file_path', 'content_text', 'page_count', 'parse_status', 'parse_error', 'parsed_at_sgt']
chunk_cols = ['chunk_id', 'document_id', 'version', 'regulator_id', 'chunk_index', 'section_title',
              'page_number', 'chunk_text', 'char_count', 'file_name', 'source_url', 'created_at_sgt']

if done:
    keys = spark.createDataFrame([(d['document_id'], d['version']) for d in done], 'document_id string, version int')
    keys.createOrReplaceTempView('done_keys')
    # Rerun safe: replace this version in Silver; in Gold keep only the current version of each document
    run_sql(f'''MERGE INTO {CONTENT_TABLE} t USING done_keys k
                ON t.document_id = k.document_id AND t.version = k.version
                WHEN MATCHED THEN DELETE''')
    run_sql(f'DELETE FROM {CHUNK_TABLE} WHERE document_id IN (SELECT document_id FROM done_keys)')
    spark.table(CONTENT_TABLE).limit(0)  # check the table exists before writing
    (spark.createDataFrame(content_rows, spark.table(CONTENT_TABLE).select(*content_cols).schema)
         .write.mode('append').saveAsTable(CONTENT_TABLE))
    if chunk_rows:
        (spark.createDataFrame(chunk_rows, spark.table(CHUNK_TABLE).select(*chunk_cols).schema)
             .write.mode('append').saveAsTable(CHUNK_TABLE))
    run_sql(f'''MERGE INTO {PROMOTION_LOG} t USING done_keys k
                ON t.document_id = k.document_id AND t.version = k.version
                WHEN MATCHED THEN UPDATE SET ingest_status = 'INGESTED', ingest_error_details = NULL,
                                             updated_at_sgt = :ts''', {'ts': now_sgt()})

for f in failed:
    run_sql(f'''
        UPDATE {PROMOTION_LOG} SET ingest_status = 'FAILED', ingest_error_details = :error, updated_at_sgt = :ts
        WHERE document_id = :document_id AND version = :version
    ''', {'error': f['error'], 'ts': now_sgt(), 'document_id': f['document_id'], 'version': f['version']})

print(f'{len(done)} parsed, {len(failed)} failed, {len(chunk_rows)} chunks written')

# COMMAND ----------

if failed:
    raise RuntimeError(f'{len(failed)} documents failed to parse. See rcm_promotion_log where ingest_status = FAILED.')
dbutils.notebook.exit(f'parsed={len(done)} failed={len(failed)} chunks={len(chunk_rows)}')
