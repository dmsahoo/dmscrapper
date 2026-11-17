# Databricks notebook source
# MAGIC %md
# MAGIC # 03_ingest_rcm_parse_chunk
# MAGIC
# MAGIC | Item | Detail |
# MAGIC |---|---|
# MAGIC | **Purpose** | Turns each newly copied document into raw (Bronze), text (Silver) and chunks (Gold), ready for the Vector Search index and the Regulator Agent. |
# MAGIC | **Job and task** | `job_sng_rcm_ingest_daily`, task `01_parse_chunk` (then `02_index_sync` and `03_regulator_analysis`) |
# MAGIC | **Parameters** | `max_documents` (default 200). Optional `env` and `config_dir`. |
# MAGIC | **Inputs** | `rcm_promotion_log` rows with copy_status COPIED and ingest_status PENDING, files in `vol_documents`, document details from the external `rcm_pdf_catalog` |
# MAGIC | **Outputs** | `rcm_doc_raw` (Bronze, one row per version), `rcm_doc_content` (Silver, every version kept), `rcm_doc_chunk` (Gold, current version only), ingest_status INGESTED or FAILED in `rcm_promotion_log`, one run log file |
# MAGIC | **Compute** | Single node classic job cluster, Databricks Runtime 17.3 LTS or above (needed for `ai_parse_document`) |
# MAGIC | **Dependencies** | Wheel `rcm_intelligence`, cluster environment variables `RCM_ENV` and `RCM_CONFIG_DIR` |
# MAGIC | **Rerun safe** | Yes: Bronze and Silver are written with a delete and insert on (document_id, version), Gold per document |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Import the package
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Load the shared code from the `rcm_intelligence` wheel. The text extraction and chunking rules are in `text_processing.py` and are unit tested. |
# MAGIC | **Input** | Wheel `rcm_intelligence` |
# MAGIC | **Output** | Imported functions, the logger. The Spark session time zone is set to Singapore. |

# COMMAND ----------

import platform

from rcm_intelligence.common import new_run_id, now_sgt, run_sql_with_retry, sha256_bytes
from rcm_intelligence.logging_setup import RunLog, get_logger, set_log_context
from rcm_intelligence.settings import external_pdf_catalog, full_name, load_config, log_root
from rcm_intelligence.text_processing import (PARSER_HTML, PARSER_PDF, html_paragraphs, make_chunks, pages_of,
                                              read_parser_output, to_text)

spark.conf.set('spark.sql.session.timeZone', 'Asia/Singapore')
logger = get_logger()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Read the parameters and the config
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Load the table names and the chunk sizes from the config, and start the run log. |
# MAGIC | **Input** | Widgets `max_documents`, `env`, `config_dir`; `common_config_<env>.yaml` |
# MAGIC | **Output** | Table names, `CHUNK_CHARS`, `OVERLAP_CHARS`, `MIN_TEXT_CHARS`, `RUN_ID`, `run_log` |

# COMMAND ----------

dbutils.widgets.text('max_documents', '200')
dbutils.widgets.text('env', '')                # empty: use the cluster environment variable RCM_ENV
dbutils.widgets.text('config_dir', '')         # empty: use the cluster environment variable RCM_CONFIG_DIR

MAX_DOCUMENTS = int(dbutils.widgets.get('max_documents').strip() or 200)
CFG = load_config(dbutils.widgets.get('env').strip() or None, dbutils.widgets.get('config_dir').strip() or None)

PROMOTION_LOG = full_name(CFG, 'tables', 'promotion_log')
RAW_TABLE = full_name(CFG, 'tables', 'doc_raw')
CONTENT_TABLE = full_name(CFG, 'tables', 'doc_content')
CHUNK_TABLE = full_name(CFG, 'tables', 'doc_chunk')
PDF_CATALOG = external_pdf_catalog(CFG)
CHUNK_CHARS = int(CFG['chunking']['chunk_chars'])
OVERLAP_CHARS = int(CFG['chunking']['overlap_chars'])
MIN_TEXT_CHARS = int(CFG['chunking']['min_text_chars'])
PDF_PARSER_VERSION = '2.0'                                   # ai_parse_document output schema version
HTML_PARSER_VERSION = f'python {platform.python_version()}'

RUN_ID = new_run_id('ingest')
set_log_context(stage='INGEST', regulator_id='ALL', run_id=RUN_ID)
run_log = RunLog(log_root(CFG, 'ingest'), 'INGEST', 'ALL', RUN_ID).start()
logger.info('Environment %s | config %s', CFG['_env'], CFG['_path'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Build the work list
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Pick the copied document versions that are not ingested yet, with their details from the scraper catalog. |
# MAGIC | **Input** | `rcm_promotion_log`, external `rcm_pdf_catalog` |
# MAGIC | **Output** | `todo`: up to `max_documents` rows, oldest copy first |

# COMMAND ----------

todo = spark.sql(f'''
    SELECT p.document_id, p.version, p.regulator_id, p.category_id, p.content_format, p.file_name,
           p.source_url, p.internal_file_path, p.content_hash_sha256, p.run_id AS copy_run_id,
           c.item_title, c.item_date, c.item_date_label, c.item_reference, c.document_url,
           c.last_seen_run_id AS scrape_run_id
    FROM {PROMOTION_LOG} p
    LEFT JOIN {PDF_CATALOG} c ON c.document_id = p.document_id
    WHERE p.copy_status = 'COPIED' AND p.ingest_status = 'PENDING'
    ORDER BY p.copied_at_sgt
    LIMIT {MAX_DOCUMENTS}
''').collect()
logger.info('%s documents to ingest', len(todo))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Load the raw files into Bronze
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Read each copied file, check its fingerprint against the promotion log, and save it with its details in `rcm_doc_raw`. A file that does not match is marked FAILED and goes no further. |
# MAGIC | **Input** | `todo`, files in `vol_documents` |
# MAGIC | **Output** | Rows in `rcm_doc_raw`, lists `loaded` and `failed` |

# COMMAND ----------

failed = []                       # (document_id, version, error)
raw_rows, loaded = [], []
ts = now_sgt()
for r in todo:
    try:
        with open(r['internal_file_path'], 'rb') as handle:
            data = handle.read()
        if sha256_bytes(data) != r['content_hash_sha256']:
            raise ValueError('Hash of the file in the volume does not match rcm_promotion_log')
        raw_rows.append((r['document_id'], r['version'], r['regulator_id'], r['category_id'], r['item_title'],
                         r['item_date'], r['item_date_label'], r['item_reference'], r['source_url'],
                         r['document_url'], r['content_format'], r['file_name'], r['internal_file_path'],
                         r['content_hash_sha256'], bytearray(data), r['scrape_run_id'], r['copy_run_id'],
                         RUN_ID, ts))
        loaded.append(r)
    except Exception as error:
        failed.append((r['document_id'], r['version'], f'{type(error).__name__}: {error}'[:4000]))
        logger.error('FAILED to load %s: %s', r['file_name'], error)

if raw_rows:
    spark.createDataFrame(raw_rows, spark.table(RAW_TABLE).schema).createOrReplaceTempView('new_raw')
    run_sql_with_retry(spark, f'''MERGE INTO {RAW_TABLE} t USING (SELECT document_id, version FROM new_raw) k
                                  ON t.document_id = k.document_id AND t.version = k.version
                                  WHEN MATCHED THEN DELETE''', what='raw delete')
    run_sql_with_retry(spark, f'INSERT INTO {RAW_TABLE} SELECT * FROM new_raw', what='raw insert')
logger.info('%s files loaded into Bronze, %s failed', len(loaded), len(failed))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Extract the text
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | PDF: `ai_parse_document` reads the content column of Bronze directly. HTML: the Python standard library reads the snapshot. Both give paragraphs with their page number (PDF only). |
# MAGIC | **Input** | `rcm_doc_raw` rows of this run |
# MAGIC | **Output** | `parsed`: for each (document_id, version), the paragraphs, page count and any error |

# COMMAND ----------

parsed = {}
pdf_keys = [(r['document_id'], r['version']) for r in loaded if r['content_format'] == 'PDF']
if pdf_keys:
    spark.createDataFrame(pdf_keys, 'document_id string, version int').createOrReplaceTempView('pdf_keys')
    for row in spark.sql(f'''
            SELECT r.document_id, r.version, to_json(ai_parse_document(r.content, map('version', '{PDF_PARSER_VERSION}'))) AS parsed
            FROM {RAW_TABLE} r JOIN pdf_keys k ON r.document_id = k.document_id AND r.version = k.version
        ''').collect():
        parsed[(row['document_id'], row['version'])] = read_parser_output(row['parsed'])

for r in loaded:
    if r['content_format'] != 'PDF':
        with open(r['internal_file_path'], 'r', encoding='utf8', errors='replace') as handle:
            parsed[(r['document_id'], r['version'])] = (html_paragraphs(handle.read()), 0, None)
logger.info('Text extracted for %s documents', len(parsed))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6: Build the Silver and Gold rows
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | For each document: the full text and the text per page (Silver), and the chunks of about `chunk_chars` characters with an overlap of `overlap_chars` (Gold). A document with less than `min_text_chars` characters counts as a failed parse. |
# MAGIC | **Input** | `parsed`, `loaded` |
# MAGIC | **Output** | `content_rows`, `chunk_rows`, `done` (document versions parsed) |

# COMMAND ----------

content_rows, chunk_rows, done = [], [], []
ts = now_sgt()
for r in loaded:
    key = (r['document_id'], r['version'])
    try:
        paragraphs, page_count, error = parsed.get(key, ([], 0, 'Not returned by the parser'))
        text = to_text(paragraphs)
        if error or len(text) < MIN_TEXT_CHARS:
            raise ValueError(error or f'Only {len(text)} characters extracted')
        is_pdf = r['content_format'] == 'PDF'
        content_rows.append((
            r['document_id'], r['version'], r['regulator_id'], r['category_id'], r['content_format'],
            r['item_title'], r['item_date'], r['source_url'], r['document_url'], r['file_name'],
            r['internal_file_path'], r['content_hash_sha256'], text,
            [{'page_number': p, 'text': t} for p, t in pages_of(paragraphs)] if is_pdf else [],
            page_count or None, PARSER_PDF if is_pdf else PARSER_HTML,
            PDF_PARSER_VERSION if is_pdf else HTML_PARSER_VERSION, 'PARSED', None, True, RUN_ID, ts))
        for i, c in enumerate(make_chunks(paragraphs, CHUNK_CHARS, OVERLAP_CHARS)):
            chunk_rows.append((
                f"{r['document_id']}_v{r['version']}_{i:04d}", r['document_id'], r['version'], r['regulator_id'],
                r['category_id'], r['item_title'], r['item_date'], r['item_reference'], r['source_url'],
                r['document_url'], r['content_format'], r['file_name'], i, c['section_title'] or None,
                c['page_from'], c['page_to'], c['chunk_text'], len(c['chunk_text']), RUN_ID, ts))
        done.append(key)
        logger.info('PARSED %s v%s', r['file_name'], r['version'])
    except Exception as error:
        failed.append((r['document_id'], r['version'], f'{type(error).__name__}: {error}'[:4000]))
        logger.error('FAILED to parse %s: %s', r['file_name'], error)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7: Save Silver and Gold, and mark the promotion log
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Save the text of each version in Silver (older versions get is_current false), replace the chunks of each document in Gold with the chunks of the new version, and mark each document INGESTED or FAILED. |
# MAGIC | **Input** | `content_rows`, `chunk_rows`, `done`, `failed` |
# MAGIC | **Output** | `rcm_doc_content`, `rcm_doc_chunk`, ingest_status in `rcm_promotion_log` |

# COMMAND ----------

if done:
    spark.createDataFrame(done, 'document_id string, version int').createOrReplaceTempView('done_keys')
    run_sql_with_retry(spark, f'''MERGE INTO {CONTENT_TABLE} t USING done_keys k
                                  ON t.document_id = k.document_id AND t.version = k.version
                                  WHEN MATCHED THEN DELETE''', what='content delete')
    run_sql_with_retry(spark, f'''UPDATE {CONTENT_TABLE} SET is_current = false
                                  WHERE document_id IN (SELECT document_id FROM done_keys)''', what='content current flag')
    spark.createDataFrame(content_rows, spark.table(CONTENT_TABLE).schema).createOrReplaceTempView('new_content')
    run_sql_with_retry(spark, f'INSERT INTO {CONTENT_TABLE} SELECT * FROM new_content', what='content insert')

    run_sql_with_retry(spark, f'DELETE FROM {CHUNK_TABLE} WHERE document_id IN (SELECT document_id FROM done_keys)',
                       what='chunk delete')
    if chunk_rows:
        spark.createDataFrame(chunk_rows, spark.table(CHUNK_TABLE).schema).createOrReplaceTempView('new_chunks')
        run_sql_with_retry(spark, f'INSERT INTO {CHUNK_TABLE} SELECT * FROM new_chunks', what='chunk insert')

    run_sql_with_retry(spark, f'''MERGE INTO {PROMOTION_LOG} t USING done_keys k
                                  ON t.document_id = k.document_id AND t.version = k.version
                                  WHEN MATCHED THEN UPDATE SET ingest_status = 'INGESTED', ingest_error_details = NULL,
                                                               updated_at_sgt = :ts''', {'ts': now_sgt()},
                       what='promotion log ingested')

for document_id, version, error in failed:
    run_sql_with_retry(spark, f'''UPDATE {PROMOTION_LOG}
                                  SET ingest_status = 'FAILED', ingest_error_details = :error, updated_at_sgt = :ts
                                  WHERE document_id = :document_id AND version = :version''',
                       {'error': error, 'ts': now_sgt(), 'document_id': document_id, 'version': version},
                       what='promotion log failed')

logger.info('%s parsed, %s failed, %s chunks written', len(done), len(failed), len(chunk_rows))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8: Save the run log and set the final status
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Copy the run log to the logs volume, and fail the task if any document failed. The documents that succeeded are already saved, so the index sync and the analysis still run for them (the next tasks use Run if All done). |
# MAGIC | **Input** | `done`, `failed`, `chunk_rows`, `run_log` |
# MAGIC | **Output** | Log file `vol_logs/ingest/ALL/<day>/<run_id>.log`, task result |

# COMMAND ----------

run_log.publish()
if failed:
    raise RuntimeError(f'{len(failed)} documents failed to ingest. See {PROMOTION_LOG} where ingest_status = FAILED.')
dbutils.notebook.exit(f'parsed={len(done)} failed={len(failed)} chunks={len(chunk_rows)}')
