# Databricks notebook source
# MAGIC %md
# MAGIC # 02_ingest_rcm_copy
# MAGIC
# MAGIC | Item | Detail |
# MAGIC |---|---|
# MAGIC | **Purpose** | Copies only the documents with the malware scan verdict `CLEAN` from the external landing storage into the internal documents volume. It is the one way gate between the external zone (internet facing) and the internal zone. |
# MAGIC | **Job and task** | `job_sng_rcm_copy_daily`, task `01_copy` (then task `02_run_ingest` starts the ingest job) |
# MAGIC | **Parameters** | `regulator_id` (default `ALL`), `max_files` (default 500). Optional `env` and `config_dir`. |
# MAGIC | **Inputs** | External `rcm_pdf_catalog` (read only), files in the landing Blob Storage (read only), `rcm_promotion_log` (what is already copied) |
# MAGIC | **Outputs** | Files in `vol_documents`, one row per document version in `rcm_promotion_log` (COPIED with ingest_status PENDING, or FAILED), one run log file |
# MAGIC | **Compute** | Single node classic job cluster, Databricks Runtime 17.3 LTS |
# MAGIC | **Dependencies** | Wheel `rcm_intelligence`, cluster environment variables `RCM_ENV` and `RCM_CONFIG_DIR`, Unity Catalog service credential with read access to the landing storage |
# MAGIC | **Writes to the external zone** | Nothing |
# MAGIC | **Fails when** | Any file fails to copy (the others are still copied). A file is tried at most `copy.max_retries` times. |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Import the package
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Load the shared code from the `rcm_intelligence` wheel installed on the job cluster. No folder paths are used. |
# MAGIC | **Input** | Wheel `rcm_intelligence` |
# MAGIC | **Output** | Imported functions, the logger. The Spark session time zone is set to Singapore. |

# COMMAND ----------

import os

from rcm_intelligence.common import new_run_id, now_sgt, run_sql_with_retry, sha256_bytes
from rcm_intelligence.logging_setup import RunLog, get_logger, set_log_context
from rcm_intelligence.settings import external_pdf_catalog, full_name, load_config, log_root, volume_path

spark.conf.set('spark.sql.session.timeZone', 'Asia/Singapore')
logger = get_logger()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Read the parameters and the config
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Read the run choices and load every name from the config: the external catalog, the landing storage, the volume and the promotion log. Then start the run log. |
# MAGIC | **Input** | Widgets `regulator_id`, `max_files`, `env`, `config_dir`; `common_config_<env>.yaml` |
# MAGIC | **Output** | Table and path names, `CLEAN_VALUE`, `MAX_RETRIES`, `RUN_ID`, `run_log` |

# COMMAND ----------

dbutils.widgets.text('regulator_id', 'ALL')    # ALL, or one regulator such as MAS
dbutils.widgets.text('max_files', '500')       # use a small number for testing
dbutils.widgets.text('env', '')                # empty: use the cluster environment variable RCM_ENV
dbutils.widgets.text('config_dir', '')         # empty: use the cluster environment variable RCM_CONFIG_DIR

REGULATOR_ID = dbutils.widgets.get('regulator_id').strip().upper() or 'ALL'
MAX_FILES = int(dbutils.widgets.get('max_files').strip() or 500)
CFG = load_config(dbutils.widgets.get('env').strip() or None, dbutils.widgets.get('config_dir').strip() or None)

PDF_CATALOG = external_pdf_catalog(CFG)                      # external, read only
PROMOTION_LOG = full_name(CFG, 'tables', 'promotion_log')
TARGET_ROOT = volume_path(CFG, 'documents')
LANDING = CFG['landing']
CLEAN_VALUE = LANDING['clean_verdict']                       # CLEAN: the only verdict that may be copied
MAX_RETRIES = int(CFG['copy']['max_retries'])

RUN_ID = new_run_id('copy')
set_log_context(stage='COPY', regulator_id=REGULATOR_ID, run_id=RUN_ID)
run_log = RunLog(log_root(CFG, 'copy'), 'COPY', REGULATOR_ID, RUN_ID).start()
logger.info('Environment %s | config %s | from %s to %s', CFG['_env'], CFG['_path'], PDF_CATALOG, TARGET_ROOT)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Build the work list
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Pick the current version of every `CLEAN` document that is not copied yet, plus earlier failures that have not reached the retry limit. |
# MAGIC | **Input** | External `rcm_pdf_catalog`, `rcm_promotion_log` |
# MAGIC | **Output** | `todo`: up to `max_files` rows, oldest first |

# COMMAND ----------

REG_FILTER = '' if REGULATOR_ID == 'ALL' else 'AND c.regulator_id = :regulator_id'
todo = spark.sql(f'''
    SELECT c.document_id, c.current_version AS version, c.regulator_id, c.category_id,
           c.content_format, c.file_name, c.source_url, c.raw_file_path,
           c.content_hash_sha256, c.malware_scan_verdict,
           coalesce(p.copy_retry_count, 0) AS copy_retry_count
    FROM {PDF_CATALOG} c
    LEFT JOIN {PROMOTION_LOG} p
      ON p.document_id = c.document_id AND p.version = c.current_version
    WHERE c.malware_scan_verdict = :clean
      AND (p.document_id IS NULL OR (p.copy_status = 'FAILED' AND p.copy_retry_count < :max_retries))
      {REG_FILTER}
    ORDER BY c.last_changed_sgt
    LIMIT {MAX_FILES}
''', args={'clean': CLEAN_VALUE, 'max_retries': MAX_RETRIES,
           **({} if REGULATOR_ID == 'ALL' else {'regulator_id': REGULATOR_ID})}).collect()
logger.info('%s files to copy', len(todo))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Prepare the copy functions
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Connect to the landing container with the managed identity, and define how one file is copied: download, check the fingerprint against the catalog, write to the volume, read back and check again, then record the result. |
# MAGIC | **Input** | Landing block of the config, Unity Catalog service credential |
# MAGIC | **Output** | Functions `copy_one` and `record` |

# COMMAND ----------

from azure.storage.blob import BlobServiceClient

landing = (BlobServiceClient(f"https://{LANDING['storage_account']}.blob.core.windows.net",
                             credential=dbutils.credentials.getServiceCredentialsProvider(LANDING['service_credential']))
           .get_container_client(LANDING['container']))

UPSERT = f'''
    MERGE INTO {PROMOTION_LOG} t
    USING (SELECT :document_id AS document_id, :version AS version) s
    ON t.document_id = s.document_id AND t.version = s.version
    WHEN MATCHED THEN UPDATE SET
        copy_status = :status, copy_retry_count = :retries, copy_error_details = :error,
        internal_file_path = :target, content_hash_sha256 = :hash, copied_at_sgt = :copied_at,
        ingest_status = :ingest_status, run_id = :run_id, updated_at_sgt = :now
    WHEN NOT MATCHED THEN INSERT (
        document_id, version, regulator_id, category_id, content_format, file_name, source_url,
        raw_file_path, internal_file_path, content_hash_sha256, malware_scan_verdict, copy_status,
        copy_retry_count, copy_error_details, copied_at_sgt, ingest_status, run_id, updated_at_sgt)
    VALUES (
        :document_id, :version, :regulator_id, :category_id, :content_format, :file_name, :source_url,
        :raw_file_path, :target, :hash, :verdict, :status,
        :retries, :error, :copied_at, :ingest_status, :run_id, :now)
'''


def record(row, status, target=None, hash_=None, error=None):
    '''Save the result of one copy (COPIED or FAILED) in rcm_promotion_log.'''
    copied = status == 'COPIED'
    run_sql_with_retry(spark, UPSERT, {
        'document_id': row['document_id'], 'version': row['version'], 'regulator_id': row['regulator_id'],
        'category_id': row['category_id'], 'content_format': row['content_format'],
        'file_name': row['file_name'], 'source_url': row['source_url'], 'raw_file_path': row['raw_file_path'],
        'verdict': row['malware_scan_verdict'], 'status': status,
        'retries': row['copy_retry_count'] + (0 if copied else 1),
        'error': error, 'target': target, 'hash': hash_,
        'copied_at': now_sgt() if copied else None,
        'ingest_status': 'PENDING' if copied else None,
        'run_id': RUN_ID, 'now': now_sgt(),
    }, what='promotion log')


def write_and_verify(path, data, expected_hash):
    '''Write the file, read it back and check the fingerprint. Removes the file if the check fails.'''
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'wb') as handle:
        handle.write(data)
    with open(path, 'rb') as handle:
        written = sha256_bytes(handle.read())
    if written != expected_hash:
        os.remove(path)
        raise IOError(f'Hash mismatch after writing {path}')
    return written


def copy_one(row):
    '''Copy one CLEAN file. For a PDF the catalog holds the file fingerprint, so the download must
    match it (SOURCE_HASH_MISMATCH is a security event). For an HTML snapshot the catalog holds a
    fingerprint of the page text, so only the copy itself is verified.'''
    blob_name = row['raw_file_path']
    prefix = LANDING['prefix'].strip('/')
    if not blob_name.startswith(prefix + '/') or '..' in blob_name:
        raise ValueError(f'Unsafe or unexpected blob name: {blob_name}')
    target = TARGET_ROOT + blob_name[len(prefix):]
    data = landing.download_blob(blob_name).readall()
    source_hash = sha256_bytes(data)
    if row['content_format'] == 'PDF' and source_hash != row['content_hash_sha256']:
        raise ValueError('SOURCE_HASH_MISMATCH: the file does not match rcm_pdf_catalog')
    return target, write_and_verify(target, data, source_hash)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Copy the files
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Copy each file in the work list. A failure is recorded and the next file continues, so one bad file never blocks the others. |
# MAGIC | **Input** | `todo` |
# MAGIC | **Output** | Files in `vol_documents`, rows in `rcm_promotion_log`, counts `copied` and `failed` |

# COMMAND ----------

copied = failed = 0
for i, row in enumerate(todo, start=1):
    try:
        target, target_hash = copy_one(row)
        record(row, 'COPIED', target=target, hash_=target_hash)
        copied += 1
        logger.info('[%s of %s] COPIED %s v%s', i, len(todo), row['file_name'], row['version'])
    except Exception as error:
        failed += 1
        logger.error('[%s of %s] FAILED %s: %s', i, len(todo), row['file_name'], error)
        try:
            record(row, 'FAILED', error=f'{type(error).__name__}: {error}'[:4000])
        except Exception as log_error:
            logger.error('Could not record the failure: %s', log_error)

logger.info('Copy run %s: %s copied, %s failed', RUN_ID, copied, failed)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6: Save the run log and set the final status
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Copy the run log to the logs volume, and fail the task if any file failed (the job alert then fires). |
# MAGIC | **Input** | `copied`, `failed`, `run_log` |
# MAGIC | **Output** | Log file `vol_logs/copy/<regulator>/<day>/<run_id>.log`, task result |

# COMMAND ----------

run_log.publish()
if failed:
    raise RuntimeError(f'Copy run {RUN_ID}: {failed} files failed. See {PROMOTION_LOG} where copy_status = FAILED.')
dbutils.notebook.exit(f'{RUN_ID} copied={copied} failed={failed}')
