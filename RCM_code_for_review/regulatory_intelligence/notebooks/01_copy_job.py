# Databricks notebook source
# MAGIC %md
# MAGIC # Copy Job (Internal ADB workspace)
# MAGIC
# MAGIC 1. Reads `pdf_catalog` (read only) for files with the verdict No threats found.
# MAGIC 2. Skips versions already COPIED, and versions that failed 3 times, using its own `rcm_promotion_log`.
# MAGIC 3. Downloads the file from the landing blob container (managed identity) and checks its hash against `pdf_catalog`.
# MAGIC 4. Writes it to the internal volume, reads it back and checks the hash again.
# MAGIC 5. Records COPIED or FAILED in `rcm_promotion_log`, with ingest_status PENDING for C4.
# MAGIC
# MAGIC Needs Databricks Runtime 15.4 LTS or above, or serverless (Unity Catalog service credential).
# MAGIC Writes nothing in the external zone. Parameters: `env` (default dev), `regulator_id` (default ALL) and `max_files` (default 500).
# MAGIC All names (tables, storage, credential, volume) come from config/<env>.yaml through common/config.py.

# COMMAND ----------

import hashlib
import logging
import time
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

spark.conf.set('spark.sql.session.timeZone', 'Asia/Singapore')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('copy_job')
SGT = ZoneInfo('Asia/Singapore')


# Current time in Singapore, without time zone, as stored in the tables
def now_sgt():
    return datetime.now(SGT).replace(tzinfo=None)


# Fingerprint (SHA 256) of the file content, checked before and after the copy
def sha256_of(data):
    return hashlib.sha256(data).hexdigest()


def write_and_verify(path, data, expected_hash):
    """Write the file, read it back and check the hash. Removes the file if the check fails."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'wb') as f:
        f.write(data)
    with open(path, 'rb') as f:
        written = sha256_of(f.read())
    if written != expected_hash:
        os.remove(path)
        raise IOError(f'Hash mismatch after writing {path}')
    return written


def run_sql(sql, args, attempts=3):
    """Run a SQL statement, retrying briefly if another job is writing the same table."""
    for i in range(1, attempts + 1):
        try:
            return spark.sql(sql, args=args)
        except Exception:
            if i == attempts:
                raise
            time.sleep(3 * i)

# COMMAND ----------

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
PDF_CATALOG = external_table(CFG, 'pdf_catalog')                          # external, read only
LANDING_ACCOUNT = CFG['landing']['storage_account']                       # external landing blob storage, read only
LANDING_CONTAINER = CFG['landing']['container']
LANDING_PREFIX = CFG['landing']['prefix']
SERVICE_CREDENTIAL = CFG['landing']['service_credential']                 # internal workspace managed identity (read access)
TARGET_ROOT = volume_path(CFG)                                            # internal volume
PROMOTION_LOG = table(CFG, 'rcm_promotion_log')                           # internal table
CLEAN_VALUE = CFG['landing']['clean_verdict']                             # Defender verdict that allows a copy
MAX_RETRIES = CFG['copy']['max_retries']

dbutils.widgets.text('regulator_id', 'ALL')            # ALL, or one regulator such as MAS
dbutils.widgets.text('max_files', '500')               # use a small number for testing
REGULATOR_ID = dbutils.widgets.get('regulator_id').strip().upper() or 'ALL'
MAX_FILES = int(dbutils.widgets.get('max_files').strip() or 500)    # empty means the default of 500
REG_FILTER = '' if REGULATOR_ID == 'ALL' else 'AND c.regulator_id = :regulator_id'

RUN_ID = 'copy_' + now_sgt().strftime('%Y%m%d_%H%M%S') + '_' + uuid.uuid4().hex[:6]

# COMMAND ----------

# Work list: clean files whose current version is not yet copied and has not failed too often
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
logger.info('Copy run %s: %s files to copy', RUN_ID, len(todo))

# COMMAND ----------

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


# Save the result of one copy (COPIED or FAILED) in rcm_promotion_log
def record(row, status, target=None, hash_=None, error=None):
    copied = status == 'COPIED'
    args = {
        'document_id': row['document_id'], 'version': row['version'], 'regulator_id': row['regulator_id'],
        'category_id': row['category_id'], 'content_format': row['content_format'],
        'file_name': row['file_name'], 'source_url': row['source_url'], 'raw_file_path': row['raw_file_path'],
        'verdict': row['malware_scan_verdict'], 'status': status,
        'retries': row['copy_retry_count'] + (0 if copied else 1),
        'error': error, 'target': target, 'hash': hash_,
        'copied_at': now_sgt() if copied else None,
        'ingest_status': 'PENDING' if copied else None,
        'run_id': RUN_ID, 'now': now_sgt(),
    }
    run_sql(UPSERT, args)


from azure.storage.blob import BlobServiceClient

landing = (BlobServiceClient(f'https://{LANDING_ACCOUNT}.blob.core.windows.net',
                             credential=dbutils.credentials.getServiceCredentialsProvider(SERVICE_CREDENTIAL))
           .get_container_client(LANDING_CONTAINER))


# Copy one clean file: download from the landing blob, check the fingerprint, write it and verify it
def copy_one(row):
    blob_name = row['raw_file_path']          # blob name in the landing container
    if not blob_name.startswith(LANDING_PREFIX + '/') or '..' in blob_name:
        raise ValueError(f'Unsafe or unexpected blob name: {blob_name}')
    target = TARGET_ROOT + blob_name[len(LANDING_PREFIX):]
    data = landing.download_blob(blob_name).readall()
    source_hash = sha256_of(data)
    # For PDFs, pdf_catalog holds the hash of the file, so the source must match it.
    # For HTML snapshots, pdf_catalog holds a fingerprint of the page text (used by the scraper
    # to detect real content changes), not of the file, so only the copy itself is verified.
    if row['content_format'] == 'PDF' and source_hash != row['content_hash_sha256']:
        raise ValueError('Source hash does not match pdf_catalog')
    return target, write_and_verify(target, data, source_hash)

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

if failed:
    raise RuntimeError(f'Copy run {RUN_ID}: {failed} files failed. See rcm_promotion_log where copy_status = FAILED.')
dbutils.notebook.exit(f'{RUN_ID} copied={copied} failed={failed}')
