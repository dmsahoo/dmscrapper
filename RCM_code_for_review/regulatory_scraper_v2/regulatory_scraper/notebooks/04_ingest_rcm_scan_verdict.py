# Databricks notebook source
# MAGIC %md
# MAGIC # 04_ingest_rcm_scan_verdict
# MAGIC
# MAGIC | Item | Detail |
# MAGIC |---|---|
# MAGIC | **Purpose** | Reads the Microsoft Defender for Storage malware scan result of each saved document and records a fixed verdict in the catalog: `PENDING`, `CLEAN`, `MALICIOUS` or `SCAN_ERROR`. Only `CLEAN` documents can be copied to the internal zone. |
# MAGIC | **Job and task** | `job_sng_rcm_scan_verdict_daily`, task `01_scan_verdict` (runs after the scrape job, before the internal copy job) |
# MAGIC | **Parameters** | `regulator_id` (default `ALL`), `max_files` (default 1000). Optional `env` and `config_dir`. |
# MAGIC | **Inputs** | `rcm_pdf_catalog` rows with verdict `PENDING` or `SCAN_ERROR` (or empty, from before the mapping), Defender blob index tags in the landing container, the `malware_scan` block of the config |
# MAGIC | **Outputs** | `malware_scan_verdict`, `malware_scan_result_raw` and `scan_timestamp_sgt` in `rcm_pdf_catalog`, RUN and FILE rows (stage SCAN) in `rcm_scraping_audit_log`, one run log file |
# MAGIC | **Compute** | Small single node classic job cluster, Databricks Runtime 15.4 LTS or above. No Playwright needed. |
# MAGIC | **Dependencies** | Wheel `rcm_scraper`, cluster environment variables `RCM_ENV` and `RCM_CONFIG_DIR`, Unity Catalog service credential with tag read access on the landing storage |
# MAGIC | **Fails when** | A document gets a new `MALICIOUS` or `SCAN_ERROR` verdict in this run (security alert), or every file failed |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Import the scraper package
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Load the shared code from the `rcm_scraper` wheel installed on the job cluster. No folder paths are used. |
# MAGIC | **Input** | Wheel `rcm_scraper` |
# MAGIC | **Output** | Imported functions and classes, the logger. The Spark session time zone is set to Singapore. |

# COMMAND ----------

from rcm_scraper.audit import AUDIT_COLUMNS, AuditWriter
from rcm_scraper.blob_storage import landing_store
from rcm_scraper.catalog import CATALOG_COLUMNS, validate_table_schema
from rcm_scraper.common import FAILED, PARTIAL_SUCCESS, RUNNING, SUCCESS, new_run_id, now_sgt, run_with_delta_retry
from rcm_scraper.logging_setup import RunLog, get_logger, set_log_context
from rcm_scraper.scan_verdict import (CLEAN, MALICIOUS, PENDING, RECHECK_VERDICTS, SCAN_ERROR,
                                      map_verdict, parse_scan_time)
from rcm_scraper.settings import ConfigError, full_table_name, load_common_config, log_root

spark.conf.set('spark.sql.session.timeZone', 'Asia/Singapore')
logger = get_logger()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Read the parameters
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Read which regulator to check and how many files at most. |
# MAGIC | **Input** | Job parameters or notebook widgets |
# MAGIC | **Output** | `REGULATOR_ID`, `MAX_FILES`, `PARAM_ENV`, `PARAM_CONFIG_DIR` |

# COMMAND ----------

dbutils.widgets.text('regulator_id', 'ALL')   # ALL, or one regulator such as MAS
dbutils.widgets.text('max_files', '1000')
dbutils.widgets.text('env', '')               # empty: use the cluster environment variable RCM_ENV
dbutils.widgets.text('config_dir', '')        # empty: use the cluster environment variable RCM_CONFIG_DIR

REGULATOR_ID = dbutils.widgets.get('regulator_id').strip().upper() or 'ALL'
MAX_FILES = int(dbutils.widgets.get('max_files').strip() or 1000)
PARAM_ENV = dbutils.widgets.get('env').strip() or None
PARAM_CONFIG_DIR = dbutils.widgets.get('config_dir').strip() or None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Load the configuration and check the tables
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Load the common config (table names, landing storage, verdict mapping), connect to the landing container, and check that both tables match the code. Then start the run log. |
# MAGIC | **Input** | `common_config_<env>.yaml`, `rcm_pdf_catalog`, `rcm_scraping_audit_log` |
# MAGIC | **Output** | `CATALOG_TABLE`, `AUDIT_TABLE`, `SCAN_CONFIG`, `store`, `RUN_ID`, `run_log` |

# COMMAND ----------

try:
    common = load_common_config(PARAM_ENV, PARAM_CONFIG_DIR)
except ConfigError as error:
    logger.error('Configuration error: %s', error)
    raise

CATALOG_TABLE = full_table_name(common, 'pdf_catalog')
AUDIT_TABLE = full_table_name(common, 'audit_log')
SCAN_CONFIG = common['malware_scan']
PENDING_ALERT_HOURS = float(SCAN_CONFIG.get('pending_alert_hours', 24))
LANDING = common['landing']
store = landing_store(LANDING, dbutils.credentials.getServiceCredentialsProvider(LANDING['service_credential']))

RUN_ID = new_run_id('scan')
RUN_STARTED = now_sgt()
set_log_context(stage='SCAN', regulator_id=REGULATOR_ID, run_id=RUN_ID)
run_log = RunLog(log_root(common), 'SCAN', REGULATOR_ID, RUN_ID).start()

try:
    validate_table_schema(spark, CATALOG_TABLE, CATALOG_COLUMNS)
    validate_table_schema(spark, AUDIT_TABLE, AUDIT_COLUMNS)
except Exception as error:
    logger.error('Table check failed: %s', error)
    run_log.publish()
    raise

logger.info('Environment %s | config %s | tables %s, %s', common['_env'], common['_path'], CATALOG_TABLE, AUDIT_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Build the work list
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Pick the documents whose verdict is not final yet: `PENDING`, `SCAN_ERROR` (checked again, in case the operations team asked Defender to scan the file again), and empty (rows saved before the verdict mapping). `CLEAN` and `MALICIOUS` are final and never read again. |
# MAGIC | **Input** | `rcm_pdf_catalog` |
# MAGIC | **Output** | `todo`: up to `max_files` rows, oldest first |

# COMMAND ----------

REG_FILTER = '' if REGULATOR_ID == 'ALL' else 'AND regulator_id = :regulator_id'
REG_ARGS = {} if REGULATOR_ID == 'ALL' else {'regulator_id': REGULATOR_ID}
RECHECK_LIST = ', '.join(f"'{value}'" for value in RECHECK_VERDICTS)

todo = spark.sql(f'''
    SELECT document_id, regulator_id, category_id, file_name, current_version, raw_file_path,
           malware_scan_verdict, malware_scan_result_raw, last_changed_sgt
    FROM {CATALOG_TABLE}
    WHERE (malware_scan_verdict IS NULL OR malware_scan_verdict IN ({RECHECK_LIST})) {REG_FILTER}
    ORDER BY last_changed_sgt
    LIMIT {MAX_FILES}
''', args=REG_ARGS).collect()
logger.info('Scan run %s: %s documents to check', RUN_ID, len(todo))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Read the Defender tags and record the verdicts
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | For each document: read its blob index tags, map the Defender text to a fixed verdict with the config mapping, and save the verdict, the original text and the Defender scan time. A file without a tag stays `PENDING`; a warning is logged when it waits longer than `pending_alert_hours`. |
# MAGIC | **Input** | `todo`, blob index tags, `SCAN_CONFIG` |
# MAGIC | **Output** | Updated rows in `rcm_pdf_catalog`, FILE rows in `rcm_scraping_audit_log`, counts per verdict |

# COMMAND ----------

SET_VERDICT = f'''
    UPDATE {CATALOG_TABLE}
    SET malware_scan_verdict = :verdict, malware_scan_result_raw = :raw, scan_timestamp_sgt = :scanned_at
    WHERE document_id = :document_id AND current_version = :version
'''

audit = AuditWriter(spark, AUDIT_TABLE, RUN_ID, REGULATOR_ID, 'SCAN')
audit.add('RUN', RUNNING, start_time_sgt=RUN_STARTED, documents_found=len(todo))
audit.flush()

counts = {PENDING: 0, CLEAN: 0, MALICIOUS: 0, SCAN_ERROR: 0}
failed = overdue = 0
new_alerts = []                         # documents that became MALICIOUS or SCAN_ERROR in this run

for row in todo:
    fields = dict(regulator_id=row['regulator_id'], category_id=row['category_id'],
                  document_id=row['document_id'], file_name=row['file_name'],
                  version=row['current_version'], raw_file_path=row['raw_file_path'])
    try:
        tags = store.read_tags(row['raw_file_path'])
        raw = tags.get(SCAN_CONFIG['tag_name'])
        verdict = map_verdict(raw, SCAN_CONFIG)
        scanned_at = parse_scan_time(tags.get(SCAN_CONFIG.get('time_tag_name'))) if raw else None
        counts[verdict] += 1

        changed = (verdict != row['malware_scan_verdict']) or (raw != row['malware_scan_result_raw'])
        if changed:
            run_with_delta_retry(lambda: spark.sql(SET_VERDICT, args={
                'verdict': verdict, 'raw': raw,
                'scanned_at': scanned_at or (now_sgt() if raw else None),
                'document_id': row['document_id'], 'version': row['current_version']}), what='set verdict')

        if verdict == PENDING:
            age_hours = (now_sgt() - row['last_changed_sgt']).total_seconds() / 3600 if row['last_changed_sgt'] else 0
            if age_hours > PENDING_ALERT_HOURS:
                overdue += 1
                logger.warning('Still PENDING after %.1f hours: %s', age_hours, row['file_name'])
            audit.add('FILE', 'WAITING', **fields, malware_scan_verdict=verdict,
                      error_details='No scan result tag yet')
            continue

        if verdict in (MALICIOUS, SCAN_ERROR) and row['malware_scan_verdict'] != verdict:
            new_alerts.append(f"{row['file_name']} ({verdict}: {raw})")
            logger.warning('%s %s: Defender result %r', verdict, row['file_name'], raw)
        audit.add('FILE', 'SCANNED', **fields, malware_scan_verdict=verdict, malware_scan_result_raw=raw)
    except Exception as error:
        failed += 1
        logger.error('FAILED %s: %s', row['file_name'], error)
        audit.add('FILE', FAILED, **fields, error_category=type(error).__name__, error_details=str(error))

checked = sum(counts.values())
run_status = FAILED if failed and not checked else PARTIAL_SUCCESS if failed else SUCCESS
summary = (f"{counts[CLEAN]} clean, {counts[MALICIOUS]} malicious, {counts[SCAN_ERROR]} scan error, "
           f"{counts[PENDING]} pending ({overdue} over {PENDING_ALERT_HOURS:g} hours), {failed} failed")
audit.add('RUN', run_status, start_time_sgt=RUN_STARTED, end_time_sgt=now_sgt(),
          documents_found=len(todo), documents_failed=failed, error_details=summary)
audit.flush()
logger.info('Scan run %s %s: %s', RUN_ID, run_status, summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6: Save the run log and set the final status
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Copy the run log to the logs volume. Fail the task when a document became `MALICIOUS` or `SCAN_ERROR` in this run (security alert), or when every file failed. Long `PENDING` files and old `SCAN_ERROR` files are reported by the SQL alerts on `v_rcm_scan_backlog`. |
# MAGIC | **Input** | `new_alerts`, `run_status`, `run_log` |
# MAGIC | **Output** | Log file `<logs volume>/scraper/<regulator>/<day>/<run_id>.log`, task result SUCCESS or failed |

# COMMAND ----------

run_log.publish()

if new_alerts:
    raise RuntimeError(f'Security alert: {len(new_alerts)} documents are not clean and will never be copied: '
                       f'{"; ".join(new_alerts[:20])}. See {CATALOG_TABLE}.malware_scan_verdict.')
if run_status == FAILED:
    raise RuntimeError(f'Scan run {RUN_ID} FAILED. See {AUDIT_TABLE} stage SCAN.')

dbutils.notebook.exit(f'{RUN_ID} {run_status} {summary}')
