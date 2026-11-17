# Databricks notebook source
# MAGIC %md
# MAGIC # 03_ingest_rcm_scrape
# MAGIC
# MAGIC | Item | Detail |
# MAGIC |---|---|
# MAGIC | **Purpose** | Scrapes one regulator for one scan period: finds the publications, downloads new and changed documents, saves each version in the landing storage, and records everything in the catalog and audit tables. |
# MAGIC | **Job and task** | `job_sng_rcm_scrape_daily`, For each task `02_scrape_regulators` (one run per regulator) |
# MAGIC | **Parameters** | `regulator_id` (required, for example `MAS`), `scan_mode` (`DEFAULT`, `INCREMENTAL`, `CUSTOM`, `LAST_N_DAYS`, `PREVIOUS_MONTH`, `MONTH_TO_DATE`), `scan_start_date` and `scan_end_date` (CUSTOM only, `YYYY-MM-DD`), `last_n_days` (LAST_N_DAYS only). Optional `env` and `config_dir`. |
# MAGIC | **Inputs** | `config/common_config_<env>.yaml`, `config/regulators/<regulator>_config.yaml`, the regulator website, table `rcm_pdf_catalog` (known documents) |
# MAGIC | **Outputs** | New document versions in the landing Blob Storage, rows in `rcm_pdf_catalog` (verdict `PENDING`) and `rcm_scraping_audit_log`, one run log file in the logs volume |
# MAGIC | **Compute** | Single node classic job cluster, Databricks Runtime 15.4 LTS or above, init script `playwright_init.sh` (Playwright and Microsoft Edge) |
# MAGIC | **Dependencies** | Wheel `rcm_scraper`, cluster environment variables `RCM_ENV` and `RCM_CONFIG_DIR`, Unity Catalog service credential for the landing storage |
# MAGIC | **Fails when** | The run is FAILED, or PARTIAL_SUCCESS while `fail_on_partial` is true in the config |

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
from rcm_scraper.base_scraper import BaseScraper, check_browser
from rcm_scraper.blob_storage import landing_store
from rcm_scraper.catalog import CATALOG_COLUMNS, CatalogRepository, validate_table_schema
from rcm_scraper.common import (FAILED, PARTIAL_SUCCESS, RUNNING, SCAN_MODES, last_successful_scan_end,
                                new_run_id, now_sgt, resolve_scan_period)
from rcm_scraper.logging_setup import RunLog, get_logger, set_log_context
from rcm_scraper.run_guard import RunGuard
from rcm_scraper.settings import (ConfigError, full_table_name, get_regulator_entry, load_common_config,
                                  load_scraper_class, log_root, regulator_config_path)

spark.conf.set('spark.sql.session.timeZone', 'Asia/Singapore')
logger = get_logger()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Read the parameters
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Read the run choices. Table names and paths never come from parameters, only from the config. |
# MAGIC | **Input** | Job parameters or notebook widgets |
# MAGIC | **Output** | `PARAM_REGULATOR`, `PARAM_SCAN_MODE`, `PARAM_START`, `PARAM_END`, `PARAM_LAST_N_DAYS`, `PARAM_ENV`, `PARAM_CONFIG_DIR` |

# COMMAND ----------

dbutils.widgets.text('regulator_id', '')
dbutils.widgets.dropdown('scan_mode', 'DEFAULT', ['DEFAULT'] + list(SCAN_MODES))
dbutils.widgets.text('scan_start_date', '')
dbutils.widgets.text('scan_end_date', '')
dbutils.widgets.text('last_n_days', '')
dbutils.widgets.text('env', '')             # empty: use the cluster environment variable RCM_ENV
dbutils.widgets.text('config_dir', '')      # empty: use the cluster environment variable RCM_CONFIG_DIR

PARAM_REGULATOR = dbutils.widgets.get('regulator_id').strip()
PARAM_SCAN_MODE = dbutils.widgets.get('scan_mode').strip()
PARAM_START = dbutils.widgets.get('scan_start_date').strip()
PARAM_END = dbutils.widgets.get('scan_end_date').strip()
PARAM_LAST_N_DAYS = dbutils.widgets.get('last_n_days').strip()
PARAM_ENV = dbutils.widgets.get('env').strip() or None
PARAM_CONFIG_DIR = dbutils.widgets.get('config_dir').strip() or None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Load the configuration
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Load the common config and the regulator config, and import the regulator scraper class. Any problem stops the notebook before anything is written. |
# MAGIC | **Input** | `common_config_<env>.yaml`, `regulators/<regulator>_config.yaml` |
# MAGIC | **Output** | `common`, `config`, `ScraperClass`, the table names, the landing store and the run settings |

# COMMAND ----------

try:
    common = load_common_config(PARAM_ENV, PARAM_CONFIG_DIR)
    REGULATOR_ID, registry_entry = get_regulator_entry(common, PARAM_REGULATOR)
    set_log_context(stage='SCRAPE', regulator_id=REGULATOR_ID)
    ScraperClass = load_scraper_class(registry_entry, BaseScraper)
    REGULATOR_CONFIG_PATH = regulator_config_path(common, registry_entry)
    config = ScraperClass.load_config(REGULATOR_CONFIG_PATH)
    if config['regulator_id'] != REGULATOR_ID:
        raise ConfigError(f'{REGULATOR_CONFIG_PATH} has regulator_id {config["regulator_id"]}, '
                          f'expected {REGULATOR_ID}')
except (ConfigError, ValueError) as error:
    logger.error('Configuration error: %s', error)
    raise

if not config.get('is_active', True):
    dbutils.notebook.exit(f'{REGULATOR_ID} is inactive in {REGULATOR_CONFIG_PATH}, nothing to do')

DEFAULTS = common['defaults']
CATALOG_TABLE = full_table_name(common, 'pdf_catalog')
AUDIT_TABLE = full_table_name(common, 'audit_log')
LANDING = common['landing']
store = landing_store(LANDING, dbutils.credentials.getServiceCredentialsProvider(LANDING['service_credential']))
FAIL_ON_PARTIAL = bool(DEFAULTS['fail_on_partial'])
STALE_RUN_HOURS = float(DEFAULTS['stale_run_hours'])

logger.info('Environment %s | config %s | scraper %s | tables %s, %s',
            common['_env'], common['_path'], ScraperClass.__name__, CATALOG_TABLE, AUDIT_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Resolve the scan period and start the run log
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Work out the dates to scan. INCREMENTAL starts a few days (overlap) before the end of the last successful run. Then create the run identifier and start the run log file. |
# MAGIC | **Input** | `scan_mode` and dates, defaults from the config, the last successful run in `rcm_scraping_audit_log` |
# MAGIC | **Output** | `SCAN_START`, `SCAN_END`, `RUN_ID`, `run_log` (writing to the local disk until Step 8) |

# COMMAND ----------

try:
    SCAN_MODE = DEFAULTS['scan_mode'] if PARAM_SCAN_MODE in ('', 'DEFAULT') else PARAM_SCAN_MODE
    WATERMARK = None
    if SCAN_MODE == 'INCREMENTAL':
        validate_table_schema(spark, AUDIT_TABLE, AUDIT_COLUMNS)
        WATERMARK = last_successful_scan_end(spark, AUDIT_TABLE, REGULATOR_ID)
    SCAN_START, SCAN_END = resolve_scan_period(
        SCAN_MODE, now_sgt().date(),
        last_n_days=PARAM_LAST_N_DAYS or DEFAULTS['last_n_days'],
        start_text=PARAM_START, end_text=PARAM_END,
        last_successful_end=WATERMARK,
        overlap_days=DEFAULTS['overlap_days'],
        max_window_days=DEFAULTS['max_window_days'])
except ValueError as error:
    logger.error('Scan period error for %s: %s', REGULATOR_ID, error)
    raise

RUN_ID = new_run_id(f'scrape_{REGULATOR_ID.lower()}')
RUN_STARTED = now_sgt()
run_log = RunLog(log_root(common), 'SCRAPE', REGULATOR_ID, RUN_ID).start()
logger.info('Run %s | mode %s | scan period %s to %s | last successful end %s',
            RUN_ID, SCAN_MODE, SCAN_START, SCAN_END, WATERMARK)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Pre flight checks
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Check everything the run needs before touching the website: the tables match the code, the landing container is writable, Microsoft Edge starts, and no other run for this regulator is in progress. |
# MAGIC | **Input** | `rcm_pdf_catalog`, `rcm_scraping_audit_log`, landing container, Microsoft Edge |
# MAGIC | **Output** | Pass, or the run stops with a clear message (the run log is saved first) |

# COMMAND ----------

try:
    validate_table_schema(spark, CATALOG_TABLE, CATALOG_COLUMNS)
    validate_table_schema(spark, AUDIT_TABLE, AUDIT_COLUMNS)

    try:
        store.check_writable(RUN_ID)
    except Exception as error:
        raise RuntimeError(f"Landing container {LANDING['storage_account']}/{LANDING['container']} "
                           f"is not writable with {LANDING['service_credential']}: {error}")
    logger.info('Landing container %s/%s is writable', LANDING['storage_account'], LANDING['container'])

    BROWSER_PATH, BROWSER_VERSION = await check_browser()
    logger.info('Microsoft Edge %s found at %s', BROWSER_VERSION, BROWSER_PATH)

    RunGuard(spark, AUDIT_TABLE, REGULATOR_ID, 'SCRAPE', STALE_RUN_HOURS).check_can_start(RUN_ID)
except Exception as error:
    logger.error('Pre flight check failed for %s: %s', REGULATOR_ID, error)
    run_log.publish()
    raise

logger.info('Pre flight checks passed')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6: Run the scraper
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Scrape every enabled category. For each document: download, fingerprint, decide NEW, CHANGED or SKIPPED, save new versions with a verified write, and update the catalog (verdict `PENDING`). A final RUN row is always written, even when the run fails. |
# MAGIC | **Input** | Regulator website, scan period, known documents |
# MAGIC | **Output** | Files in the landing container, rows in `rcm_pdf_catalog` and `rcm_scraping_audit_log`, `final_status` and `counters` |

# COMMAND ----------

audit = AuditWriter(spark, AUDIT_TABLE, RUN_ID, REGULATOR_ID, 'SCRAPE', SCAN_START, SCAN_END)
catalog_repo = CatalogRepository(spark, CATALOG_TABLE)

audit.add('RUN', RUNNING, start_time_sgt=RUN_STARTED, source_url=config['landing_url'])
audit.flush()

result = {'status': FAILED, 'failed_categories': [], 'counters': {}}
run_error = None
try:
    scraper = ScraperClass(config, RUN_ID, store, catalog_repo, audit, logger)
    result = await scraper.run(SCAN_START, SCAN_END)
except Exception as error:
    run_error = error
    logger.exception('Run %s failed', RUN_ID)
finally:
    counters = result.get('counters') or {}
    failed_categories = result.get('failed_categories') or []
    final_status = FAILED if run_error else result['status']
    if run_error:
        error_category, error_details = type(run_error).__name__, str(run_error)
    elif failed_categories:
        error_category, error_details = 'CATEGORY_FAILURE', ', '.join(failed_categories)
    else:
        error_category, error_details = None, None
    try:
        audit.add('RUN', final_status, start_time_sgt=RUN_STARTED, end_time_sgt=now_sgt(),
                  source_url=config['landing_url'],
                  items_found=counters.get('items_found'), documents_found=counters.get('documents_found'),
                  documents_new=counters.get('documents_new'), documents_changed=counters.get('documents_changed'),
                  documents_skipped=counters.get('documents_skipped'),
                  documents_failed=counters.get('documents_failed'),
                  error_category=error_category, error_details=error_details)
        audit.flush()
    except Exception as audit_error:
        logger.error('Could not write the final RUN row for %s: %s. The next run closes it as ABANDONED.',
                     RUN_ID, audit_error)
        final_status = FAILED
        error_details = f'{error_details or ""} Final RUN row not written: {audit_error}'.strip()

logger.info('Run %s finished with %s | %s', RUN_ID, final_status, counters)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7: Coverage summary for this run
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Show what was found per category, and the file decisions, for a quick check by the operator. Information only: an error here does not change the run result. |
# MAGIC | **Input** | `rcm_scraping_audit_log` rows of this run |
# MAGIC | **Output** | Two tables in the notebook output |

# COMMAND ----------

try:
    display(spark.sql(f'''
        SELECT category_id, category_name, status, items_found, documents_found,
               documents_new, documents_changed, documents_skipped, documents_failed, error_details
        FROM {AUDIT_TABLE}
        WHERE run_id = :run_id AND record_type = 'CATEGORY'
        ORDER BY category_id
    ''', args={'run_id': RUN_ID}))

    display(spark.sql(f'''
        SELECT status, skip_reason, error_category, count(*) AS files
        FROM {AUDIT_TABLE}
        WHERE run_id = :run_id AND record_type = 'FILE'
        GROUP BY status, skip_reason, error_category
        ORDER BY status
    ''', args={'run_id': RUN_ID}))
except Exception as error:
    logger.warning('Could not display the run summary: %s', error)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8: Save the run log and set the final status
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Copy the run log to the logs volume, publish task values for information, and fail the task when the run failed (so the job alert fires). |
# MAGIC | **Input** | `final_status`, `error_details`, `run_log` |
# MAGIC | **Output** | Log file `<logs volume>/scraper/<regulator>/<day>/<run_id>.log`, task result SUCCESS or failed |

# COMMAND ----------

run_log.publish()

# Task values are information only. Databricks does not allow them inside a For each
# iteration (the normal way this notebook runs), so a failure here must not fail the run.
try:
    dbutils.jobs.taskValues.set(key='scrape_run_id', value=RUN_ID)
    dbutils.jobs.taskValues.set(key='scrape_status', value=final_status)
    dbutils.jobs.taskValues.set(key='regulator_id', value=REGULATOR_ID)
except Exception as error:
    logger.info('Task values not set (not supported inside a For each iteration): %s',
                str(error).splitlines()[0][:200])

if final_status == FAILED:
    raise RuntimeError(f'Run {RUN_ID} FAILED: {error_details}')
if final_status == PARTIAL_SUCCESS and FAIL_ON_PARTIAL:
    raise RuntimeError(f'Run {RUN_ID} PARTIAL_SUCCESS: {error_details or "document or item failures"}. '
                       f'See {AUDIT_TABLE} rows with status FAILED for this run_id.')

dbutils.notebook.exit(f'{RUN_ID} {final_status}')
