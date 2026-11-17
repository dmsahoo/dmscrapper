# Databricks notebook source
# MAGIC %md
# MAGIC # 01_view_creation_rcm_scraper
# MAGIC
# MAGIC | Item | Detail |
# MAGIC |---|---|
# MAGIC | **Purpose** | Creates or updates the monitoring views used by the operations dashboard and the SQL alerts. Safe to run again: every statement uses CREATE OR REPLACE VIEW. Run it after 00_table_creation_rcm_scraper. |
# MAGIC | **When to run** | Once per environment at set up, and again after a change to the SQL files. Run by the deployment pipeline or by hand. |
# MAGIC | **Parameters** | `files`: `ALL` (default) or a comma separated list of file names. Optional `env` and `config_dir`. |
# MAGIC | **Inputs** | `config/common_config_<env>.yaml` (names), SQL files `view_01_rcm_monitoring_views.sql` shipped inside the `rcm_scraper` wheel |
# MAGIC | **Outputs** | Views `v_rcm_run_history`, `v_rcm_scrape_coverage`, `v_rcm_scan_backlog`, `v_rcm_recent_failures` |
# MAGIC | **Compute** | Any cluster or serverless with the `rcm_scraper` wheel installed |
# MAGIC | **Permissions** | The running identity needs CREATE TABLE (and CREATE VOLUME) on the schema. In SMBC the AI Admin creates the schema itself. |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Import the scraper package
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Load the config reader and the SQL runner from the `rcm_scraper` wheel. No folder paths are used: the SQL files are read from inside the package. |
# MAGIC | **Input** | Wheel `rcm_scraper` |
# MAGIC | **Output** | Imported functions and the logger |

# COMMAND ----------

from rcm_scraper.ddl_runner import list_ddl_files, run_ddl_files
from rcm_scraper.logging_setup import get_logger, set_log_context
from rcm_scraper.settings import load_common_config, sql_placeholders

set_log_context(stage='DDL', regulator_id='ALL')
logger = get_logger()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Read the parameters and the config
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Load the config of the environment and work out the real names for the `${...}` placeholders in the SQL files. |
# MAGIC | **Input** | Widgets `files`, `env`, `config_dir`; `common_config_<env>.yaml` |
# MAGIC | **Output** | `VALUES` (placeholder to name), `FILE_NAMES` (files to run, in name order) |

# COMMAND ----------

dbutils.widgets.text('files', 'ALL')
dbutils.widgets.text('env', '')             # empty: use the cluster environment variable RCM_ENV
dbutils.widgets.text('config_dir', '')      # empty: use the cluster environment variable RCM_CONFIG_DIR

common = load_common_config(dbutils.widgets.get('env').strip() or None,
                            dbutils.widgets.get('config_dir').strip() or None)
VALUES = sql_placeholders(common)

AVAILABLE = list_ddl_files('view_')
WANTED = dbutils.widgets.get('files').strip()
if WANTED.upper() == 'ALL':
    FILE_NAMES = AVAILABLE
else:
    names = [name.strip() if name.strip().endswith('.sql') else name.strip() + '.sql' for name in WANTED.split(',')]
    unknown = sorted(set(names) - set(AVAILABLE))
    if unknown:
        raise ValueError(f'Unknown SQL files {unknown}. Available: {AVAILABLE}')
    FILE_NAMES = [name for name in AVAILABLE if name in names]

logger.info('Environment %s | config %s | files %s', common['_env'], common['_path'], FILE_NAMES)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Run the SQL files
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Fill in the names and run every statement, file by file, in name order. Stops at the first error, so nothing is left half done. |
# MAGIC | **Input** | `FILE_NAMES`, `VALUES` |
# MAGIC | **Output** | Views `v_rcm_run_history`, `v_rcm_scrape_coverage`, `v_rcm_scan_backlog`, `v_rcm_recent_failures` |

# COMMAND ----------

run_ddl_files(spark, VALUES, FILE_NAMES, logger)
logger.info('All SQL files ran successfully')
dbutils.notebook.exit(','.join(FILE_NAMES))
