# Databricks notebook source
# MAGIC %md
# MAGIC # 01_view_creation_rcm_intelligence
# MAGIC
# MAGIC | Item | Detail |
# MAGIC |---|---|
# MAGIC | **Purpose** | Creates or updates the app views, the agent tool function and the app grants. Safe to run again (CREATE OR REPLACE). Runs after the tables and the Vector Search index exist, because the grants include the index. |
# MAGIC | **Job and task** | `job_sng_rcm_intelligence_setup`, task `03_view_creation`. Run once per environment, and again after a change to the SQL files. |
# MAGIC | **Parameters** | `files`: `ALL` (default) or a comma separated list of file names. Optional `env` and `config_dir` (empty means: use the cluster environment variables). |
# MAGIC | **Inputs** | `config/common_config_<env>.yaml` (names), the `view_*.sql` files shipped inside the `rcm_intelligence` wheel |
# MAGIC | **Outputs** | Views `v_rcm_app_updates` and `v_rcm_app_history`, function `fn_get_changes`, grants for the app service principal |
# MAGIC | **Permissions** | The running identity needs CREATE privileges on the schemas, and MANAGE (or ownership) to grant. In SMBC the AI Admin creates the schemas. |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Import the package
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Load the config reader and the SQL runner from the `rcm_intelligence` wheel installed on the job cluster. No folder paths are used: the SQL files are read from inside the package. |
# MAGIC | **Input** | Wheel `rcm_intelligence` |
# MAGIC | **Output** | Imported functions and the logger |

# COMMAND ----------

from rcm_intelligence.ddl_runner import list_ddl_files, run_ddl_files
from rcm_intelligence.logging_setup import get_logger, set_log_context
from rcm_intelligence.settings import load_config, sql_placeholders

set_log_context(stage='DDL', regulator_id='ALL')
logger = get_logger()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Read the parameters and the config
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Load the config of the environment and work out the real, fully quoted names for the `${...}` placeholders in the SQL files. |
# MAGIC | **Input** | Widgets `files`, `env`, `config_dir`; `common_config_<env>.yaml` |
# MAGIC | **Output** | `VALUES` (placeholder to name), `FILE_NAMES` (files to run, in name order) |

# COMMAND ----------

dbutils.widgets.text('files', 'ALL')
dbutils.widgets.text('env', '')             # empty: use the cluster environment variable RCM_ENV
dbutils.widgets.text('config_dir', '')      # empty: use the cluster environment variable RCM_CONFIG_DIR

CFG = load_config(dbutils.widgets.get('env').strip() or None, dbutils.widgets.get('config_dir').strip() or None)
VALUES = sql_placeholders(CFG)

AVAILABLE = list_ddl_files('view_')
WANTED = dbutils.widgets.get('files').strip()
if WANTED.upper() == 'ALL':
    FILE_NAMES = AVAILABLE
else:
    names = [n.strip() if n.strip().endswith('.sql') else n.strip() + '.sql' for n in WANTED.split(',')]
    unknown = sorted(set(names) - set(AVAILABLE))
    if unknown:
        raise ValueError(f'Unknown SQL files {unknown}. Available: {AVAILABLE}')
    FILE_NAMES = [n for n in AVAILABLE if n in names]

logger.info('Environment %s | config %s | files %s', CFG['_env'], CFG['_path'], FILE_NAMES)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Run the SQL files
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Fill in the names and run every statement, file by file, in name order. Stops at the first error, so nothing is left half done. |
# MAGIC | **Input** | `FILE_NAMES`, `VALUES` |
# MAGIC | **Output** | Views `v_rcm_app_updates` and `v_rcm_app_history`, function `fn_get_changes`, grants for the app service principal |

# COMMAND ----------

run_ddl_files(spark, VALUES, FILE_NAMES, logger)
logger.info('All SQL files ran successfully')
dbutils.notebook.exit(','.join(FILE_NAMES))
