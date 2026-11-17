# Databricks notebook source
# MAGIC %md
# MAGIC # 02_ingest_rcm_resolve_regulators
# MAGIC
# MAGIC | Item | Detail |
# MAGIC |---|---|
# MAGIC | **Purpose** | Turns the job parameter `regulators` into the list of regulators to scrape in this run, and passes it to the next task. |
# MAGIC | **Job and task** | `job_sng_rcm_scrape_daily`, task `01_resolve_regulators` (first task) |
# MAGIC | **Parameters** | `regulators`: `ALL` (default, every enabled regulator), one regulator such as `MAS`, or a list such as `MAS,PDPC`. Optional `env` and `config_dir` (empty means: use the cluster environment variables). |
# MAGIC | **Inputs** | `config/common_config_<env>.yaml` (regulator list) |
# MAGIC | **Outputs** | Task value `regulator_ids` (list), used by the For each task `02_scrape_regulators` |
# MAGIC | **Compute** | Job cluster of the scrape job |
# MAGIC | **Dependencies** | Wheel `rcm_scraper`, cluster environment variables `RCM_ENV` and `RCM_CONFIG_DIR` |
# MAGIC | **Writes to tables** | None |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Import the scraper package
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Load the shared code. The `rcm_scraper` wheel is installed on the job cluster by the deployment bundle, so the notebook needs no folder paths. |
# MAGIC | **Input** | Wheel `rcm_scraper` |
# MAGIC | **Output** | Settings functions and the logger |

# COMMAND ----------

from rcm_scraper.logging_setup import get_logger, set_log_context
from rcm_scraper.settings import ConfigError, load_common_config, resolve_regulator_ids

set_log_context(stage='SCRAPE', regulator_id='ALL')
logger = get_logger()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Read the parameters
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Read which regulators to run. `env` and `config_dir` are optional and normally empty, because the cluster environment variables give them. |
# MAGIC | **Input** | Job parameters or notebook widgets |
# MAGIC | **Output** | `PARAM_REGULATORS`, `PARAM_ENV`, `PARAM_CONFIG_DIR` |

# COMMAND ----------

dbutils.widgets.text('regulators', 'ALL')
dbutils.widgets.text('env', '')             # empty: use the cluster environment variable RCM_ENV
dbutils.widgets.text('config_dir', '')      # empty: use the cluster environment variable RCM_CONFIG_DIR

PARAM_REGULATORS = dbutils.widgets.get('regulators').strip()
PARAM_ENV = dbutils.widgets.get('env').strip() or None
PARAM_CONFIG_DIR = dbutils.widgets.get('config_dir').strip() or None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Load the config and resolve the regulator list
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Load `common_config_<env>.yaml` and turn the parameter into a checked list. A regulator that is unknown or disabled stops the run, so a typo never becomes a silent run that scrapes nothing. |
# MAGIC | **Input** | `PARAM_REGULATORS`, the common config |
# MAGIC | **Output** | `REGULATOR_IDS`, for example `['MAS']` |

# COMMAND ----------

try:
    common = load_common_config(PARAM_ENV, PARAM_CONFIG_DIR)
    REGULATOR_IDS = resolve_regulator_ids(common, PARAM_REGULATORS)
except (ConfigError, ValueError) as error:
    logger.error('Cannot resolve regulators from %r: %s', PARAM_REGULATORS, error)
    raise

logger.info('Environment %s | config %s | regulators %r resolved to %s',
            common['_env'], common['_path'], PARAM_REGULATORS or 'ALL', REGULATOR_IDS)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Pass the list to the next task
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Publish the list as a task value. The For each task reads it and runs `03_ingest_rcm_scrape` once per regulator. |
# MAGIC | **Input** | `REGULATOR_IDS` |
# MAGIC | **Output** | Task value `regulator_ids`, and the notebook exit value (comma separated list) |

# COMMAND ----------

try:
    dbutils.jobs.taskValues.set(key='regulator_ids', value=REGULATOR_IDS)
except Exception as error:
    logger.error('Could not set the task value regulator_ids: %s', error)
    raise

dbutils.notebook.exit(','.join(REGULATOR_IDS))
