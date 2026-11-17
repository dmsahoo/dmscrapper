# Databricks notebook source
# MAGIC %md
# MAGIC # 04_index_rcm_doc_chunk
# MAGIC
# MAGIC | Item | Detail |
# MAGIC |---|---|
# MAGIC | **Purpose** | Creates the Databricks Vector Search index `idx_rcm_doc_chunk` on the Gold chunk table if it does not exist, then syncs it and checks that it holds every chunk. |
# MAGIC | **Job and task** | `job_sng_rcm_intelligence_setup`, task `02_index_create` (mode `CREATE`), and `job_sng_rcm_ingest_daily`, task `02_index_sync` (mode `SYNC`) |
# MAGIC | **Parameters** | `mode`: `SYNC` (default: create if missing, then sync and validate) or `CREATE` (create if missing, no sync). Optional `env` and `config_dir`. |
# MAGIC | **Inputs** | `rcm_doc_chunk` (Change Data Feed on), the `vector_search` block of the config |
# MAGIC | **Outputs** | Index `idx_rcm_doc_chunk` (Delta Sync, managed embeddings, triggered), one run log file |
# MAGIC | **Compute** | Single node classic job cluster, Databricks Runtime 17.3 LTS |
# MAGIC | **Dependencies** | Wheel `rcm_intelligence`, pinned `databricks-sdk`, CAN USE on the shared Vector Search endpoint, CREATE TABLE on the ai schema (to create the index) |
# MAGIC | **Fails when** | The index row count does not reach the chunk table row count within `sync_timeout_minutes`. The index then keeps serving its previous content. |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Import the package
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Load the index functions from the `rcm_intelligence` wheel (module `vector_index.py`). |
# MAGIC | **Input** | Wheel `rcm_intelligence` |
# MAGIC | **Output** | Imported functions and the logger |

# COMMAND ----------

from rcm_intelligence.common import new_run_id
from rcm_intelligence.logging_setup import RunLog, get_logger, set_log_context
from rcm_intelligence.settings import load_config, log_root, plain_name
from rcm_intelligence.vector_index import create_index_if_missing, sync_and_validate, wait_until_ready

logger = get_logger()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Read the parameters and the config
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Read the mode and the index settings, and start the run log. |
# MAGIC | **Input** | Widgets `mode`, `env`, `config_dir`; `common_config_<env>.yaml` |
# MAGIC | **Output** | `MODE`, `INDEX_NAME`, `RUN_ID`, `run_log` |

# COMMAND ----------

dbutils.widgets.dropdown('mode', 'SYNC', ['SYNC', 'CREATE'])
dbutils.widgets.text('env', '')                # empty: use the cluster environment variable RCM_ENV
dbutils.widgets.text('config_dir', '')         # empty: use the cluster environment variable RCM_CONFIG_DIR

MODE = dbutils.widgets.get('mode')
CFG = load_config(dbutils.widgets.get('env').strip() or None, dbutils.widgets.get('config_dir').strip() or None)
INDEX_NAME = plain_name(CFG, 'indexes', 'doc_chunk')

RUN_ID = new_run_id('index')
set_log_context(stage='INDEX', regulator_id='ALL', run_id=RUN_ID)
run_log = RunLog(log_root(CFG, 'index'), 'INDEX', 'ALL', RUN_ID).start()
logger.info('Environment %s | index %s | endpoint %s | mode %s',
            CFG['_env'], INDEX_NAME, CFG['vector_search']['endpoint'], MODE)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Create the index if it is missing
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Create the Delta Sync index on `rcm_doc_chunk` with primary key `chunk_id`, embedding source column `chunk_text`, the embedding model from the config, and triggered sync. Does nothing when the index exists. The index structure is fixed at creation: a change to it means a new index. |
# MAGIC | **Input** | `vector_search` block of the config |
# MAGIC | **Output** | The index (new or existing) |

# COMMAND ----------

try:
    created = create_index_if_missing(CFG)
    logger.info('Index %s %s', INDEX_NAME, 'created' if created else 'already exists')
except Exception as error:
    logger.error('Could not create the index %s: %s', INDEX_NAME, error)
    run_log.publish()
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Sync and validate
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | In mode `SYNC`: start a sync, then wait until the index row count equals the chunk table row count. In mode `CREATE`: only wait until the new index is ready. |
# MAGIC | **Input** | `rcm_doc_chunk`, the index |
# MAGIC | **Output** | Indexed rows and chunk rows (shown in the run log) |

# COMMAND ----------

try:
    if MODE == 'SYNC':
        indexed, chunk_rows = sync_and_validate(spark, CFG)
        result = f'indexed={indexed} chunks={chunk_rows}'
    else:
        status = wait_until_ready(INDEX_NAME, float(CFG['vector_search'].get('sync_timeout_minutes', 60)))
        result = f'ready indexed={status.indexed_row_count}'
except Exception as error:
    logger.error('Index step failed: %s', error)
    run_log.publish()
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Save the run log
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Copy the run log to the logs volume and finish. |
# MAGIC | **Input** | `run_log`, `result` |
# MAGIC | **Output** | Log file `vol_logs/index/ALL/<day>/<run_id>.log` |

# COMMAND ----------

run_log.publish()
dbutils.notebook.exit(f'{RUN_ID} {result}')
