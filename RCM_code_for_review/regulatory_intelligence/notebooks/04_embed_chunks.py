# Databricks notebook source
# MAGIC %md
# MAGIC ## 04 Embed chunks
# MAGIC Adds an embedding for every new chunk in rcm_doc_chunk, and removes chunks that no longer exist.
# MAGIC The search (and later the Vector Search index) reads this table. Runs as a task in the pipeline job.
# MAGIC
# MAGIC Parameter: `env` (default dev). Table names and the embedding model come from config/<env>.yaml.
# MAGIC Compute: serverless. This replaces the earlier SQL version of this notebook.

# COMMAND ----------

# Load the settings for the chosen environment from config/<env>.yaml, so nothing is hardcoded here.
import os
import sys
_repo_root = os.path.dirname(os.getcwd()) if os.path.basename(os.getcwd()) == 'notebooks' else os.getcwd()
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)
from common.config import load_config, table, external_table

dbutils.widgets.text('env', 'dev')
CFG = load_config(dbutils.widgets.get('env'))
print('Settings from', CFG['_path'])

CHUNK_TABLE = table(CFG, 'rcm_doc_chunk')
EMBEDDING_TABLE = table(CFG, 'rcm_chunk_embedding')
PDF_CATALOG = external_table(CFG, 'pdf_catalog')
EMBEDDING_MODEL = CFG['embedding_model']

# COMMAND ----------

# Add new chunks only: chunks that do not have an embedding yet.
# The published date comes from pdf_catalog, so the agent can filter by date.
# The model name is passed as a parameter (:model), not pasted into the SQL.
spark.sql(f'''
    INSERT INTO {EMBEDDING_TABLE}
    SELECT c.chunk_id, c.document_id, c.version, c.regulator_id, c.section_title, c.chunk_text,
           p.item_date, c.source_url,
           ai_query(:model, c.chunk_text),
           from_utc_timestamp(current_timestamp(), 'Asia/Singapore')
    FROM {CHUNK_TABLE} c
    LEFT JOIN {PDF_CATALOG} p ON p.document_id = c.document_id
    LEFT ANTI JOIN {EMBEDDING_TABLE} e ON e.chunk_id = c.chunk_id
''', args={'model': EMBEDDING_MODEL})

# COMMAND ----------

# Remove chunks that no longer exist in Gold (old versions replaced by ingestion)
spark.sql(f'''
    DELETE FROM {EMBEDDING_TABLE}
    WHERE chunk_id NOT IN (SELECT chunk_id FROM {CHUNK_TABLE})
''')

# COMMAND ----------

# Check: number of chunks, embedding size (should be 1024 for databricks-gte-large-en), and how many have a date
display(spark.sql(f'''
    SELECT COUNT(*) AS chunks, MIN(size(embedding)) AS dims, COUNT(published_date) AS with_date
    FROM {EMBEDDING_TABLE}
'''))
