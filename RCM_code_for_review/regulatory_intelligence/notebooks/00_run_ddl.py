# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Run DDL (Internal ADB workspace)
# MAGIC
# MAGIC Creates or updates the schema, volume, tables, views, functions and app grants for one environment.
# MAGIC The SQL files in the `ddl` folder use placeholders such as `${catalog}.${schema}` instead of real names.
# MAGIC This notebook fills them from config/<env>.yaml and runs the files in name order (01, 02, 03, ...).
# MAGIC
# MAGIC Safe to run again: tables use CREATE IF NOT EXISTS, views and functions use CREATE OR REPLACE,
# MAGIC and the grants file runs last, so grants removed by CREATE OR REPLACE are given back.
# MAGIC
# MAGIC Parameters: `env` (default dev) and `files` (ALL, or a comma separated list such as 05_app_view,10_app_grants).

# COMMAND ----------

# Load the settings for the chosen environment from config/<env>.yaml
import glob
import os
import re
import sys
_repo_root = os.path.dirname(os.getcwd()) if os.path.basename(os.getcwd()) == 'notebooks' else os.getcwd()
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)
from common.config import load_config, sql_placeholders

dbutils.widgets.text('env', 'dev')
dbutils.widgets.text('files', 'ALL')
CFG = load_config(dbutils.widgets.get('env'))
FILES = dbutils.widgets.get('files').strip()
print('Settings from', CFG['_path'])

# COMMAND ----------

# Fill the placeholders in one SQL file, remove comments, and split it into single statements
def statements_of(path, values):
    text = open(path, encoding='utf-8').read()
    for placeholder, value in values.items():
        text = text.replace(placeholder, str(value))
    left = re.findall(r'\$\{[a-z_]+\}', text)
    if left:
        raise ValueError(f'{os.path.basename(path)}: no value in the config for {sorted(set(left))}')
    text = re.sub(r'/\*.*?\*/', '', text, flags=re.S)                  # remove /* block comments */
    text = '\n'.join(line for line in text.splitlines() if not line.strip().startswith('--'))   # remove -- comments
    return [s.strip() for s in re.split(r';\s*(?:\n|$)', text) if s.strip()]


# Pick the files to run: all of them, or only the ones named in the files widget
paths = sorted(glob.glob(os.path.join(_repo_root, 'ddl', '*.sql')))
if FILES.upper() != 'ALL':
    wanted = {f.strip().replace('.sql', '') for f in FILES.split(',')}
    paths = [p for p in paths if os.path.basename(p).replace('.sql', '') in wanted]

# COMMAND ----------

# Run every statement, file by file, and stop at the first error so nothing runs half done
values = sql_placeholders(CFG)
for path in paths:
    statements = statements_of(path, values)
    print(f'{os.path.basename(path)}: {len(statements)} statements')
    for statement in statements:
        spark.sql(statement)
print('All DDL files ran successfully')
