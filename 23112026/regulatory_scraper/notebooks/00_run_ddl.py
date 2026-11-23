# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Run DDL (External ADB workspace)
# MAGIC
# MAGIC Creates or updates the scraper schema, tables and monitoring views for one environment.
# MAGIC The SQL files in the `ddl` folder use `${catalog}.${schema}` instead of real names.
# MAGIC This notebook fills them from the config file and runs the files in name order (01, 03, 05).
# MAGIC
# MAGIC Parameters: `common_config_path` (default config/common_config.yaml, the same parameter the scraper job uses)
# MAGIC and `files` (ALL, or a comma separated list such as 05_monitoring_views).

# COMMAND ----------

# Read the catalog and schema from the config file, so nothing is hardcoded here
import glob
import os
import re

import yaml

_repo_root = os.path.dirname(os.getcwd()) if os.path.basename(os.getcwd()) == 'notebooks' else os.getcwd()
dbutils.widgets.text('common_config_path', 'config/common_config.yaml')
dbutils.widgets.text('files', 'ALL')
CONFIG_PATH = os.path.join(_repo_root, dbutils.widgets.get('common_config_path').strip())
FILES = dbutils.widgets.get('files').strip()
with open(CONFIG_PATH) as f:
    CFG = yaml.safe_load(f)                                  # plain settings only, never runs code
print('Settings from', CONFIG_PATH)

# The placeholders used in the SQL files, and their values
VALUES = {'${catalog}': CFG['catalog'], '${schema}': CFG['schema']}

# COMMAND ----------

# Fill the placeholders in one SQL file, remove comments, and split it into single statements
def statements_of(path):
    text = open(path, encoding='utf-8').read()
    for placeholder, value in VALUES.items():
        text = text.replace(placeholder, value)
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
for path in paths:
    statements = statements_of(path)
    print(f'{os.path.basename(path)}: {len(statements)} statements')
    for statement in statements:
        spark.sql(statement)
print('All DDL files ran successfully')
