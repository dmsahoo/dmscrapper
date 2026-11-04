"""
Shared settings loader for the internal workspace.

Every notebook, the Q and A Agent and the app read their names (catalog, schema, tables, model,
endpoints) from one file per environment: config/dev.yaml, config/uat.yaml or config/prod.yaml.
Nothing is hardcoded in the code, so the same code runs in every environment.

How the environment is chosen:
    notebooks and jobs   the env widget or job parameter (default dev)
    the Databricks App   the RCM_ENV environment variable in app.yaml (default dev)
"""
import os

import yaml                                   # PyYAML, already available on Databricks runtimes and serverless


# Find the repository root: the first folder, going up from here, that has both config and common in it
def repo_root():
    folder = os.path.dirname(os.path.abspath(__file__))
    while folder != os.path.dirname(folder):
        if os.path.isdir(os.path.join(folder, 'config')) and os.path.isdir(os.path.join(folder, 'common')):
            return folder
        folder = os.path.dirname(folder)
    raise FileNotFoundError('Repository root with config and common folders not found')


# Read config/<env>.yaml. env comes from the caller, then from RCM_ENV, and defaults to dev
def load_config(env=None):
    env = (env or os.getenv('RCM_ENV') or 'dev').strip().lower()
    path = os.path.join(repo_root(), 'config', f'{env}.yaml')
    with open(path) as f:
        cfg = yaml.safe_load(f)                   # safe_load: reads plain settings only, never runs code
    cfg['_path'] = path                                   # kept so notebooks can print which file they used
    return cfg


# Full name of a table, view or function in the internal schema, for example <catalog>.<schema>.rcm_doc_chunk
def table(cfg, name):
    return f"{cfg['catalog']}.{cfg['schema']}.{name}"


# Full name of a table in the external (scraper) schema, for example <external_catalog>.<external_schema>.pdf_catalog
def external_table(cfg, name):
    return f"{cfg['external_catalog']}.{cfg['external_schema']}.{name}"


# Path of the internal volume where copied documents are stored
def volume_path(cfg):
    return f"/Volumes/{cfg['catalog']}/{cfg['schema']}/{cfg['volume']}"


# Placeholders used in the SQL files in the ddl folder, and their values from the config
def sql_placeholders(cfg):
    return {
        '${catalog}': cfg['catalog'],
        '${schema}': cfg['schema'],
        '${volume}': cfg['volume'],
        '${external_catalog}': cfg['external_catalog'],
        '${external_schema}': cfg['external_schema'],
        '${embedding_model}': cfg['embedding_model'],
        '${app_service_principal}': cfg['app_service_principal'],
    }
