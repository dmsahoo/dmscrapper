'''
Configuration of the internal workspace: environment, config file and every object name.

Where the settings come from (no folder guessing, no os.getcwd)
  1. RCM_ENV         environment variable on the job cluster or the app: dev, uat or prod.
  2. RCM_CONFIG_DIR  environment variable: the folder that holds the config files.
  3. config/common_config_<env>.yaml: catalog, schemas, object names, model, limits.

Object names follow the SMBC ADP Development Standards: the layer is in the schema name
(raw, curated, aggregated, ai, user_view), the table name is <source>_<table_name>.
Each object in the config says its layer and its name, for example
    tables:
      doc_chunk: {layer: aggregated, name: rcm_doc_chunk}
and full_name(config, 'tables', 'doc_chunk') returns `catalog`.`sng_agg_rcm`.`rcm_doc_chunk`.
'''

import os
import re

import yaml                                   # PyYAML, included in the Databricks Runtime

ENV_VARIABLE = 'RCM_ENV'
CONFIG_DIR_VARIABLE = 'RCM_CONFIG_DIR'
ENVIRONMENTS = ('dev', 'uat', 'prod')
LAYERS = ('raw', 'curated', 'aggregated', 'ai', 'user_view')
OBJECT_GROUPS = ('tables', 'views', 'functions', 'volumes', 'indexes')

REQUIRED_KEYS = ('environment', 'catalog', 'schemas', 'external', 'landing', 'tables', 'views',
                 'functions', 'volumes', 'indexes', 'vector_search', 'model_service', 'sql_warehouse_id',
                 'mlflow_experiment_root', 'app_service_principal', 'name_of_office', 'copy',
                 'chunking', 'agent', 'guardrails', 'logging')
REQUIRED_OBJECTS = {
    'tables': ('promotion_log', 'doc_raw', 'doc_content', 'doc_chunk', 'doc_analysis', 'doc_diff',
               'agent_task', 'guardrail_log', 'user_request_log'),
    'views': ('app_updates', 'app_history'),
    'functions': ('get_changes',),
    'volumes': ('documents', 'logs'),
    'indexes': ('doc_chunk',),
}

_UC_NAME = re.compile(r'^[A-Za-z0-9_][A-Za-z0-9_\-]*$')


class ConfigError(Exception):
    '''Raised for any problem in the environment variables or the config file.'''


def config_location(env=None, config_dir=None):
    '''Environment and config folder: from the arguments if given, otherwise from the
    environment variables RCM_ENV and RCM_CONFIG_DIR.'''
    env = (env or os.environ.get(ENV_VARIABLE) or '').strip().lower()
    config_dir = (config_dir or os.environ.get(CONFIG_DIR_VARIABLE) or '').strip()
    if env not in ENVIRONMENTS:
        raise ConfigError(f'Environment is {env!r}. Set the environment variable {ENV_VARIABLE} '
                          f'to one of {ENVIRONMENTS}, or pass env to the notebook.')
    if not config_dir:
        raise ConfigError(f'Config folder is not set. Set the environment variable {CONFIG_DIR_VARIABLE}, '
                          f'or pass config_dir to the notebook.')
    return env, config_dir


def validate_config(config, env=None):
    '''Check the config and return it. Raises ConfigError listing every problem.'''
    problems = [f'missing {key}' for key in REQUIRED_KEYS if config.get(key) in (None, '', {})]
    if env and config.get('environment') and config['environment'] != env:
        problems.append(f'environment in the file is {config["environment"]!r}, but {ENV_VARIABLE} is {env!r}')

    names = [('catalog', config.get('catalog'))]
    names += [(f'schemas.{k}', v) for k, v in (config.get('schemas') or {}).items()]
    names += [('external.catalog', (config.get('external') or {}).get('catalog')),
              ('external.schema', (config.get('external') or {}).get('schema')),
              ('external.pdf_catalog', (config.get('external') or {}).get('pdf_catalog'))]
    for label, value in names:
        if value and not _UC_NAME.match(str(value)):
            problems.append(f'{label} {value!r} is not a valid Unity Catalog name')
    for layer in LAYERS:
        if not (config.get('schemas') or {}).get(layer):
            problems.append(f'schemas: missing {layer}')

    for group, keys in REQUIRED_OBJECTS.items():
        for key in keys:
            entry = (config.get(group) or {}).get(key) or {}
            if entry.get('layer') not in LAYERS:
                problems.append(f'{group}.{key}: layer must be one of {LAYERS}')
            if not entry.get('name') or not _UC_NAME.match(str(entry.get('name'))):
                problems.append(f'{group}.{key}: missing or invalid name')

    if (config.get('landing') or {}).get('clean_verdict') != 'CLEAN':
        problems.append('landing.clean_verdict must be CLEAN (the fixed verdict written by the scraper)')
    if problems:
        raise ConfigError('Invalid config: ' + '; '.join(problems))
    return config


def load_config(env=None, config_dir=None):
    '''Load and check config/common_config_<env>.yaml. The result also holds _env, _config_dir and _path.'''
    env, config_dir = config_location(env, config_dir)
    path = os.path.join(config_dir, f'common_config_{env}.yaml')
    try:
        with open(path, 'r', encoding='utf8') as handle:
            config = yaml.safe_load(handle)           # plain settings only, never runs code
    except FileNotFoundError:
        raise ConfigError(f'Config not found: {path}')
    except yaml.YAMLError as error:
        raise ConfigError(f'Config {path} is not valid YAML: {error}')
    config = validate_config(config or {}, env)
    config.update(_env=env, _config_dir=config_dir, _path=path)
    return config


# ---------------------------------------------------------------------
# Names built from the config
# ---------------------------------------------------------------------

def quote(name):
    '''Unity Catalog name in backticks, so names with a hyphen work in SQL.'''
    return f'`{name}`'


def schema_name(config, layer):
    '''Quoted catalog and schema of a layer, for example `catalog`.`sng_agg_rcm`.'''
    return f"{quote(config['catalog'])}.{quote(config['schemas'][layer])}"


def full_name(config, group, key):
    '''Quoted three part name of one object, for example full_name(config, 'tables', 'doc_chunk').'''
    entry = config[group][key]
    return f"{schema_name(config, entry['layer'])}.{quote(entry['name'])}"


def plain_name(config, group, key):
    '''Three part name without backticks, for APIs that take a plain name (Vector Search).'''
    entry = config[group][key]
    return f"{config['catalog']}.{config['schemas'][entry['layer']]}.{entry['name']}"


def volume_path(config, key):
    '''File path of a volume, for example /Volumes/<catalog>/<schema>/vol_documents.'''
    entry = config['volumes'][key]
    return f"/Volumes/{config['catalog']}/{config['schemas'][entry['layer']]}/{entry['name']}"


def external_pdf_catalog(config):
    '''Quoted name of the scraper catalog table in the external zone (read only).'''
    ext = config['external']
    return f"{quote(ext['catalog'])}.{quote(ext['schema'])}.{quote(ext['pdf_catalog'])}"


def log_root(config, folder):
    '''Folder in the logs volume for one kind of job, for example .../vol_logs/copy.'''
    return f"{volume_path(config, 'logs')}/{folder}"


def sql_placeholders(config):
    '''Values for the ${...} placeholders in the ddl folder: every schema and object, fully quoted.'''
    values = {'${catalog}': quote(config['catalog']),
              '${app_service_principal}': config['app_service_principal']}
    for layer in LAYERS:
        values[f'${{schema_{layer}}}'] = schema_name(config, layer)
    singular = {'tables': 'table', 'views': 'view', 'functions': 'function', 'volumes': 'volume', 'indexes': 'index'}
    for group in OBJECT_GROUPS:
        for key in config[group]:
            values[f'${{{singular[group]}_{key}}}'] = full_name(config, group, key)
    return values
