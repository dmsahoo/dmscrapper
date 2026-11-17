'''
Configuration: which environment we are in, where the config files are, and the regulator list.

Where the settings come from (no folder guessing, no os.getcwd)
  1. RCM_ENV         environment variable on the cluster: dev, uat or prod.
  2. RCM_CONFIG_DIR  environment variable on the cluster: the folder that holds the config files.
                     The deployment bundle sets both on the job cluster.
  3. config/common_config_<env>.yaml   names, storage, scan defaults, verdict mapping, regulators.
  4. config/regulators/<regulator>_config.yaml   the website rules of one regulator.

For an interactive run, a notebook can pass env and config_dir explicitly (optional widgets),
for example when the cluster has no environment variables.
'''

import importlib
import os
import re

import yaml                                   # PyYAML, included in the Databricks Runtime

from rcm_scraper.scan_verdict import validate_scan_config

ENV_VARIABLE = 'RCM_ENV'
CONFIG_DIR_VARIABLE = 'RCM_CONFIG_DIR'
ENVIRONMENTS = ('dev', 'uat', 'prod')

REQUIRED_KEYS = ('environment', 'catalog', 'schema', 'tables', 'landing', 'malware_scan',
                 'logging', 'defaults', 'regulators')
REQUIRED_TABLE_KEYS = ('pdf_catalog', 'audit_log')
REQUIRED_LANDING_KEYS = ('storage_account', 'container', 'prefix', 'service_credential')
REQUIRED_LOGGING_KEYS = ('volume', 'folder')
REQUIRED_DEFAULTS = ('scan_mode', 'last_n_days', 'overlap_days', 'max_window_days',
                     'fail_on_partial', 'stale_run_hours')
REQUIRED_REGULATOR_KEYS = ('config_path', 'scraper_module', 'scraper_class')
PACKAGE_PREFIX = 'rcm_scraper.'

_UC_NAME = re.compile(r'^[A-Za-z0-9_][A-Za-z0-9_\-]*$')     # catalog, schema, table or volume name
_REGULATOR_ID = re.compile(r'^[A-Z0-9_]{2,20}$')


class ConfigError(Exception):
    '''Raised for any problem in the environment variables, the config files or the regulator list.'''


# ---------------------------------------------------------------------
# Environment and file locations
# ---------------------------------------------------------------------

def config_location(env=None, config_dir=None):
    '''Environment name and config folder: from the arguments if given, otherwise from the
    cluster environment variables RCM_ENV and RCM_CONFIG_DIR.'''
    env = (env or os.environ.get(ENV_VARIABLE) or '').strip().lower()
    config_dir = (config_dir or os.environ.get(CONFIG_DIR_VARIABLE) or '').strip()
    if env not in ENVIRONMENTS:
        raise ConfigError(f'Environment is {env!r}. Set the cluster environment variable {ENV_VARIABLE} '
                          f'to one of {ENVIRONMENTS}, or pass env to the notebook.')
    if not config_dir:
        raise ConfigError(f'Config folder is not set. Set the cluster environment variable '
                          f'{CONFIG_DIR_VARIABLE}, or pass config_dir to the notebook.')
    return env, config_dir


def common_config_path(env, config_dir):
    return os.path.join(config_dir, f'common_config_{env}.yaml')


def read_yaml(path):
    '''Read a YAML settings file. safe_load reads plain settings only and never runs code.'''
    with open(path, 'r', encoding='utf8') as handle:
        return yaml.safe_load(handle)


# ---------------------------------------------------------------------
# Common config
# ---------------------------------------------------------------------

def validate_common_config(config, env=None):
    '''Check the common config and return it. Raises ConfigError listing every problem.'''
    problems = []
    for key in REQUIRED_KEYS:
        if key not in config or config[key] in (None, '', {}):
            problems.append(f'missing {key}')
    if env and config.get('environment') and config['environment'] != env:
        problems.append(f'environment in the file is {config["environment"]!r}, but {ENV_VARIABLE} is {env!r}')

    for key in ('catalog', 'schema'):
        value = config.get(key)
        if value and not _UC_NAME.match(str(value)):
            problems.append(f'{key} {value!r} is not a valid Unity Catalog name')
    tables = config.get('tables') or {}
    for key in REQUIRED_TABLE_KEYS:
        if not tables.get(key):
            problems.append(f'tables: missing {key}')
        elif not _UC_NAME.match(str(tables[key])):
            problems.append(f'tables.{key} {tables[key]!r} is not a valid table name')
    landing = config.get('landing') or {}
    for key in REQUIRED_LANDING_KEYS:
        if not landing.get(key):
            problems.append(f'landing: missing {key}')
    log_config = config.get('logging') or {}
    for key in REQUIRED_LOGGING_KEYS:
        if not log_config.get(key):
            problems.append(f'logging: missing {key}')
    defaults = config.get('defaults') or {}
    for key in REQUIRED_DEFAULTS:
        if key not in defaults:
            problems.append(f'defaults: missing {key}')
    problems += validate_scan_config(config.get('malware_scan') or {})

    for regulator_id, entry in (config.get('regulators') or {}).items():
        if not _REGULATOR_ID.match(regulator_id):
            problems.append(f'regulators: id {regulator_id!r} must be upper case letters, digits or underscore')
        for key in REQUIRED_REGULATOR_KEYS:
            if not (entry or {}).get(key):
                problems.append(f'regulators.{regulator_id}: missing {key}')
        module = (entry or {}).get('scraper_module') or ''
        if module and not module.startswith(PACKAGE_PREFIX):
            problems.append(f'regulators.{regulator_id}: scraper_module must be inside the rcm_scraper package')

    if problems:
        raise ConfigError('Invalid common config: ' + '; '.join(problems))
    return config


def load_common_config(env=None, config_dir=None):
    '''Load and check config/common_config_<env>.yaml. The returned dictionary also holds
    _env, _config_dir and _path, so later steps know where it came from.'''
    env, config_dir = config_location(env, config_dir)
    path = common_config_path(env, config_dir)
    try:
        config = read_yaml(path)
    except FileNotFoundError:
        raise ConfigError(f'Common config not found: {path}')
    except yaml.YAMLError as error:
        raise ConfigError(f'Common config {path} is not valid YAML: {error}')
    config = validate_common_config(config or {}, env)
    config.update(_env=env, _config_dir=config_dir, _path=path)
    return config


# ---------------------------------------------------------------------
# Names built from the config
# ---------------------------------------------------------------------

def quote_name(name):
    '''Unity Catalog name in backticks, so names with a hyphen (for example smbcsg-devai) work in SQL.'''
    return f'`{name}`'


def full_table_name(config, table_key):
    '''Fully qualified table name, for example `catalog`.`sng_stg_raw`.`rcm_pdf_catalog`.'''
    return '.'.join(quote_name(part) for part in (config['catalog'], config['schema'],
                                                   config['tables'][table_key]))


def log_root(config):
    '''Folder in the logs volume for this repository: /Volumes/<catalog>/<schema>/<volume>/<folder>.'''
    log_config = config['logging']
    return f"/Volumes/{config['catalog']}/{config['schema']}/{log_config['volume']}/{log_config['folder']}"


def sql_placeholders(config):
    '''Values for the ${...} placeholders in the ddl folder.'''
    return {
        '${catalog}': quote_name(config['catalog']),
        '${schema}': quote_name(config['schema']),
        '${table_pdf_catalog}': quote_name(config['tables']['pdf_catalog']),
        '${table_audit_log}': quote_name(config['tables']['audit_log']),
        '${log_volume}': quote_name(config['logging']['volume']),
    }


# ---------------------------------------------------------------------
# Regulators
# ---------------------------------------------------------------------

def get_regulator_entry(config, regulator_id):
    '''Registry entry for one regulator. Fails clearly if unknown or disabled.'''
    regulator_id = (regulator_id or '').strip().upper()
    if not regulator_id:
        raise ConfigError('regulator_id parameter is required, for example MAS')
    regulators = config['regulators']
    if regulator_id not in regulators:
        raise ConfigError(f'Unknown regulator_id {regulator_id}. Known: {sorted(regulators)}')
    entry = regulators[regulator_id]
    if not entry.get('enabled', False):
        raise ConfigError(f'Regulator {regulator_id} is disabled in the common config')
    return regulator_id, entry


def resolve_regulator_ids(config, regulators_text):
    '''Which regulators one job run should scrape, from the regulators job parameter.

    ALL (or empty)   every regulator with enabled true in the common config, in config order.
    MAS              one regulator.
    MAS,PDPC         several regulators, comma separated, in the order typed.

    A regulator named explicitly must exist and be enabled, otherwise the run stops, so a
    typo never turns into a silent run that scrapes nothing.'''
    text = (regulators_text or '').strip().upper()
    regulators = config['regulators']

    if text in ('', 'ALL'):
        ids = [rid for rid, entry in regulators.items() if (entry or {}).get('enabled', False)]
        if not ids:
            raise ConfigError('regulators is ALL but no regulator has enabled true in the common config')
        return ids

    ids = []
    for part in text.split(','):
        part = part.strip()
        if not part:
            continue
        if part == 'ALL':
            raise ConfigError('ALL cannot be combined with other regulators')
        regulator_id, _ = get_regulator_entry(config, part)
        if regulator_id not in ids:
            ids.append(regulator_id)
    if not ids:
        raise ConfigError(f'No regulator found in the regulators parameter {regulators_text!r}')
    return ids


def regulator_config_path(config, entry):
    '''Path of a regulator config file. A relative path is read from the config folder.'''
    path = entry['config_path']
    return path if os.path.isabs(path) else os.path.join(config['_config_dir'], path)


def load_scraper_class(entry, base_class):
    '''Import the scraper class named in the registry and check that it is a proper scraper.'''
    module_name, class_name = entry['scraper_module'], entry['scraper_class']
    try:
        module = importlib.import_module(module_name)
    except ImportError as error:
        raise ConfigError(f'Cannot import scraper module {module_name}: {error}')
    scraper_class = getattr(module, class_name, None)
    if scraper_class is None:
        raise ConfigError(f'Scraper class {class_name} not found in {module_name}')
    if not isinstance(scraper_class, type) or not issubclass(scraper_class, base_class):
        raise ConfigError(f'{module_name}.{class_name} must be a subclass of {base_class.__name__}')
    return scraper_class
