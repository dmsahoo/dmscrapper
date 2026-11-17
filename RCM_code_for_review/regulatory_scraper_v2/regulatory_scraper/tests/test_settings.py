'''Unit tests of the config loading from environment variables (no Spark needed).'''
import os
import shutil

import pytest

from rcm_scraper.settings import (ConfigError, full_table_name, load_common_config, log_root,
                                  regulator_config_path, resolve_regulator_ids, sql_placeholders)

REPO_CONFIG = os.path.join(os.path.dirname(__file__), '..', 'config')


def _config_dir(tmp_path):
    '''Copy of the repository config folder, with the placeholders replaced by test names.'''
    folder = tmp_path / 'config'
    shutil.copytree(REPO_CONFIG, folder)
    for env in ('dev', 'uat', 'prod'):
        path = folder / f'common_config_{env}.yaml'
        lines = []
        for line in path.read_text().splitlines():
            if line.startswith('catalog:'):
                line = f'catalog: ext_{env}'
            elif line.startswith('schema:'):
                line = 'schema: sng_stg_raw'
            lines.append(line)
        path.write_text('\n'.join(lines))
    return str(folder)


def test_placeholders_are_rejected():
    with pytest.raises(ConfigError, match='not a valid Unity Catalog name'):
        load_common_config('dev', REPO_CONFIG)


def test_reads_environment_variables(tmp_path, monkeypatch):
    monkeypatch.setenv('RCM_ENV', 'dev')
    monkeypatch.setenv('RCM_CONFIG_DIR', _config_dir(tmp_path))
    config = load_common_config()
    assert config['_env'] == 'dev'
    assert config['tables']['pdf_catalog'] == 'rcm_pdf_catalog'
    assert regulator_config_path(config, config['regulators']['MAS']).endswith('regulators/mas_config.yaml')


def test_arguments_win_over_environment(tmp_path, monkeypatch):
    monkeypatch.setenv('RCM_ENV', 'prod')
    config = load_common_config('uat', _config_dir(tmp_path))
    assert config['_env'] == 'uat'


def test_missing_environment_is_a_clear_error(monkeypatch):
    monkeypatch.delenv('RCM_ENV', raising=False)
    monkeypatch.delenv('RCM_CONFIG_DIR', raising=False)
    with pytest.raises(ConfigError, match='RCM_ENV'):
        load_common_config()


def test_environment_must_match_the_file(tmp_path):
    folder = _config_dir(tmp_path)
    text = open(os.path.join(folder, 'common_config_uat.yaml')).read().replace('environment: uat', 'environment: dev')
    open(os.path.join(folder, 'common_config_uat.yaml'), 'w').write(text)
    with pytest.raises(ConfigError, match='environment in the file'):
        load_common_config('uat', folder)


def test_names_are_quoted_for_hyphens(tmp_path):
    config = load_common_config('dev', _config_dir(tmp_path))
    config.update(catalog='smbcsg-devai', schema='sng_stg_raw')
    assert full_table_name(config, 'pdf_catalog') == '`smbcsg-devai`.`sng_stg_raw`.`rcm_pdf_catalog`'
    assert log_root(config) == '/Volumes/smbcsg-devai/sng_stg_raw/vol_logs/scraper'
    assert sql_placeholders(config)['${table_audit_log}'] == '`rcm_scraping_audit_log`'


def test_regulator_list(tmp_path):
    config = load_common_config('dev', _config_dir(tmp_path))
    assert resolve_regulator_ids(config, 'ALL') == ['MAS']
    with pytest.raises(ConfigError):
        resolve_regulator_ids(config, 'PDPC')          # disabled
    with pytest.raises(ConfigError):
        resolve_regulator_ids(config, 'MAS,ALL')
