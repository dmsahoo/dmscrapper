'''Config loading from environment variables and the SMBC layer names.'''
from importlib import resources

import pytest

from rcm_intelligence.ddl_runner import list_ddl_files, statements_of
from rcm_intelligence.settings import (ConfigError, full_name, load_config, plain_name, sql_placeholders,
                                       volume_path)

from conftest import REPO_CONFIG


def test_placeholders_in_the_repository_config_are_rejected():
    with pytest.raises(ConfigError, match='not a valid Unity Catalog name'):
        load_config('dev', REPO_CONFIG)


def test_reads_environment_variables(config_dir, monkeypatch):
    monkeypatch.setenv('RCM_ENV', 'uat')
    monkeypatch.setenv('RCM_CONFIG_DIR', config_dir)
    config = load_config()
    assert config['_env'] == 'uat'


def test_missing_environment_is_a_clear_error(monkeypatch):
    monkeypatch.delenv('RCM_ENV', raising=False)
    with pytest.raises(ConfigError, match='RCM_ENV'):
        load_config()


def test_layer_names(config_dir):
    config = load_config('dev', config_dir)
    config['catalog'] = 'smbcsg-devai'
    config['schemas']['aggregated'] = 'sng_agg_rcm'
    config['schemas']['ai'] = 'sng_ai_rcm'
    assert full_name(config, 'tables', 'doc_chunk') == '`smbcsg-devai`.`sng_agg_rcm`.`rcm_doc_chunk`'
    assert plain_name(config, 'indexes', 'doc_chunk') == 'smbcsg-devai.sng_ai_rcm.idx_rcm_doc_chunk'
    assert volume_path(config, 'logs') == '/Volumes/smbcsg-devai/sng_ai_rcm/vol_logs'


def test_clean_verdict_must_be_clean(config_dir):
    path = f'{config_dir}/common_config_dev.yaml'
    text = open(path).read().replace('clean_verdict: CLEAN', 'clean_verdict: No threats found')
    open(path, 'w').write(text)
    with pytest.raises(ConfigError, match='clean_verdict'):
        load_config('dev', config_dir)


def test_every_ddl_file_fills_all_placeholders(config_dir):
    values = sql_placeholders(load_config('dev', config_dir))
    files = list_ddl_files()
    assert len([f for f in files if f.startswith('table_')]) == 10
    assert len([f for f in files if f.startswith('view_')]) == 3
    for name in files:
        text = resources.files('rcm_intelligence.ddl').joinpath(name).read_text(encoding='utf8')
        statements = statements_of(text, values, name)
        assert statements and all('${' not in s for s in statements)
