'''Unit tests of the duplicate decision, the scan period, the audit rows and the SQL runner.'''
from datetime import date

import pytest

from rcm_scraper.audit import build_audit_row
from rcm_scraper.catalog import CatalogState, classify_document
from rcm_scraper.common import CHANGED, NEW, SKIPPED, resolve_scan_period
from rcm_scraper.ddl_runner import list_ddl_files, statements_of
from rcm_scraper.settings import sql_placeholders


def test_classification():
    state = CatalogState()
    assert classify_document('a', 'h1', state, {}).status == NEW
    state.apply('a', 'h1', 1)
    assert classify_document('a', 'h1', state, {}).status == SKIPPED
    decision = classify_document('a', 'h2', state, {})
    assert decision.status == CHANGED and decision.new_version == 2
    assert classify_document('b', 'h1', state, {}).skip_reason == 'SAME_HASH_OTHER_URL'


def test_incremental_scan_period():
    today = date(2026, 11, 17)
    assert resolve_scan_period('INCREMENTAL', today, last_successful_end=date(2026, 11, 16),
                               overlap_days=3) == (date(2026, 11, 13), today)
    with pytest.raises(ValueError):
        resolve_scan_period('CUSTOM', today, start_text='2026-11-10', end_text='2026-11-01')


def test_audit_row_rejects_unknown_columns():
    row = build_audit_row({'run_id': 'r'}, 'FILE', 'SCANNED', {'malware_scan_result_raw': 'Malicious'})
    assert row['malware_scan_result_raw'] == 'Malicious'
    with pytest.raises(ValueError):
        build_audit_row({}, 'FILE', 'SCANNED', {'not_a_column': 1})


def test_every_ddl_file_fills_all_placeholders():
    config = {'catalog': 'smbcsg-devai', 'schema': 'sng_stg_raw',
              'tables': {'pdf_catalog': 'rcm_pdf_catalog', 'audit_log': 'rcm_scraping_audit_log'},
              'logging': {'volume': 'vol_logs', 'folder': 'scraper'}}
    values = sql_placeholders(config)
    from importlib import resources
    files = list_ddl_files()
    assert files == ['table_01_schema_and_volume.sql', 'table_02_rcm_pdf_catalog.sql',
                     'table_03_rcm_scraping_audit_log.sql', 'view_01_rcm_monitoring_views.sql']
    counts = {}
    for name in files:
        text = resources.files('rcm_scraper.ddl').joinpath(name).read_text(encoding='utf8')
        statements = statements_of(text, values, name)
        counts[name] = len(statements)
        assert all('${' not in s for s in statements)
    assert counts == {'table_01_schema_and_volume.sql': 2, 'table_02_rcm_pdf_catalog.sql': 1,
                      'table_03_rcm_scraping_audit_log.sql': 1, 'view_01_rcm_monitoring_views.sql': 4}
