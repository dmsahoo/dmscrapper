'''Unit tests of the malware scan verdict mapping (no Spark, no Azure needed).'''
from datetime import datetime

from rcm_scraper.scan_verdict import (CLEAN, MALICIOUS, PENDING, SCAN_ERROR, map_verdict,
                                      parse_scan_time, validate_scan_config)

CONFIG = {
    'tag_name': 'Malware Scanning scan result',
    'verdict_map': {'No threats found': CLEAN, 'Malicious': MALICIOUS},
    'missing_tag_verdict': PENDING,
    'unknown_value_verdict': SCAN_ERROR,
}


def test_no_tag_is_pending():
    assert map_verdict(None, CONFIG) == PENDING
    assert map_verdict('   ', CONFIG) == PENDING


def test_known_values():
    assert map_verdict('No threats found', CONFIG) == CLEAN
    assert map_verdict('Malicious', CONFIG) == MALICIOUS


def test_matching_ignores_case_and_spaces():
    assert map_verdict('  no   THREATS found ', CONFIG) == CLEAN


def test_unknown_value_is_scan_error_never_clean():
    assert map_verdict('Error', CONFIG) == SCAN_ERROR
    assert map_verdict('Not scanned', CONFIG) == SCAN_ERROR


def test_config_checks():
    assert validate_scan_config(CONFIG) == []
    bad = dict(CONFIG, unknown_value_verdict=CLEAN)
    assert any('never be CLEAN' in p for p in validate_scan_config(bad))
    bad = dict(CONFIG, verdict_map={'No threats found': 'OK'})
    assert validate_scan_config(bad)


def test_scan_time_is_converted_to_sgt():
    assert parse_scan_time('2026-11-17 01:00:00Z') == datetime(2026, 11, 17, 9, 0, 0)
    assert parse_scan_time('2026-11-17T01:00:00Z') == datetime(2026, 11, 17, 9, 0, 0)
    assert parse_scan_time('not a time') is None
    assert parse_scan_time(None) is None
