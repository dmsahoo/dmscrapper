'''Unit tests of the per regulator run log (no Spark needed).'''
import json
import os
from datetime import date

from rcm_scraper.logging_setup import RunLog, get_logger, run_log_path


def test_log_path_has_regulator_and_day():
    path = run_log_path('/Volumes/c/s/vol_logs/scraper', 'MAS', 'scrape_mas_1', date(2026, 11, 17))
    assert path == '/Volumes/c/s/vol_logs/scraper/MAS/2026-11-17/scrape_mas_1.log'


def test_run_log_writes_json_lines_with_labels(tmp_path):
    run_log = RunLog(str(tmp_path), 'SCRAPE', 'MAS', 'scrape_mas_test').start()
    get_logger().info('hello %s', 'world')
    target = run_log.publish()
    assert target and target.startswith(str(tmp_path / 'MAS'))
    lines = [json.loads(line) for line in open(target, encoding='utf8')]
    hello = [entry for entry in lines if entry['message'] == 'hello world'][0]
    assert hello['regulator_id'] == 'MAS' and hello['run_id'] == 'scrape_mas_test' and hello['stage'] == 'SCRAPE'


def test_publish_failure_never_fails_the_run(tmp_path):
    blocker = tmp_path / 'not_a_folder'
    blocker.write_text('x')
    run_log = RunLog(str(blocker), 'SCAN', 'ALL', 'scan_test').start()
    assert run_log.publish() is None
    assert os.path.exists(run_log.local_path)
