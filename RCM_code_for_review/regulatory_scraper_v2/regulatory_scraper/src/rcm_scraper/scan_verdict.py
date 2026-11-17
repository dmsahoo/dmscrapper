'''
Malware scan verdict: turns the text Microsoft Defender for Storage writes on each blob
(blob index tag) into one of four fixed values that the rest of the solution uses.

  Defender tag value            malware_scan_verdict   Next step
  no tag yet                    PENDING                checked again on the next run
  No threats found              CLEAN                  can be copied to the internal zone
  Malicious                     MALICIOUS              never copied, security alert
  any other value               SCAN_ERROR             never copied, alert for investigation

The mapping lives in the config (malware_scan block), so a change in the Defender text is a
config change, not a code change. Matching ignores spaces and upper or lower case.
Anything that does not match counts as SCAN_ERROR, never CLEAN (fail closed).

The original Defender text is also kept in malware_scan_result_raw, as audit evidence.
'''

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

PENDING = 'PENDING'
CLEAN = 'CLEAN'
MALICIOUS = 'MALICIOUS'
SCAN_ERROR = 'SCAN_ERROR'
VERDICTS = (PENDING, CLEAN, MALICIOUS, SCAN_ERROR)

# Verdicts that are checked again on every run. CLEAN and MALICIOUS are final.
RECHECK_VERDICTS = (PENDING, SCAN_ERROR)

_SGT = ZoneInfo('Asia/Singapore')


def _key(text):
    return ' '.join(str(text or '').split()).lower()


def validate_scan_config(scan_config):
    '''Check the malware_scan block of the common config. Returns a list of problems.'''
    problems = []
    for key in ('tag_name', 'verdict_map'):
        if not scan_config.get(key):
            problems.append(f'malware_scan: missing {key}')
    for raw, verdict in (scan_config.get('verdict_map') or {}).items():
        if verdict not in VERDICTS:
            problems.append(f'malware_scan.verdict_map: {raw!r} maps to {verdict!r}, expected one of {VERDICTS}')
    for key, default in (('missing_tag_verdict', PENDING), ('unknown_value_verdict', SCAN_ERROR)):
        if scan_config.get(key, default) not in VERDICTS:
            problems.append(f'malware_scan.{key} must be one of {VERDICTS}')
    if scan_config.get('unknown_value_verdict', SCAN_ERROR) == CLEAN:
        problems.append('malware_scan.unknown_value_verdict must never be CLEAN')
    return problems


def map_verdict(raw_value, scan_config):
    '''Fixed verdict for one Defender tag value. raw_value is None when there is no tag yet.'''
    if raw_value is None or not str(raw_value).strip():
        return scan_config.get('missing_tag_verdict', PENDING)
    lookup = {_key(raw): verdict for raw, verdict in (scan_config.get('verdict_map') or {}).items()}
    return lookup.get(_key(raw_value), scan_config.get('unknown_value_verdict', SCAN_ERROR))


def parse_scan_time(text):
    '''Defender scan time (UTC) from its tag, as Singapore time without a time zone.
    Returns None when the tag is missing or not in a known format.'''
    if not text:
        return None
    value = str(text).strip().replace('Z', '+00:00')
    for parse in (datetime.fromisoformat,
                  lambda v: datetime.strptime(v, '%Y-%m-%d %H:%M:%S%z')):
        try:
            moment = parse(value)
            break
        except ValueError:
            moment = None
    if moment is None:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(_SGT).replace(tzinfo=None)
