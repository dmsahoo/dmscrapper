'''
Small shared helpers used by every module of the scraper.

Contents
  1. Status values used in the audit table
  2. Time and run identifiers (Singapore time)
  3. Scan period rules (INCREMENTAL, CUSTOM, LAST_N_DAYS, PREVIOUS_MONTH, MONTH_TO_DATE)
  4. Hashing, URL and file name helpers
  5. Retry helper for Delta write conflicts

Spark is imported only inside the functions that need it, so the pure helpers can be
unit tested without a Spark session.
'''

import hashlib
import re
import time
import uuid
from datetime import date, datetime, timedelta
from urllib.parse import urljoin, urlsplit, urlunsplit
from zoneinfo import ZoneInfo

from rcm_scraper.logging_setup import get_logger

SGT = ZoneInfo('Asia/Singapore')

# =====================================================================
# 1. Status values
# =====================================================================

# Run status values written to rcm_scraping_audit_log
RUNNING = 'RUNNING'
SUCCESS = 'SUCCESS'
PARTIAL_SUCCESS = 'PARTIAL_SUCCESS'
FAILED = 'FAILED'
ABANDONED = 'ABANDONED'
TERMINAL_RUN_STATUSES = (SUCCESS, PARTIAL_SUCCESS, FAILED, ABANDONED)

# Decision for each downloaded document
NEW = 'NEW'
CHANGED = 'CHANGED'
SKIPPED = 'SKIPPED'

# Reasons for SKIPPED
SAME_URL_SAME_HASH = 'SAME_URL_SAME_HASH'
SAME_HASH_OTHER_URL = 'SAME_HASH_OTHER_URL'
DUPLICATE_IN_RUN = 'DUPLICATE_IN_RUN'


# =====================================================================
# 2. Time and run identifiers
# =====================================================================

def now_sgt():
    '''Current Singapore wall clock time, without a time zone. Every notebook sets
    spark.sql.session.timeZone to Asia/Singapore, so Spark stores it unchanged.'''
    return datetime.now(SGT).replace(tzinfo=None)


def new_run_id(prefix):
    '''Readable and unique run identifier: prefix, SGT time stamp, short random suffix.
    Example: scrape_mas_20261117_093015_a1b2c3'''
    return f'{prefix}_{now_sgt().strftime("%Y%m%d_%H%M%S")}_{uuid.uuid4().hex[:6]}'


# =====================================================================
# 3. Scan period
# =====================================================================

SCAN_MODES = ('INCREMENTAL', 'CUSTOM', 'LAST_N_DAYS', 'PREVIOUS_MONTH', 'MONTH_TO_DATE')


def resolve_scan_period(mode, today, last_n_days='7', start_text='', end_text='',
                        last_successful_end=None, overlap_days=3, max_window_days=31):
    '''Scan period as (start date, end date), both inclusive, in SGT calendar days.

    INCREMENTAL      daily schedule. Starts overlap_days before the end date of the last
                     successful scrape and ends today, so missed days are covered. The
                     overlap is free because already stored documents are marked SKIPPED.
    CUSTOM           initial bulk load, or a rerun of a given period.
    LAST_N_DAYS      fixed window ending today.
    PREVIOUS_MONTH   the full previous calendar month.
    MONTH_TO_DATE    the first of this month to today.
    '''
    if mode == 'INCREMENTAL':
        if last_successful_end is None:
            raise ValueError('INCREMENTAL needs a previous successful scrape. Run the bulk load first '
                             'with scan_mode CUSTOM and scan_end_date set to today or yesterday.')
        overlap = int(overlap_days)
        if overlap < 0:
            raise ValueError('overlap_days must be 0 or more')
        start = min(last_successful_end - timedelta(days=overlap), today)
        end = today
        window = (end - start).days + 1
        if window > int(max_window_days):
            raise ValueError(f'INCREMENTAL window would be {window} days (from {start}), above '
                             f'max_window_days {max_window_days}. The scraper has not succeeded for a '
                             f'while: run a CUSTOM backfill from {start} to {end} first.')
    elif mode == 'PREVIOUS_MONTH':
        end = today.replace(day=1) - timedelta(days=1)
        start = end.replace(day=1)
    elif mode == 'MONTH_TO_DATE':
        start, end = today.replace(day=1), today
    elif mode == 'LAST_N_DAYS':
        days = int(last_n_days)
        if days < 1:
            raise ValueError('last_n_days must be at least 1')
        start, end = today - timedelta(days=days - 1), today
    elif mode == 'CUSTOM':
        if not start_text or not end_text:
            raise ValueError('CUSTOM scan_mode needs scan_start_date and scan_end_date')
        try:
            start, end = date.fromisoformat(start_text), date.fromisoformat(end_text)
        except ValueError:
            raise ValueError(f'Dates must be ISO format year, month, day. Got {start_text} and {end_text}')
    else:
        raise ValueError(f'Unknown scan_mode {mode}, expected one of {SCAN_MODES}')
    if start > end:
        raise ValueError(f'Scan start {start} is after scan end {end}')
    if end > today:
        raise ValueError(f'Scan end {end} is in the future')
    return start, end


def last_successful_scan_end(spark, audit_table, regulator_id):
    '''Watermark for INCREMENTAL: the latest scan_end_date of a SCRAPE run that covered
    every category. SUCCESS counts, and so does PARTIAL_SUCCESS when only single documents
    failed. A run where a whole category failed does not move the watermark, so the next
    run scans that period again.'''
    row = spark.sql(f'''
        SELECT max(scan_end_date) AS last_end
        FROM {audit_table}
        WHERE stage = 'SCRAPE' AND record_type = 'RUN' AND regulator_id = :regulator_id
          AND (status = 'SUCCESS'
               OR (status = 'PARTIAL_SUCCESS' AND coalesce(error_category, '') <> 'CATEGORY_FAILURE'))
    ''', args={'regulator_id': regulator_id}).first()
    return row['last_end'] if row else None


# =====================================================================
# 4. Hashing, URL and file name helpers
# =====================================================================

def sha256_bytes(data):
    '''Fingerprint (SHA 256) of some bytes, used to detect changed files.'''
    return hashlib.sha256(data).hexdigest()


def normalize_url(url, base_url=None):
    '''Absolute URL with lower case scheme and host, no query string, no fragment.
    MAS links the same PDF with and without ?sc_lang=en, so the query is dropped.'''
    absolute = urljoin(base_url, url) if base_url else url
    parts = urlsplit((absolute or '').strip())
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path, '', ''))


def make_document_id(regulator_id, normalized_url):
    '''Stable document identifier: the fingerprint of the regulator and the normalised link.'''
    return sha256_bytes(f'{regulator_id}|{normalized_url}'.encode('utf8'))


_UNSAFE_CHARS = re.compile(r'[^A-Za-z0-9._\-]+')


def safe_file_name(name, default='document'):
    '''File name safe for storage: unusual characters replaced and length limited.'''
    cleaned = _UNSAFE_CHARS.sub('_', name or '').strip('._')
    return cleaned[:180] or default


def normalize_text(text):
    '''Text with all whitespace collapsed. HTML snapshots are hashed on this, so a layout
    only change on a page does not create a new version.'''
    return re.sub(r'\s+', ' ', text or '').strip()


# =====================================================================
# 5. Retry helper for Delta write conflicts
# =====================================================================

def run_with_delta_retry(action, what='delta write', attempts=4, base_delay_seconds=3):
    '''Retries only Delta concurrent modification conflicts, which can happen if two jobs
    write the same table at the same moment. Any other error is raised at once.'''
    logger = get_logger()
    for attempt in range(1, attempts + 1):
        try:
            return action()
        except Exception as error:
            text = f'{type(error).__name__} {error}'
            is_conflict = 'Concurrent' in text or 'DELTA_CONCURRENT' in text
            if not is_conflict or attempt == attempts:
                raise
            delay = base_delay_seconds * attempt
            logger.warning('Delta conflict on %s, attempt %s of %s, retrying in %ss',
                           what, attempt, attempts, delay)
            time.sleep(delay)
