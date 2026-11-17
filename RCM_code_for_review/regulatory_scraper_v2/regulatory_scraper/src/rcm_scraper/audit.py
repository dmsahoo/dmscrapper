'''
The audit trail (table rcm_scraping_audit_log): append only rows for every run, category,
item and file, for both stages (SCRAPE and SCAN).

Contents
  1. AUDIT_COLUMNS: the columns the code expects
  2. build_audit_row: shapes one row (pure function, unit tested)
  3. AuditWriter: collects rows and appends them in one write per flush
'''

import uuid

from rcm_scraper.catalog import spark_schema
from rcm_scraper.common import now_sgt, run_with_delta_retry

AUDIT_COLUMNS = [
    ('audit_id', 'string'), ('run_id', 'string'), ('record_type', 'string'), ('stage', 'string'),
    ('regulator_id', 'string'), ('event_time_sgt', 'timestamp'), ('status', 'string'),
    ('scan_start_date', 'date'), ('scan_end_date', 'date'),
    ('category_id', 'string'), ('category_name', 'string'),
    ('item_title', 'string'), ('item_date', 'date'), ('item_date_label', 'string'),
    ('item_reference', 'string'), ('source_url', 'string'),
    ('document_id', 'string'), ('document_url', 'string'), ('content_format', 'string'),
    ('file_name', 'string'), ('version', 'int'), ('content_hash_sha256', 'string'),
    ('file_size_bytes', 'bigint'), ('raw_file_path', 'string'), ('internal_file_path', 'string'),
    ('skip_reason', 'string'), ('duplicate_of_document_id', 'string'),
    ('malware_scan_verdict', 'string'), ('malware_scan_result_raw', 'string'),
    ('start_time_sgt', 'timestamp'), ('end_time_sgt', 'timestamp'),
    ('items_found', 'int'), ('documents_found', 'int'), ('documents_new', 'int'),
    ('documents_changed', 'int'), ('documents_skipped', 'int'), ('documents_copied', 'int'),
    ('documents_failed', 'int'), ('retry_count', 'int'), ('error_category', 'string'),
    ('error_details', 'string'),
]
_AUDIT_NAMES = [name for name, _ in AUDIT_COLUMNS]
_AUDIT_NAME_SET = set(_AUDIT_NAMES)
MAX_ERROR_CHARS = 4000


def build_audit_row(base, record_type, status, fields):
    '''Shape one audit row. Raises on an unknown column, so a typo in a caller fails loudly
    instead of silently dropping data.'''
    unknown = set(fields) - _AUDIT_NAME_SET
    if unknown:
        raise ValueError(f'Unknown audit columns: {sorted(unknown)}')
    row = dict.fromkeys(_AUDIT_NAMES)
    row.update(base)
    row.update(fields)
    row['audit_id'] = str(uuid.uuid4())
    row['record_type'] = record_type
    row['status'] = status
    row['event_time_sgt'] = row.get('event_time_sgt') or now_sgt()
    if row.get('error_details') is not None:
        row['error_details'] = str(row['error_details'])[:MAX_ERROR_CHARS]
    return row


class AuditWriter:
    '''Collects audit rows in memory and appends them in one write per flush.
    Callers flush after each category and in finally blocks, so a crash loses at most
    the rows of the unit of work in progress.'''

    def __init__(self, spark, table_name, run_id, regulator_id, stage,
                 scan_start_date=None, scan_end_date=None):
        self.spark = spark
        self.table_name = table_name
        self.base = {'run_id': run_id, 'regulator_id': regulator_id, 'stage': stage,
                     'scan_start_date': scan_start_date, 'scan_end_date': scan_end_date}
        self.rows = []

    def add(self, record_type, status, **fields):
        '''Add one audit row to the buffer (written by the next flush).'''
        self.rows.append(build_audit_row(self.base, record_type, status, fields))

    def flush(self):
        '''Append the buffered rows to rcm_scraping_audit_log in one write.'''
        if not self.rows:
            return
        pending, self.rows = self.rows, []
        frame = self.spark.createDataFrame([tuple(r[name] for name in _AUDIT_NAMES) for r in pending],
                                           schema=spark_schema(AUDIT_COLUMNS))
        try:
            run_with_delta_retry(
                lambda: frame.write.format('delta').mode('append').saveAsTable(self.table_name),
                what='audit append')
        except Exception:
            self.rows = pending + self.rows
            raise
