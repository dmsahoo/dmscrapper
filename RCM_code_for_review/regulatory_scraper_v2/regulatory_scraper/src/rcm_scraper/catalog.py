'''
The document catalog (table rcm_pdf_catalog): one row per document with its current version,
fingerprint and malware scan verdict.

Contents
  1. Duplicate classification: decides NEW, CHANGED or SKIPPED (pure function, unit tested)
  2. CatalogState: in memory view of the catalog for one regulator
  3. Table contracts: the columns the code expects, and a start up schema check
  4. CatalogRepository: reads and writes rcm_pdf_catalog
'''

import re
from dataclasses import dataclass, field

from rcm_scraper.common import (CHANGED, DUPLICATE_IN_RUN, NEW, SAME_HASH_OTHER_URL,
                                SAME_URL_SAME_HASH, SKIPPED, run_with_delta_retry)


# =====================================================================
# 1. Duplicate classification
# =====================================================================

@dataclass
class Classification:
    status: str
    skip_reason: str = None
    duplicate_of_document_id: str = None
    new_version: int = None


def classify_document(document_id, content_hash, state, hashes_seen_in_run):
    '''Decide NEW, CHANGED or SKIPPED for one downloaded document.

    Order of checks
      1. Same content already handled earlier in this run    SKIPPED DUPLICATE_IN_RUN
      2. Same link already catalogued with the same hash      SKIPPED SAME_URL_SAME_HASH
      3. Same content catalogued under a different link       SKIPPED SAME_HASH_OTHER_URL
      4. Same link already catalogued with a different hash   CHANGED, version plus 1
      5. Otherwise                                            NEW, version 1
    '''
    seen_document = hashes_seen_in_run.get(content_hash)
    if seen_document is not None:
        return Classification(SKIPPED, DUPLICATE_IN_RUN,
                              seen_document if seen_document != document_id else None)

    existing = state.by_id.get(document_id)
    if existing is not None and existing['content_hash_sha256'] == content_hash:
        return Classification(SKIPPED, SAME_URL_SAME_HASH)

    other_document = state.by_hash.get(content_hash)
    if other_document is not None and other_document != document_id:
        return Classification(SKIPPED, SAME_HASH_OTHER_URL, other_document)

    if existing is not None:
        return Classification(CHANGED, new_version=existing['current_version'] + 1)

    return Classification(NEW, new_version=1)


# =====================================================================
# 2. CatalogState
# =====================================================================

@dataclass
class CatalogState:
    by_id: dict = field(default_factory=dict)    # document_id to {content_hash_sha256, current_version}
    by_hash: dict = field(default_factory=dict)  # content_hash_sha256 to document_id

    def apply(self, document_id, content_hash, version):
        '''Update the in memory state after a file is saved, so later files in the same run see it.'''
        previous = self.by_id.get(document_id)
        if previous is not None and self.by_hash.get(previous['content_hash_sha256']) == document_id:
            del self.by_hash[previous['content_hash_sha256']]
        self.by_id[document_id] = {'content_hash_sha256': content_hash, 'current_version': version}
        self.by_hash.setdefault(content_hash, document_id)


# =====================================================================
# 3. Table contracts
# =====================================================================

CATALOG_COLUMNS = [
    ('document_id', 'string'), ('regulator_id', 'string'), ('category_id', 'string'),
    ('content_format', 'string'), ('item_title', 'string'), ('item_date', 'date'),
    ('item_date_label', 'string'), ('item_reference', 'string'), ('source_url', 'string'),
    ('document_url', 'string'), ('link_text', 'string'), ('file_name', 'string'),
    ('current_version', 'int'), ('content_hash_sha256', 'string'), ('file_size_bytes', 'bigint'),
    ('raw_file_path', 'string'), ('scrape_status', 'string'),
    ('first_seen_run_id', 'string'), ('first_seen_sgt', 'timestamp'),
    ('last_seen_run_id', 'string'), ('last_seen_sgt', 'timestamp'), ('last_changed_sgt', 'timestamp'),
    ('malware_scan_verdict', 'string'), ('malware_scan_result_raw', 'string'),
    ('scan_timestamp_sgt', 'timestamp'),
]

# Columns a CHANGED version overwrites. first_seen_* is kept. The verdict goes back to
# PENDING, so the new version is scanned and checked again before it can be copied.
CATALOG_UPDATE_ON_CHANGE = [
    'category_id', 'content_format', 'item_title', 'item_date', 'item_date_label', 'item_reference',
    'source_url', 'link_text', 'file_name', 'current_version', 'content_hash_sha256',
    'file_size_bytes', 'raw_file_path', 'scrape_status', 'last_seen_run_id', 'last_seen_sgt',
    'last_changed_sgt', 'malware_scan_verdict', 'malware_scan_result_raw', 'scan_timestamp_sgt',
]

_SQL_TYPES = {'string': 'STRING', 'int': 'INT', 'bigint': 'BIGINT', 'date': 'DATE', 'timestamp': 'TIMESTAMP'}


def spark_schema(columns):
    '''Spark schema from a list of (column name, type) pairs.'''
    from pyspark.sql.types import (StructType, StructField, StringType, IntegerType,
                                   LongType, DateType, TimestampType)
    type_map = {'string': StringType(), 'int': IntegerType(), 'bigint': LongType(),
                'date': DateType(), 'timestamp': TimestampType()}
    return StructType([StructField(name, type_map[kind], True) for name, kind in columns])


def schema_problems(actual_types, expected_columns):
    '''Compare {column: simple type} of a table with the expected columns.
    Returns a list of readable problems (empty when the table matches).'''
    problems = []
    for name, kind in expected_columns:
        actual = actual_types.get(name)
        if actual is None:
            problems.append(f'missing column {name} {_SQL_TYPES[kind]}')
        elif actual != kind:
            problems.append(f'column {name} is {actual}, expected {kind}')
    return problems


def validate_table_schema(spark, table_name, expected_columns):
    '''Stop early with a clear message when a table does not match the code.'''
    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(f'Table {table_name} does not exist. Run notebooks/00_table_creation_rcm_scraper.')
    actual = {f.name: f.dataType.simpleString() for f in spark.table(table_name).schema.fields}
    problems = schema_problems(actual, expected_columns)
    if problems:
        missing = [p for p in problems if p.startswith('missing column ')]
        fix = ''
        if missing and len(missing) == len(problems):
            columns = ', '.join(p.replace('missing column ', '') for p in missing)
            fix = f' Fix: ALTER TABLE {table_name} ADD COLUMNS ({columns});'
        raise RuntimeError(f'Table {table_name} does not match the code: {"; ".join(problems)}.{fix}')


# =====================================================================
# 4. CatalogRepository
# =====================================================================

class CatalogRepository:
    '''Reads and writes rcm_pdf_catalog.'''

    def __init__(self, spark, table_name):
        self.spark = spark
        self.table_name = table_name

    def load_state(self, regulator_id):
        '''What is already known for one regulator: identifiers, fingerprints and versions.'''
        from pyspark.sql import functions as F
        rows = (self.spark.table(self.table_name)
                .where(F.col('regulator_id') == F.lit(regulator_id))
                .select('document_id', 'content_hash_sha256', 'current_version')
                .collect())
        state = CatalogState()
        for row in rows:
            state.apply(row['document_id'], row['content_hash_sha256'], row['current_version'])
        return state

    def upsert_version(self, record):
        '''Insert a NEW document or overwrite the current version of a CHANGED one, in one MERGE.
        An update only happens when the stored version is older than the incoming one, which
        protects against an out of date in memory state.'''
        from delta.tables import DeltaTable

        full_record = {name: record.get(name) for name, _ in CATALOG_COLUMNS}
        source = self.spark.createDataFrame([full_record], schema=spark_schema(CATALOG_COLUMNS))

        def _merge():
            (DeltaTable.forName(self.spark, self.table_name).alias('t')
             .merge(source.alias('s'), 't.document_id = s.document_id')
             .whenMatchedUpdate(condition='t.current_version < s.current_version',
                                set={c: f's.{c}' for c in CATALOG_UPDATE_ON_CHANGE})
             .whenNotMatchedInsertAll()
             .execute())

        run_with_delta_retry(_merge, what=f'catalog merge {record.get("document_id")}')

    def mark_seen(self, document_ids, run_id, seen_at):
        '''At the end of a run, record that SKIPPED documents were seen again (one UPDATE per 500 ids).'''
        ids = sorted({value for value in document_ids if re.fullmatch(r'[0-9a-f]{64}', value or '')})
        for start in range(0, len(ids), 500):
            id_list = ', '.join(f"'{value}'" for value in ids[start:start + 500])
            sql = (f'UPDATE {self.table_name} '
                   f'SET last_seen_run_id = :run_id, last_seen_sgt = :seen_at '
                   f'WHERE document_id IN ({id_list})')
            run_with_delta_retry(lambda: self.spark.sql(sql, args={'run_id': run_id, 'seen_at': seen_at}),
                                 what='catalog mark_seen')
