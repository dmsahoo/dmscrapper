/* =====================================================================
   table_03_rcm_scraping_audit_log.sql
   rcm_scraping_audit_log: append only, never updated. record_type gives the grain:
     RUN       one row at the start (RUNNING) and one at the end, per job run and stage
     CATEGORY  one row per coverage category per run (feeds the Coverage Log)
     ITEM      one row per regulatory item found in the scan period
     FILE      one row per document per run and stage

   stage: SCRAPE (scrape job) or SCAN (scan verdict job)

   status values
     RUN               RUNNING, SUCCESS, PARTIAL_SUCCESS, FAILED, ABANDONED
     CATEGORY          SUCCESS, FAILED
     ITEM              SUCCESS, PARTIAL_SUCCESS, FAILED
     FILE and SCRAPE   NEW, CHANGED, SKIPPED, FAILED
     FILE and SCAN     SCANNED, WAITING, FAILED (verdict in malware_scan_verdict)

   skip_reason values (only when status = SKIPPED)
     SAME_URL_SAME_HASH   same document, content unchanged since the last run
     SAME_HASH_OTHER_URL  identical content already catalogued under another link
     DUPLICATE_IN_RUN     same link or same content already handled earlier in this run
   ===================================================================== */

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.${table_audit_log} (
    audit_id                 STRING    NOT NULL COMMENT 'Unique identifier of the audit row',
    run_id                   STRING    NOT NULL COMMENT 'Job run, for example scrape_mas_20261117_091500_a1b2c3',
    record_type              STRING    NOT NULL COMMENT 'RUN, CATEGORY, ITEM or FILE',
    stage                    STRING    NOT NULL COMMENT 'SCRAPE or SCAN',
    regulator_id             STRING    NOT NULL COMMENT 'Regulator code, or ALL',
    event_time_sgt           TIMESTAMP NOT NULL COMMENT 'When the row was written (SGT)',
    status                   STRING    NOT NULL COMMENT 'See the header of this file',

    scan_start_date          DATE               COMMENT 'Start of the scan period',
    scan_end_date            DATE               COMMENT 'End of the scan period',
    category_id              STRING,
    category_name            STRING,

    item_title               STRING,
    item_date                DATE,
    item_date_label          STRING,
    item_reference           STRING,
    source_url               STRING,

    document_id              STRING,
    document_url             STRING,
    content_format           STRING,
    file_name                STRING,
    version                  INT,
    content_hash_sha256      STRING,
    file_size_bytes          BIGINT,
    raw_file_path            STRING,
    internal_file_path       STRING             COMMENT 'Not used by the scraper; kept for a common audit layout',
    skip_reason              STRING,
    duplicate_of_document_id STRING,
    malware_scan_verdict     STRING             COMMENT 'PENDING, CLEAN, MALICIOUS or SCAN_ERROR (stage SCAN)',
    malware_scan_result_raw  STRING             COMMENT 'Exact Defender tag text (stage SCAN)',

    start_time_sgt           TIMESTAMP,
    end_time_sgt             TIMESTAMP,
    items_found              INT,
    documents_found          INT,
    documents_new            INT,
    documents_changed        INT,
    documents_skipped        INT,
    documents_copied         INT,
    documents_failed         INT,

    retry_count              INT,
    error_category           STRING,
    error_details            STRING
)
USING DELTA
COMMENT 'Append only event trail for the scrape and scan verdict stages.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.appendOnly'                 = 'true'
);
