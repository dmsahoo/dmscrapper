/* =====================================================================
   rcm_promotion_log (internal catalog)
   One row per document version copied from the external zone.
   Written only by the internal copy job; C4 updates ingest_status.
   Replaces the copy columns that used to be in pdf_catalog.
   ===================================================================== */


CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.rcm_promotion_log (
    document_id          STRING    NOT NULL,
    version              INT       NOT NULL,
    regulator_id         STRING    NOT NULL,
    category_id          STRING,
    content_format       STRING    COMMENT 'PDF or HTML',
    file_name            STRING,
    source_url           STRING,
    raw_file_path        STRING    COMMENT 'Path in the external raw volume',
    internal_file_path   STRING    COMMENT 'Path in the internal documents volume',
    content_hash_sha256  STRING    COMMENT 'Hash verified after copy',
    malware_scan_verdict STRING    COMMENT 'Verdict read from pdf_catalog at copy time',
    copy_status          STRING    NOT NULL COMMENT 'COPIED or FAILED',
    copy_retry_count     INT       NOT NULL,
    copy_error_details   STRING,
    copied_at_sgt        TIMESTAMP,
    ingest_status        STRING    COMMENT 'PENDING after copy, then INGESTED or FAILED (set by C4)',
    ingest_error_details STRING    COMMENT 'Why ingestion failed, empty when INGESTED',
    run_id               STRING,
    updated_at_sgt       TIMESTAMP,
    CONSTRAINT pk_rcm_promotion_log PRIMARY KEY (document_id, version) NOT ENFORCED
)
USING DELTA
COMMENT 'Copy state of every document version promoted from external to internal';
