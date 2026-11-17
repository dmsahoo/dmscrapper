/* =====================================================================
   table_02_rcm_promotion_log.sql   (raw layer)
   One row per document version copied from the external zone. Written by 02_ingest_rcm_copy;
   03_ingest_rcm_parse_chunk updates ingest_status.
   ===================================================================== */

CREATE TABLE IF NOT EXISTS ${table_promotion_log} (
    document_id          STRING    NOT NULL COMMENT 'Document identifier from rcm_pdf_catalog',
    version              INT       NOT NULL COMMENT 'Document version',
    regulator_id         STRING    NOT NULL COMMENT 'Regulator code, for example MAS',
    category_id          STRING             COMMENT 'Coverage category',
    content_format       STRING             COMMENT 'PDF or HTML',
    file_name            STRING             COMMENT 'Original file name',
    source_url           STRING             COMMENT 'Item page where the document was found',
    raw_file_path        STRING             COMMENT 'Blob name in the landing container',
    internal_file_path   STRING             COMMENT 'Path in the documents volume',
    content_hash_sha256  STRING             COMMENT 'Fingerprint verified after the copy',
    malware_scan_verdict STRING             COMMENT 'Verdict at copy time, always CLEAN for copied rows',
    copy_status          STRING    NOT NULL COMMENT 'COPIED or FAILED',
    copy_retry_count     INT       NOT NULL COMMENT 'Failed attempts so far',
    copy_error_details   STRING             COMMENT 'Why the copy failed',
    copied_at_sgt        TIMESTAMP          COMMENT 'When the copy finished (SGT)',
    ingest_status        STRING             COMMENT 'PENDING after the copy, then INGESTED or FAILED',
    ingest_error_details STRING             COMMENT 'Why ingestion failed, empty when INGESTED',
    run_id               STRING             COMMENT 'Copy run that wrote the row',
    updated_at_sgt       TIMESTAMP          COMMENT 'Last change of the row (SGT)',
    CONSTRAINT pk_rcm_promotion_log PRIMARY KEY (document_id, version) NOT ENFORCED
)
USING DELTA
COMMENT 'Copy and ingest state of every document version promoted from the external zone';
