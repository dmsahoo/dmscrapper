/* =====================================================================
   table_03_rcm_doc_raw.sql   (raw layer, Bronze)
   The copied file as binary content, with its document details. One row per document version.
   Loaded by 03_ingest_rcm_parse_chunk with a MERGE on (document_id, version), so a rerun never
   creates a duplicate. ai_parse_document reads the content column directly.
   ===================================================================== */

CREATE TABLE IF NOT EXISTS ${table_doc_raw} (
    document_id          STRING    NOT NULL COMMENT 'Stable identifier of the document',
    version              INT       NOT NULL COMMENT '1 for the first file, increased for each change',
    regulator_id         STRING    NOT NULL COMMENT 'Regulator code, for example MAS',
    category_id          STRING             COMMENT 'Category from the regulator config, for example 1_NOTICES',
    item_title           STRING             COMMENT 'Title of the item on the regulator website',
    item_date            DATE               COMMENT 'Date of the item on the regulator website',
    item_date_label      STRING             COMMENT 'Meaning of item_date, for example Published Date',
    item_reference       STRING             COMMENT 'Regulator reference, for example a notice number',
    source_url           STRING             COMMENT 'Item page where the document was found',
    document_url         STRING             COMMENT 'Direct link to the document',
    content_format       STRING             COMMENT 'PDF or HTML',
    file_name            STRING             COMMENT 'Original file name',
    file_path            STRING             COMMENT 'Location of the file in the documents volume',
    content_hash_sha256  STRING             COMMENT 'SHA 256 of the file, checked against the promotion log on load',
    content              BINARY             COMMENT 'The document itself',
    scrape_run_id        STRING             COMMENT 'Latest scrape run that saw this version',
    copy_run_id          STRING             COMMENT 'Copy run that promoted the file',
    ingest_run_id        STRING             COMMENT 'Ingest run that loaded the row',
    ingested_at_sgt      TIMESTAMP          COMMENT 'Load time (SGT)',
    CONSTRAINT pk_rcm_doc_raw PRIMARY KEY (document_id, version) NOT ENFORCED
)
USING DELTA
COMMENT 'Bronze: raw content and details of every promoted document version';
