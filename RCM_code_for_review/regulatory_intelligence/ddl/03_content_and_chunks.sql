/* =====================================================================
   C4 ingestion tables (internal catalog)
   rcm_promotion_log (copy job) acts as the Bronze record of each file.
   rcm_doc_content  Silver: extracted text, one row per document version (all versions kept)
   rcm_doc_chunk    Gold:   chunks of the CURRENT version only, source of the AI Search Index
   ===================================================================== */

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.rcm_doc_content (
    document_id          STRING    NOT NULL,
    version              INT       NOT NULL,
    regulator_id         STRING    NOT NULL,
    content_format       STRING    COMMENT 'PDF or HTML',
    file_name            STRING,
    source_url           STRING,
    internal_file_path   STRING,
    content_text         STRING    COMMENT 'Extracted text. Headings start with ##, tables kept as HTML',
    page_count           INT,
    parse_status         STRING    COMMENT 'PARSED or FAILED',
    parse_error          STRING,
    parsed_at_sgt        TIMESTAMP,
    CONSTRAINT pk_rcm_doc_content PRIMARY KEY (document_id, version) NOT ENFORCED
)
USING DELTA
COMMENT 'Silver: extracted text per document version';

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.rcm_doc_chunk (
    chunk_id             STRING    NOT NULL COMMENT 'document_id, version and chunk number',
    document_id          STRING    NOT NULL,
    version              INT       NOT NULL,
    regulator_id         STRING    NOT NULL,
    chunk_index          INT,
    section_title        STRING,
    page_number          INT       COMMENT 'First page of the chunk (PDF only)',
    chunk_text           STRING,
    char_count           INT,
    file_name            STRING,
    source_url           STRING,
    created_at_sgt       TIMESTAMP,
    CONSTRAINT pk_rcm_doc_chunk PRIMARY KEY (chunk_id) NOT ENFORCED
)
USING DELTA
COMMENT 'Gold: chunks of the current version of each document, source of the AI Search Index'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
