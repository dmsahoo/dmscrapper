/* =====================================================================
   table_04_rcm_doc_content.sql   (curated layer, Silver)
   Extracted text of every document version (all versions are kept, for comparing versions).
   ===================================================================== */

CREATE TABLE IF NOT EXISTS ${table_doc_content} (
    document_id          STRING    NOT NULL COMMENT 'Document identifier, as in Bronze',
    version              INT       NOT NULL COMMENT 'Document version, as in Bronze',
    regulator_id         STRING    NOT NULL COMMENT 'Regulator code',
    category_id          STRING             COMMENT 'Category',
    content_format       STRING             COMMENT 'PDF or HTML',
    item_title           STRING             COMMENT 'Publication title',
    item_date            DATE               COMMENT 'Publication or revision date',
    source_url           STRING             COMMENT 'Item page',
    document_url         STRING             COMMENT 'Direct document link',
    file_name            STRING             COMMENT 'Original file name',
    internal_file_path   STRING             COMMENT 'Location of the file in the documents volume',
    content_hash_sha256  STRING             COMMENT 'Hash of the file that was parsed',
    content_text         STRING             COMMENT 'Full text in reading order. Headings start with ##',
    pages                ARRAY<STRUCT<page_number: INT, text: STRING>> COMMENT 'Text per page, for page level citations (PDF only)',
    page_count           INT                COMMENT 'Number of pages (PDF only)',
    parser               STRING             COMMENT 'ai_parse_document or html.parser',
    parser_version       STRING             COMMENT 'Version of the parser used',
    parse_status         STRING             COMMENT 'PARSED or FAILED',
    parse_error          STRING             COMMENT 'Reason for a failure',
    is_current           BOOLEAN            COMMENT 'True only for the latest version of the document',
    parse_run_id         STRING             COMMENT 'Ingest run that produced the row',
    parsed_at_sgt        TIMESTAMP          COMMENT 'Parse time (SGT)',
    CONSTRAINT pk_rcm_doc_content PRIMARY KEY (document_id, version) NOT ENFORCED
)
USING DELTA
COMMENT 'Silver: extracted text per document version';
