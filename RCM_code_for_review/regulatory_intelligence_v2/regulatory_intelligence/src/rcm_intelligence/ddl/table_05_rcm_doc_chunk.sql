/* =====================================================================
   table_05_rcm_doc_chunk.sql   (aggregated layer, Gold)
   Chunks of the CURRENT version of each document only. When a new version arrives, the chunks
   of the previous version are replaced. Change Data Feed is on, because the Vector Search index
   idx_rcm_doc_chunk syncs from it.
   ===================================================================== */

CREATE TABLE IF NOT EXISTS ${table_doc_chunk} (
    chunk_id             STRING    NOT NULL COMMENT 'Primary key: document_id, version and chunk number',
    document_id          STRING    NOT NULL COMMENT 'Parent document',
    version              INT       NOT NULL COMMENT 'Version of the parent document',
    regulator_id         STRING    NOT NULL COMMENT 'Regulator code, used as a search filter',
    category_id          STRING             COMMENT 'Category code',
    item_title           STRING             COMMENT 'Publication title, used in citations',
    item_date            DATE               COMMENT 'Publication date, used as a search filter',
    item_reference       STRING             COMMENT 'Reference such as a notice number, helps exact match search',
    source_url           STRING             COMMENT 'Item page',
    document_url         STRING             COMMENT 'Document link used in citations',
    content_format       STRING             COMMENT 'PDF or HTML',
    file_name            STRING             COMMENT 'Original file name',
    chunk_index          INT                COMMENT 'Position of the chunk within the document',
    section_title        STRING             COMMENT 'Nearest section heading above the chunk',
    page_from            INT                COMMENT 'First page covered by the chunk (PDF only)',
    page_to              INT                COMMENT 'Last page covered by the chunk (PDF only)',
    chunk_text           STRING             COMMENT 'Text that is embedded, searched and returned to the agents',
    char_count           INT                COMMENT 'Chunk size in characters, checked against the chunking limit',
    chunk_run_id         STRING             COMMENT 'Ingest run that produced the chunk',
    created_at_sgt       TIMESTAMP          COMMENT 'Creation time (SGT)',
    CONSTRAINT pk_rcm_doc_chunk PRIMARY KEY (chunk_id) NOT ENFORCED
)
USING DELTA
COMMENT 'Gold: chunks of the current version of each document, source of the Vector Search index'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
