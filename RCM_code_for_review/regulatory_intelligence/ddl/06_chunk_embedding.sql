/* =====================================================================
   06_chunk_embedding.sql
   One row per chunk: its text, the details used for filters, and its embedding.
   Change Data Feed is on, so a Vector Search index can sync from this table later.
   ===================================================================== */

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.rcm_chunk_embedding (
    chunk_id         STRING    NOT NULL,
    document_id      STRING    NOT NULL,
    version          INT       NOT NULL,
    regulator_id     STRING    NOT NULL,
    section_title    STRING,
    chunk_text       STRING    COMMENT 'Used for the keyword part of hybrid search',
    published_date   DATE      COMMENT 'From pdf_catalog.item_date, used for date filters',
    source_url       STRING,
    embedding        ARRAY<FLOAT> COMMENT 'Numbers from the embedding model set in the config',
    embedded_at_sgt  TIMESTAMP
)
USING DELTA
COMMENT 'Source table for the search used by the Q and A Agent'
TBLPROPERTIES (delta.enableChangeDataFeed = true);
