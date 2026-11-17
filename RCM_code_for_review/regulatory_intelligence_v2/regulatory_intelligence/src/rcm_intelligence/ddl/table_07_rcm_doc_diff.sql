/* =====================================================================
   table_07_rcm_doc_diff.sql   (aggregated layer, Gold)
   One row per difference between two versions of a document (ADDED, REMOVED or CHANGED passage).
   Found by code (Python difflib) in 05_agent_rcm_regulator, by comparing the full text in Silver.
   ===================================================================== */

CREATE TABLE IF NOT EXISTS ${table_doc_diff} (
    document_id       STRING    NOT NULL COMMENT 'Document identifier',
    version           INT       NOT NULL COMMENT 'The newer version',
    previous_version  INT                COMMENT 'The older version it was compared with',
    change_no         INT       NOT NULL COMMENT 'Order of the difference in the document',
    change_kind       STRING    NOT NULL COMMENT 'ADDED, REMOVED or CHANGED',
    old_text          STRING             COMMENT 'Wording in the older version (empty when ADDED)',
    new_text          STRING             COMMENT 'Wording in the newer version (empty when REMOVED)',
    created_at_sgt    TIMESTAMP          COMMENT 'Creation time (SGT)'
)
USING DELTA
COMMENT 'Exact differences between document versions (code, not AI)';
