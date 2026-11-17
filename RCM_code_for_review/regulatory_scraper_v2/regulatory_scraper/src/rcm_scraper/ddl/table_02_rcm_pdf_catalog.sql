/* =====================================================================
   table_02_rcm_pdf_catalog.sql
   rcm_pdf_catalog: one row per unique document (regulator plus normalised link).
   PDF documents and HTML page snapshots both live here, told apart by content_format.
   Holds the CURRENT version and its malware scan verdict.

   Written only by the external workspace (scrape job and scan verdict job).
   The internal copy job reads it (read only) and copies only rows with verdict CLEAN.

   malware_scan_verdict values
     PENDING     saved, waiting for the Defender scan result
     CLEAN       Defender found no threats; can be copied to the internal zone
     MALICIOUS   Defender found malware; never copied, security alert
     SCAN_ERROR  any other Defender result; never copied, alert for investigation
   ===================================================================== */

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.${table_pdf_catalog} (
    document_id              STRING    NOT NULL COMMENT 'sha256 of regulator_id and normalised document_url',
    regulator_id             STRING    NOT NULL COMMENT 'Regulator code, for example MAS',
    category_id              STRING    NOT NULL COMMENT 'Coverage category from the regulator config',
    content_format           STRING    NOT NULL COMMENT 'PDF or HTML',

    item_title               STRING             COMMENT 'Title of the regulatory item (listing entry)',
    item_date                DATE               COMMENT 'Date used for the scan period filter',
    item_date_label          STRING             COMMENT 'Meaning of item_date as shown by the regulator, for example Published Date',
    item_reference           STRING             COMMENT 'Regulator reference where available, for example a notice number',
    source_url               STRING    NOT NULL COMMENT 'Item page where the document was found',
    document_url             STRING    NOT NULL COMMENT 'Normalised document link (query string removed)',
    link_text                STRING             COMMENT 'Text of the link on the item page',
    file_name                STRING    NOT NULL COMMENT 'Original file name',

    current_version          INT       NOT NULL COMMENT 'Starts at 1 and increases by 1 on each content change',
    content_hash_sha256      STRING    NOT NULL COMMENT 'SHA 256 fingerprint of the current version',
    file_size_bytes          BIGINT             COMMENT 'Size of the current version',
    raw_file_path            STRING    NOT NULL COMMENT 'Blob name of the current version in the landing container',
    scrape_status            STRING    NOT NULL COMMENT 'NEW or CHANGED, describes the current version',

    first_seen_run_id        STRING             COMMENT 'Scrape run that first found the document',
    first_seen_sgt           TIMESTAMP          COMMENT 'When the document was first found (SGT)',
    last_seen_run_id         STRING             COMMENT 'Latest scrape run that saw the document, including SKIPPED',
    last_seen_sgt            TIMESTAMP          COMMENT 'When the document was last seen (SGT)',
    last_changed_sgt         TIMESTAMP          COMMENT 'When the current version was saved (SGT)',

    malware_scan_verdict     STRING             COMMENT 'PENDING, CLEAN, MALICIOUS or SCAN_ERROR. The copy job copies only CLEAN',
    malware_scan_result_raw  STRING             COMMENT 'Exact Defender blob index tag text, kept as audit evidence',
    scan_timestamp_sgt       TIMESTAMP          COMMENT 'When Defender scanned the file (from its scan time tag), in SGT',

    CONSTRAINT pk_rcm_pdf_catalog PRIMARY KEY (document_id) NOT ENFORCED
)
USING DELTA
COMMENT 'Current state and malware scan verdict of every scraped regulatory document. Read only for the internal zone.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.enableChangeDataFeed'       = 'true'
);
