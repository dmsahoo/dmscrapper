/* =====================================================================
   table_06_rcm_doc_analysis.sql   (aggregated layer, Gold)
   One CPDAP CPL record per document version, written by the Regulator Agent, keyed on
   document_id and version, so a rerun updates the row instead of duplicating it.
   Fields 1, 2 and 4 are filled by code. Fields 3 and 5 to 12 by the Regulator Agent.
   Field 4 (Date information collected and source) is stored as two columns.
   ===================================================================== */

CREATE TABLE IF NOT EXISTS ${table_doc_analysis} (
    document_id                 STRING    NOT NULL COMMENT 'Document identifier',
    version                     INT       NOT NULL COMMENT 'Document version',
    regulator_id                STRING    NOT NULL COMMENT 'Regulator code',
    category_id                 STRING             COMMENT 'Category code',
    -- The 12 CPDAP CPL fields, in Law Tracker order
    name_of_office              STRING    COMMENT '1. Normally SNG Office (code)',
    assessment_date             DATE      COMMENT '2. Date the draft assessment was generated (code)',
    law_or_regulation           STRING    COMMENT '3. Official title of the regulatory change',
    info_collected_date         DATE      COMMENT '4a. Date the information was retrieved (code)',
    source_url                  STRING    COMMENT '4b. Authoritative source URL (code)',
    published_date              DATE      COMMENT 'Publication date from the regulator website (code)',
    date_introduced             STRING    COMMENT '5. Publication or issue date, or N.A.',
    effective_date              STRING    COMMENT '6. Effective date, Not Stated, or N.A.',
    intention_background        STRING    COMMENT '7. Source grounded purpose and context',
    summary_key_items           STRING    COMMENT '8. Factual summary, not legal advice',
    areas_of_impact             STRING    COMMENT '9. Potentially affected areas, subject to SME confirmation',
    proposed_action_timeline    STRING    COMMENT '10. Draft follow up suggestion and timeline',
    compliance_risk_category    STRING    COMMENT '11. AML, CFT, Sanctions, Data Protection, Conduct, Operational Risk, Cybersecurity, Other',
    importance                  STRING    COMMENT '12. High, Medium, Low or Pending Review',
    -- Version information
    change_type                 STRING    COMMENT 'NEW for version 1, UPDATED for a newer copy at the same link (code)',
    event_type                  STRING    COMMENT 'Kind of change, for example Notices, Amendment, Consultation Papers (code)',
    previous_version            INT       COMMENT 'Version this record was compared with. Empty for NEW',
    what_changed                STRING    COMMENT 'What is different from the previous version. Only for UPDATED',
    -- Supporting fields
    relevance                   STRING    COMMENT 'RELEVANT, NOT_RELEVANT or EXCLUDED (Tax, CPF)',
    relevance_reason            STRING    COMMENT 'Short reason for the relevance decision',
    citations                   ARRAY<STRING> COMMENT 'chunk_id values that support the record',
    analysis_status             STRING    COMMENT 'VALIDATED or FLAGGED',
    check_problems              STRING    COMMENT 'Why a record was FLAGGED, empty when VALIDATED',
    -- Traceability: what produced the record
    agent_name                  STRING    COMMENT 'Agent that produced the record',
    agent_version               STRING    COMMENT 'Version of the rcm_intelligence package',
    prompt_version              STRING    COMMENT 'Versions of the base prompt and the regulator module',
    model_endpoint              STRING    COMMENT 'Model service used',
    release_tag                 STRING    COMMENT 'Release (Git tag) that produced the record',
    trace_id                    STRING    COMMENT 'MLflow trace of the analysis',
    created_at_sgt              TIMESTAMP COMMENT 'Creation time (SGT)',
    CONSTRAINT pk_rcm_doc_analysis PRIMARY KEY (document_id, version) NOT ENFORCED
)
USING DELTA
COMMENT 'One CPDAP CPL record per document version, produced by the Regulator Agent';
