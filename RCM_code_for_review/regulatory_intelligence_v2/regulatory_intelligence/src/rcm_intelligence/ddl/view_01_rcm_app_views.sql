/* =====================================================================
   view_01_rcm_app_views.sql   (user_view schema)
   v_rcm_app_updates  Latest changes tab: ONE row per document, its newest validated version.
   v_rcm_app_history  every validated version of every document, for the Previous versions list.
   CREATE OR REPLACE removes grants, so view_03_app_grants.sql always runs after this file.
   ===================================================================== */

CREATE OR REPLACE VIEW ${view_app_updates}
COMMENT 'Newest validated CPDAP CPL record of each document, for the Latest changes tab'
AS
SELECT document_id, regulator_id, category_id, version, change_type, event_type, previous_version, what_changed,
       name_of_office, assessment_date, law_or_regulation,
       info_collected_date, source_url, published_date, date_introduced, effective_date,
       intention_background, summary_key_items, areas_of_impact, proposed_action_timeline,
       compliance_risk_category, importance, relevance, relevance_reason, citations, created_at_sgt
FROM ${table_doc_analysis}
WHERE analysis_status = 'VALIDATED'
QUALIFY row_number() OVER (PARTITION BY document_id ORDER BY version DESC) = 1;

CREATE OR REPLACE VIEW ${view_app_history}
COMMENT 'Every validated CPDAP CPL record, for the Previous versions list'
AS
SELECT document_id, regulator_id, version, change_type, what_changed, law_or_regulation,
       info_collected_date, published_date, effective_date, source_url, created_at_sgt
FROM ${table_doc_analysis}
WHERE analysis_status = 'VALIDATED';
