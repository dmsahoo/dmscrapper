/* =====================================================================
   05_app_view.sql
   v_app_updates   what the app's Latest changes tab shows: ONE row per document, its newest validated version.
   v_app_history   every validated version of every document, for the Previous versions list.
   CREATE OR REPLACE removes grants, so 10_app_grants.sql always runs after this file (00_run_ddl does that).
   ===================================================================== */

CREATE OR REPLACE VIEW ${catalog}.${schema}.v_app_updates AS
SELECT document_id, regulator_id, version, change_type, event_type, previous_version, what_changed,
       name_of_office, assessment_date, law_or_regulation,
       info_collected_date, source_url, published_date, date_introduced, effective_date,
       intention_background, summary_key_items, areas_of_impact, proposed_action_timeline,
       compliance_risk_category, importance, relevance, relevance_reason, citations, created_at_sgt
FROM ${catalog}.${schema}.rcm_doc_analysis
WHERE analysis_status = 'VALIDATED'
-- keep only the newest validated version of each document
QUALIFY row_number() OVER (PARTITION BY document_id ORDER BY version DESC) = 1;

CREATE OR REPLACE VIEW ${catalog}.${schema}.v_app_history AS
SELECT document_id, regulator_id, version, change_type, what_changed, law_or_regulation,
       info_collected_date, published_date, effective_date, source_url, created_at_sgt
FROM ${catalog}.${schema}.rcm_doc_analysis
WHERE analysis_status = 'VALIDATED';
