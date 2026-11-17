/* =====================================================================
   view_03_app_grants.sql
   Unity Catalog permissions of the Databricks App service principal (least privilege).
   ${app_service_principal} is its Application ID, from the config.
   The model service, the SQL warehouse and the secrets are given as app resources instead.
   Runs last, because CREATE OR REPLACE VIEW and FUNCTION remove grants. The Vector Search
   index must exist first (notebooks/04_index_rcm_doc_chunk runs before this file in the set up job).
   ===================================================================== */

-- Reach the schemas the app uses
GRANT USE CATALOG ON CATALOG ${catalog} TO `${app_service_principal}`;
GRANT USE SCHEMA ON SCHEMA ${schema_user_view}  TO `${app_service_principal}`;
GRANT USE SCHEMA ON SCHEMA ${schema_aggregated} TO `${app_service_principal}`;
GRANT USE SCHEMA ON SCHEMA ${schema_ai}         TO `${app_service_principal}`;

-- Latest changes tab: the views and the exact wording changes
GRANT SELECT ON TABLE ${view_app_updates} TO `${app_service_principal}`;
GRANT SELECT ON TABLE ${view_app_history} TO `${app_service_principal}`;
GRANT SELECT ON TABLE ${table_doc_diff}   TO `${app_service_principal}`;

-- AI Assistant tab: the period tool, the search index and the two log tables (insert only)
GRANT EXECUTE ON FUNCTION ${function_get_changes} TO `${app_service_principal}`;
GRANT SELECT ON TABLE ${index_doc_chunk}          TO `${app_service_principal}`;
GRANT MODIFY ON TABLE ${table_guardrail_log}      TO `${app_service_principal}`;
GRANT MODIFY ON TABLE ${table_user_request_log}   TO `${app_service_principal}`;
