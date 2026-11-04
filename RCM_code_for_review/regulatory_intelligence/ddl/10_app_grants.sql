/* =====================================================================
   10_app_grants.sql
   Unity Catalog permissions for the Databricks App's service principal.
   ${app_service_principal} is its Application ID, from config/<env>.yaml.
   The model, the SQL warehouse and the secrets are given as App resources instead.
   Always runs after 05_app_view.sql, because CREATE OR REPLACE VIEW removes grants.
   ===================================================================== */

-- Reach the schema
GRANT USE CATALOG ON CATALOG ${catalog} TO `${app_service_principal}`;
GRANT USE SCHEMA ON SCHEMA ${catalog}.${schema} TO `${app_service_principal}`;

-- Latest changes tab: the views and the exact wording changes
GRANT SELECT ON TABLE ${catalog}.${schema}.v_app_updates TO `${app_service_principal}`;
GRANT SELECT ON TABLE ${catalog}.${schema}.v_app_history TO `${app_service_principal}`;
GRANT SELECT ON TABLE ${catalog}.${schema}.rcm_doc_diff  TO `${app_service_principal}`;

-- AI Assistant tab: the two tools and the table search_chunks reads
GRANT EXECUTE ON FUNCTION ${catalog}.${schema}.get_changes   TO `${app_service_principal}`;
GRANT EXECUTE ON FUNCTION ${catalog}.${schema}.search_chunks TO `${app_service_principal}`;
GRANT SELECT ON TABLE ${catalog}.${schema}.rcm_chunk_embedding TO `${app_service_principal}`;
