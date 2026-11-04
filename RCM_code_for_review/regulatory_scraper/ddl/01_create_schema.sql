/* =====================================================================
   01_create_schema.sql
   The scraper schema. ${catalog} and ${schema} are filled from config/common_config.yaml
   (or the file in the common_config_path parameter) by notebooks/00_run_ddl.
   The landing files are in Azure Blob Storage (landing block of the config), not in a volume.
   ===================================================================== */

CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema}
COMMENT 'Regulatory scraper: pdf_catalog, scraping_audit_log and monitoring views';
