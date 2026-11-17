/* =====================================================================
   table_01_schema_and_volume.sql
   Schema of the scraper tables and the volume for the run logs.
   Placeholders are filled from config/common_config_<env>.yaml by
   notebooks/00_table_creation_rcm_scraper.

   In SMBC environments the AI Admin creates the schema. IF NOT EXISTS makes this
   statement do nothing when the schema is already there.
   The documents themselves are in Azure Blob Storage (landing block of the config),
   not in a volume, because Defender blob index tags need a storage account without HNS.
   ===================================================================== */

CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema}
COMMENT 'Raw layer (Bronze) of the Regulatory Change Management use case: scraper catalog and audit trail';

CREATE VOLUME IF NOT EXISTS ${catalog}.${schema}.${log_volume}
COMMENT 'Run logs of the RCM scraper, one folder per regulator and per day';
