/* =====================================================================
   01_create_schema_volume.sql
   The internal schema and the volume for copied documents.
   Placeholders ${...} are filled from config/<env>.yaml by notebooks/00_run_ddl.
   ===================================================================== */

CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema};

CREATE VOLUME IF NOT EXISTS ${catalog}.${schema}.${volume}
COMMENT 'Documents copied from the external landing storage after the malware scan';
