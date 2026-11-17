/* =====================================================================
   table_01_schemas_and_volumes.sql
   One schema per layer (SMBC ADP Development Standards) and the two volumes.
   In SMBC environments the AI Admin creates the schemas; IF NOT EXISTS then does nothing.
   Placeholders are filled from config/common_config_<env>.yaml by notebooks/00_table_creation_rcm_intelligence.
   ===================================================================== */

CREATE SCHEMA IF NOT EXISTS ${schema_raw}        COMMENT 'Raw layer (Bronze): copied files, promotion log, raw records';
CREATE SCHEMA IF NOT EXISTS ${schema_curated}    COMMENT 'Curated layer (Silver): extracted text of every document version';
CREATE SCHEMA IF NOT EXISTS ${schema_aggregated} COMMENT 'Aggregated layer (Gold): chunks, CPDAP CPL records, version differences';
CREATE SCHEMA IF NOT EXISTS ${schema_ai}         COMMENT 'AI objects of the RCM use case: index, tools, agent state, guardrail and request logs';
CREATE SCHEMA IF NOT EXISTS ${schema_user_view}  COMMENT 'Views read by the Regulatory Scanner app';

CREATE VOLUME IF NOT EXISTS ${volume_documents} COMMENT 'Documents copied from the landing storage after a CLEAN malware scan verdict';
CREATE VOLUME IF NOT EXISTS ${volume_logs}      COMMENT 'Run log files of the internal jobs, one folder per job and per regulator';
