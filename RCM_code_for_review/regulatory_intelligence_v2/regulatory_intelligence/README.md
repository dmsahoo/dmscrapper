# Regulatory Intelligence (internal workspace)

Turns the clean regulatory documents collected by the scraper (repository regulatory_scraper) into
searchable text and CPDAP CPL records, and serves them in the Regulatory Scanner app with an AI Assistant.

## How it works

| Step | Job and task | Notebook | What it does |
|---|---|---|---|
| Set up | `job_sng_rcm_intelligence_setup`: `01_table_creation`, `02_index_create`, `03_view_creation` | `00_table_creation_rcm_intelligence`, `04_index_rcm_doc_chunk`, `01_view_creation_rcm_intelligence` | Schemas, volumes, tables, the Vector Search index, views, the tool function and the app grants |
| 1. Copy | `job_sng_rcm_copy_daily`, `01_copy` (10:00 SGT) | `02_ingest_rcm_copy` | Copies only documents with the verdict `CLEAN`, checks the fingerprint before and after, records each copy in `rcm_promotion_log` |
| 2. Parse and chunk | `job_sng_rcm_ingest_daily`, `01_parse_chunk` | `03_ingest_rcm_parse_chunk` | Bronze `rcm_doc_raw` (file content), Silver `rcm_doc_content` (text of every version), Gold `rcm_doc_chunk` (chunks of the current version) |
| 3. Index sync | `job_sng_rcm_ingest_daily`, `02_index_sync` | `04_index_rcm_doc_chunk` | Syncs the Vector Search index `idx_rcm_doc_chunk` and checks that it holds every chunk |
| 4. Analysis | `job_sng_rcm_ingest_daily`, `03_regulator_analysis` | `05_agent_rcm_regulator` | Regulator Agent: 12 CPDAP CPL fields per new version, code checks, version differences, task state in `rcm_agent_task` |
| App | `rcm-scanner-<env>dw` | `app/app.py` | Latest changes tab and the AI Assistant (Q and A Agent with guardrails G1 and G2) |
| Test | none | `06_agent_rcm_qa` | Runs the Q and A Agent with the standard test questions |

The copy job starts the ingest job when it finishes (task `02_run_ingest`). The index sync and the analysis
run even when some documents failed to parse (Run if: All done).

## Repository layout

| Path | Content |
|---|---|
| `databricks.yml`, `resources/` | Deployment bundle: wheel, notebooks, config, three jobs and the app, with targets dev, uat and prod |
| `pyproject.toml` | Build file of the `rcm_intelligence` wheel |
| `src/rcm_intelligence/` | Shared code (module list in `__init__.py`) |
| `src/rcm_intelligence/ddl/` | SQL for tables (`table_*`) and views, function and grants (`view_*`), inside the wheel |
| `src/rcm_intelligence/prompts/` | `rcm_regulator_base.md`, `rcm_regulator_mas.md`, `rcm_qa_system.md`, each with a version line, inside the wheel |
| `notebooks/` | Job entry points, SMBC ADP names, a markdown cell (Purpose, Input, Output) before every code cell |
| `app/app.py`, `requirements.txt` | Databricks App. requirements.txt installs this package from the repository root |
| `config/common_config_<env>.yaml` | Catalog, layer schemas, every object name, Vector Search, model, limits, guardrails |
| `tests/` | Unit tests, run in CI with `pytest` |

## Configuration: no folder paths in the code

1. Job clusters carry `RCM_ENV`, `RCM_CONFIG_DIR` and `RCM_RELEASE_TAG`, set by the bundle. The app gets the same variables from the bundle app definition.
2. The code reads `config/common_config_<RCM_ENV>.yaml`, which states its own environment. The code stops if they differ.
3. The shared code and the SQL and prompt files are inside the wheel, so notebooks only import.
4. For an interactive run without these variables, fill the optional notebook widgets `env` and `config_dir`.

## Naming (SMBC ADP Development Standards, P02 Appendix A)

| Layer (schema from the config) | Objects |
|---|---|
| raw, for example `sng_stg_raw` | `rcm_promotion_log`, `rcm_doc_raw`, volume `vol_documents` |
| curated, for example `sng_curated_rcm` | `rcm_doc_content` |
| aggregated, for example `sng_agg_rcm` | `rcm_doc_chunk`, `rcm_doc_analysis`, `rcm_doc_diff` |
| ai, for example `sng_ai_rcm` | `idx_rcm_doc_chunk`, `fn_get_changes`, `rcm_agent_task`, `rcm_guardrail_log`, `rcm_user_request_log`, volume `vol_logs` |
| user_view, for example `sng_agg_rcm_usr_view` | `v_rcm_app_updates`, `v_rcm_app_history` |

Jobs follow `job_<branch>_<use_case>_<process>_<frequency>`, tasks `<nn>_<process>`, the app `<use_case>-<purpose>-<env>dw`.

## Search: Databricks Vector Search only

1. Delta Sync index `idx_rcm_doc_chunk` on `rcm_doc_chunk` (Change Data Feed on), primary key `chunk_id`, managed embeddings from `chunk_text` with the embedding model in the config, triggered sync.
2. The ingest job syncs it after every run and checks that the index row count equals the chunk table.
3. The Q and A Agent queries it directly (hybrid, with a regulator filter). The old SQL search and `rcm_chunk_embedding` are removed.

## Logs and traceability

| What | Where |
|---|---|
| Copy and ingest state | `rcm_promotion_log` |
| Regulator Agent task state | `rcm_agent_task` (QUEUED, ANALYSED, VALIDATED, FLAGGED, ERROR) |
| What produced a record | `rcm_doc_analysis`: agent_name, agent_version, prompt_version, model_endpoint, release_tag, trace_id |
| Guardrail results | `rcm_guardrail_log` (question as a hash only) |
| Who asked what | `rcm_user_request_log` (user, outcome, trace link; restricted access) |
| Technical run logs | `vol_logs/<job>/<regulator>/<yyyy-mm-dd>/<run_id>.log`, JSON lines |
| Model calls | MLflow traces under `mlflow_experiment_root` |

## Deploy and run

1. The scraper repository is deployed first (it writes `rcm_pdf_catalog` with the verdict `CLEAN`).
2. Fill `config/common_config_<env>.yaml` and the bundle variables with the values from the AI Admin.
3. The CI workflow runs `pytest`, then `databricks bundle validate`, `deploy` (with `--var release_tag=<tag>`) and `run job_sng_rcm_intelligence_setup`.
4. `job_sng_rcm_copy_daily` runs at 10:00 SGT and starts the ingest job.

## Required access

| Identity | Needs |
|---|---|
| Job service principal | ACCESS on the landing and AI services service credentials; read on the external `rcm_pdf_catalog`; USE, SELECT and MODIFY on the five layer schemas; READ and WRITE VOLUME on both volumes; CREATE TABLE on the ai schema (index); CAN USE on the Vector Search endpoint; EXECUTE on the model service |
| App service principal | Grants in `view_03_app_grants.sql`; CAN USE on the SQL warehouse and READ on the secret scope (app resources); EXECUTE on the model service; CAN USE on the Vector Search endpoint (to confirm in DEV) |
| Access connector behind the landing credential | Storage Blob Data Reader on the landing container |

## To confirm in DEV

1. Installing the package from the repository root through `requirements.txt` (line with a dot) in Databricks Apps.
2. `GRANT SELECT ON TABLE` for the Vector Search index, and whether the app also needs CAN USE on the endpoint.
3. That MODIFY alone lets the app insert into the two log tables.
