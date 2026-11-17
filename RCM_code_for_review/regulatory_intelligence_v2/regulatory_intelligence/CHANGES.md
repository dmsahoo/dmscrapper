# Changes for the client code review (version 2.0.0)

## Review points and what changed

| # | Review point | Change |
|---|---|---|
| 1 | Tables in sync with the TSD and SMBC naming | One schema per SMBC ADP layer (raw, curated, aggregated, ai, user_view). New tables from the TSD: `rcm_doc_raw` (Bronze, TSD rcm_stg_raw), `rcm_agent_task`, `rcm_guardrail_log`, `rcm_user_request_log`. TSD columns added to Silver, Gold and the analysis table (category, title, page range, hash, parser, is_current, run ids, agent and release traceability). Views `v_rcm_*`, function `fn_get_changes`, index `idx_rcm_doc_chunk`, volumes `vol_documents` and `vol_logs` |
| 2 | No `os.getcwd`, markdown before every code cell | Shared code, SQL and prompts are the `rcm_intelligence` wheel; the app installs the same package. Every notebook has a header table and a Purpose, Input, Output cell before each code cell. No `%pip install` in notebooks: libraries are pinned in the job definitions |
| 3 | Logging | Labelled logging (stage, regulator, run, SGT time) and one JSON log file per run in `vol_logs/<job>/<regulator>/<day>/`. Guardrail and request log tables for the AI Assistant |
| 4 | File naming | Notebooks numbered in run order, `<nn>_<action>_rcm_<subject>` (`00_table_creation` to `06_agent_rcm_qa`). Prompts `rcm_<agent>_<role>.md`. Jobs `job_sng_rcm_*`, tasks `<nn>_<process>`, app `rcm-scanner-<env>dw` |
| 5 | apt-get | Not used in this repository (the init script is only in the scraper) |
| 6 | Distribution of common files | `databricks.yml` bundle with dev, uat and prod targets for the jobs and the app, `.gitignore`, pinned `requirements.txt`, set up job |
| 7 | Environment variables at cluster level | `RCM_ENV`, `RCM_CONFIG_DIR`, `RCM_RELEASE_TAG` on every job cluster and in the app definition. One config file per environment |
| Vector Search only | | `rcm_chunk_embedding`, `04_embed_chunks` and the SQL search function are removed. New Delta Sync index with managed embeddings, synced and validated after each ingest, queried by the Q and A Agent (hybrid, regulator filter) |

## Other fixes

1. The copy job reads the new scraper table `rcm_pdf_catalog` and copies only the verdict `CLEAN` (the config refuses any other value).
2. A consultation on proposed amendments is no longer labelled Amendment (event type rule).
3. The prompt version stored on each record now comes from the prompt files (it was a fixed text in the notebook and did not match the file).
4. The Regulator Agent saves each document on its own, so a crash never loses finished work, and a technical failure moves the task to ERROR (retried next run) instead of a FLAGGED record.
5. A tool error is no longer passed to the model as raw text (only the error type).
6. Compute: classic single node job clusters with Databricks Runtime 17.3 LTS, so the environment variables and pinned libraries apply (serverless does not take cluster environment variables).

## Old to new file names

| Old | New |
|---|---|
| `common/config.py` | `src/rcm_intelligence/settings.py` |
| `notebooks/00_run_ddl.py` | `notebooks/00_table_creation_rcm_intelligence.py`, `01_view_creation_rcm_intelligence.py` |
| `notebooks/01_copy_job.py` | `notebooks/02_ingest_rcm_copy.py` |
| `notebooks/02_parse_and_chunk.py` | `notebooks/03_ingest_rcm_parse_chunk.py` (plus the Bronze load) and `src/rcm_intelligence/text_processing.py` |
| `notebooks/04_embed_chunks.py` | Removed. `notebooks/04_index_rcm_doc_chunk.py` and `src/rcm_intelligence/vector_index.py` |
| `notebooks/03_regulator_agent.py` | `notebooks/05_agent_rcm_regulator.py` and `src/rcm_intelligence/analysis.py` |
| `notebooks/05_qa_agent.py` | `notebooks/06_agent_rcm_qa.py` |
| `agents/qa_agent.py`, `guardrails/guardrails.py` | `src/rcm_intelligence/qa_agent.py`, `guardrails.py` |
| `prompts/base_prompt.md`, `regulators/mas.md`, `qa_prompt.md` | `src/rcm_intelligence/prompts/rcm_regulator_base.md`, `rcm_regulator_mas.md`, `rcm_qa_system.md` |
| `ddl/01 to 10` | `src/rcm_intelligence/ddl/table_01 to table_10`, `view_01 to view_03` |
| `config/dev.yaml` | `config/common_config_dev.yaml`, `_uat.yaml`, `_prod.yaml` |
| `app.yaml` | App settings per target in `resources/app_rcm_scanner.yml` |

## TSD updates that follow from this code

1. Data model: the five layer schemas, and the columns of every table as in the `ddl` folder (the code keeps some names that differ from the TSD draft, for example `content_text`, `section_title`, `law_or_regulation`, `analysis_status`, `relevance_reason`, `check_problems`, and the change_type value UPDATED).
2. Fields in the TSD draft that the agent does not produce yet: `tier`, and `areas_of_impact` as a list. Either remove them from the TSD or plan them as a prompt change.
3. Search: Vector Search index, sync and validation, the hybrid query from the agent.
4. Jobs: names, tasks, schedule (copy 10:00 SGT starts ingest), Run if rules, compute.
5. C9 audit: guardrail log and request log columns, run log files.
6. Agents: the Supervisor and Report Writer agents in the TSD draft are not built. Mark them as a later phase, or plan them.

## Tested here

1. 17 unit tests pass (config and names, every SQL file fills its placeholders, text extraction and chunking with page ranges, version differences, code checks, prompt versions, guardrail log results, and a blocked question that is logged without its text).
2. The wheel builds with the SQL and prompt files inside.
3. Not run on Databricks yet: run the set up job, one copy and ingest run, the analysis and the app in DEV before the review.
