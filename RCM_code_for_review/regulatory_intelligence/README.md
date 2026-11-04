# Regulatory Intelligence (internal workspace)

Turns the clean regulatory documents collected by the scraper (repository regulatory_scraper) into CPDAP CPL
records, and gives users a Databricks App with a Latest changes tab and an AI Assistant.

## How it works

| Step | Notebook | What it does |
|---|---|---|
| Copy | `01_copy_job` | Copies only files with the verdict No threats found from the landing storage to the internal volume, checks the SHA 256 fingerprint before and after, and records each copy in `rcm_promotion_log` |
| Parse and chunk | `02_parse_and_chunk` | PDF text with `ai_parse_document`, HTML text with the Python standard library. Saves the full text of every version in `rcm_doc_content` (Silver) and the chunks of the current version in `rcm_doc_chunk` (Gold) |
| Analyse | `03_regulator_agent` | The Regulator Agent fills the 12 CPDAP CPL fields for each new document version, checks them in code (up to 2 reworks) and saves them in `rcm_doc_analysis`. For an updated document, code compares the old and new full text (`rcm_doc_diff`) and the agent explains what changed |
| Embed | `04_embed_chunks` | Adds an embedding for every new chunk in `rcm_chunk_embedding`, used by the search tool |
| Q and A test | `05_qa_agent` | Runs the Q and A Agent in a notebook, with the standard test questions |
| App | `app/app.py` | Latest changes (one row per document, labelled New or Updated, with what changed) and the AI Assistant |

The Q and A Agent (`agents/qa_agent.py`) answers from two Unity Catalog functions: `get_changes` (validated
records in a period) and `search_chunks` (meaning plus keyword search over the chunks). Guardrails
(`guardrails/guardrails.py`) check every question (G1: prompt attack, harmful content, PII removal) and every
answer (G2: source links must come from the tool results, harmful content). Both fail closed.

## Folders

| Folder | Content |
|---|---|
| `config/` | `<env>.yaml`: every name and limit for one environment (dev, uat, prod) |
| `common/` | `config.py`: loads the config and builds table names |
| `agents/` | `qa_agent.py`: the Q and A Agent, shared by the notebook and the app |
| `guardrails/` | `guardrails.py`: G1 and G2 |
| `prompts/` | `base_prompt.md` and `regulators/<regulator>.md` (Regulator Agent), `qa_prompt.md` (Q and A Agent) |
| `notebooks/` | `00_run_ddl` (setup) and the pipeline steps 01 to 05 |
| `app/` and `app.yaml` | The Databricks App. It is deployed from the repository root, so it can use the shared folders |
| `ddl/` | Tables, views, functions and grants with `${...}` placeholders, run by `00_run_ddl` |

## Configuration

Nothing is hardcoded in the code. Notebooks read `config/<env>.yaml` using the `env` widget or job parameter
(default dev). The app reads it using `RCM_ENV` in `app.yaml`. Values in `<angle brackets>` are placeholders.
No secrets are stored in the repository.

## Setup and run order

1. Fill in `config/dev.yaml` (copy it to `uat.yaml` and `prod.yaml` for other environments).
2. Create the model service in the Unity AI Gateway (AI Admin) and set its name in the config.
3. Run `notebooks/00_run_ddl` (safe to run again; the grants run last).
4. Daily pipeline, after the scan verdict job in the external workspace: `01_copy_job`, `02_parse_and_chunk`,
   `04_embed_chunks`, `03_regulator_agent`.
5. Deploy the app from the repository root, with the resources `sql-warehouse`, `content-safety-key` and `language-key`.

## Current design choices to review for production

| Area | Current | Production option |
|---|---|---|
| Search | SQL search on `rcm_chunk_embedding` (no idle cost) | Databricks Vector Search (hybrid, Delta Sync, triggered). Only the body of `search_chunks` changes |
| App sign in to Azure AI services | Keys in a Databricks secret scope, given as app secret resources | An Entra identity, or calling the services through a serving endpoint |
| Libraries in notebooks 03 and 05 | `%pip install` at run time | A pinned serverless environment, because the internal zone may not reach PyPI |
| Model | The model service set in the config | The approved model service (for example Claude) through the Unity AI Gateway |
