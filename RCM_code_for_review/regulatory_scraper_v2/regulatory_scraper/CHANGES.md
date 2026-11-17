# Changes for the client code review (version 2.0.0)

## Review points and what changed

| # | Review point | Change |
|---|---|---|
| 1 | Tables in sync with the TSD and SMBC naming | Tables renamed to `rcm_pdf_catalog` and `rcm_scraping_audit_log`, views to `v_rcm_*`, schema from the config (raw layer `<branch>_stg_raw`). New column `malware_scan_result_raw`. Fixed verdict values PENDING, CLEAN, MALICIOUS, SCAN_ERROR, as in the TSD |
| 2 | No `os.getcwd`, markdown before every code cell | Shared code is now the `rcm_scraper` wheel, so notebooks import it directly. Every notebook has a header cell and a Purpose, Input, Output cell before each code cell |
| 3 | Logging per regulator | New `logging_setup.py`: labels (stage, regulator, run, SGT time) on every line, one JSON log file per run in `vol_logs/scraper/<regulator>/<day>/` |
| 4 | File naming | Notebooks follow SMBC ADP names, numbered in run order (00_ to 04_), stale reference to 03_reconcile removed, `tracking.py` (574 lines) split into five modules, including `blob_storage.py` as in the TSD |
| 5 | apt-get not in the TSD | Init script header lists why apt-get is used, the permissions, the allowlist and the network needs. Edge version can be pinned. Container image proposed for production |
| 6 | Distribution of common files | `databricks.yml` bundle with dev, uat and prod targets, `.gitignore`, `pyproject.toml` with pinned test tools, set up job for the DDL |
| 7 | Environment variables at cluster level | `RCM_ENV` and `RCM_CONFIG_DIR` set on every job cluster by the bundle. One config file per environment |

## Old to new file names

| Old | New |
|---|---|
| `notebooks/00_run_ddl.py` | `notebooks/00_table_creation_rcm_scraper.py` and `notebooks/01_view_creation_rcm_scraper.py` |
| `notebooks/00_resolve_regulators.py` | `notebooks/02_ingest_rcm_resolve_regulators.py` |
| `notebooks/01_orchestrator.py` | `notebooks/03_ingest_rcm_scrape.py` |
| `notebooks/04_scan_verdict.py` | `notebooks/04_ingest_rcm_scan_verdict.py` |
| `scrapers/settings.py` | `src/rcm_scraper/settings.py` (reads `RCM_ENV`, `RCM_CONFIG_DIR`) |
| `scrapers/tracking.py` | `src/rcm_scraper/common.py`, `blob_storage.py`, `catalog.py`, `audit.py`, `run_guard.py` |
| (new) | `src/rcm_scraper/logging_setup.py`, `scan_verdict.py`, `ddl_runner.py` |
| `scrapers/base_scraper.py`, `mas_scraper.py` | `src/rcm_scraper/` (imports updated, new versions saved as PENDING) |
| `ddl/01, 03, 05` | `src/rcm_scraper/ddl/table_01, table_02, table_03, view_01` |
| `config/common_config.yaml` | `config/common_config_dev.yaml`, `_uat.yaml`, `_prod.yaml` |
| (new) | `databricks.yml`, `resources/`, `pyproject.toml`, `.gitignore`, `tests/` |

## Must be done together with the internal repository

The internal copy job reads this catalog. When this version is deployed, the internal code must also
change in the same release:

1. Read `rcm_pdf_catalog` instead of `pdf_catalog` (internal config `external_*` names).
2. Copy only rows with `malware_scan_verdict = 'CLEAN'` (internal config `clean_verdict: CLEAN`).

## TSD updates that follow from this code

1. 4.1.1: file table (new paths and names), config per environment, wheel, environment variables.
2. 4.1.1 and 4.1.3: landing storage is Blob Storage without HNS (blob index tags), not ADLS or a volume. Path `<prefix>/<regulator>/<document id>/v<version>/<file>`.
3. 4.1.2: job names, task names, schedules (07:00 scrape, 08:00 scan verdict), set up job.
4. 4.1.3: verdict table with `malware_scan_result_raw`, recheck rule for PENDING and SCAN_ERROR, fail closed rule.
5. 4.1.8: run log files per regulator and day, columns of `rcm_scraping_audit_log`.
6. 4.1.10: wheel build in CI, bundle targets, unit tests.
7. New section: init script, apt-get, permissions, network, container image proposal.

## Tested here

1. 20 unit tests pass (verdict mapping, config from environment variables, name quoting, run log, duplicate decisions, scan period, audit rows, every SQL file fills all placeholders).
2. The wheel builds and contains the SQL files.
3. Not run on Databricks yet: run the set up job and one scrape in DEV before the review.
