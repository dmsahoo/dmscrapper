# Regulatory Scraper (external workspace)

Collects regulatory publications from regulator websites (MAS first), saves each document version in
the landing Azure Blob Storage, and records every document, version and malware scan verdict in
Unity Catalog. The internal workspace (repository regulatory_intelligence) copies only documents
with the verdict CLEAN.

## How it works

| Step | Job and task | Notebook | What it does |
|---|---|---|---|
| Set up | `job_sng_rcm_scraper_setup` | `00_table_creation_rcm_scraper`, `01_view_creation_rcm_scraper` | Creates the schema objects, the two tables, the logs volume and the monitoring views |
| 1. Resolve | `job_sng_rcm_scrape_daily`, `01_resolve_regulators` | `02_ingest_rcm_resolve_regulators` | Turns the `regulators` parameter (ALL, MAS, MAS,PDPC) into the list of regulators |
| 2. Scrape | `job_sng_rcm_scrape_daily`, `02_scrape_regulators` (For each) | `03_ingest_rcm_scrape` | Headless Microsoft Edge (Playwright) reads the listing pages, downloads documents, decides NEW, CHANGED or SKIPPED with a SHA 256 fingerprint, saves each version, verdict `PENDING` |
| 3. Scan verdict | `job_sng_rcm_scan_verdict_daily`, `01_scan_verdict` | `04_ingest_rcm_scan_verdict` | Reads the Microsoft Defender for Storage blob index tag and records `CLEAN`, `MALICIOUS`, `SCAN_ERROR` or keeps `PENDING` |

## Repository layout

| Path | Content |
|---|---|
| `databricks.yml`, `resources/` | Deployment bundle: wheel, notebooks, config and the three jobs, with targets dev, uat and prod |
| `pyproject.toml` | Build file of the `rcm_scraper` wheel |
| `src/rcm_scraper/` | Shared code, installed on the job clusters as a wheel (see the module list in `__init__.py`) |
| `src/rcm_scraper/ddl/` | SQL files for tables (`table_*`) and views (`view_*`), shipped inside the wheel |
| `notebooks/` | Job entry points, SMBC ADP names, a markdown cell (Purpose, Input, Output) before every code cell |
| `config/common_config_<env>.yaml` | Names, landing storage, verdict mapping, logging, scan defaults and regulator list per environment |
| `config/regulators/<regulator>_config.yaml` | Website rules of one regulator |
| `scripts/playwright_init.sh` | Cluster init script (Playwright and Microsoft Edge), with its permission needs in the header |
| `tests/` | Unit tests of the pure functions, run in CI with `pytest` |

## Configuration: no folder paths in the code

The notebooks never work out folder paths (no `os.getcwd`, no `sys.path`).

1. The shared code is the `rcm_scraper` wheel, installed on the job cluster by the bundle, so notebooks import it directly.
2. The job clusters carry two environment variables, set by the bundle:
   1. `RCM_ENV`: dev, uat or prod
   2. `RCM_CONFIG_DIR`: the deployed `config` folder
3. The code reads `config/common_config_<RCM_ENV>.yaml`. The file states its own `environment`, and the code stops if it does not match `RCM_ENV`.
4. For an interactive run without these variables, fill the optional notebook widgets `env` and `config_dir`.

Catalog and schema names come from the AI Admin and go in the config file. Names with a hyphen work, because the code quotes every name.

## Naming (SMBC ADP Development Standards, P02 Appendix A)

| Object | Name |
|---|---|
| Schema | Raw layer schema `<branch>_stg_raw`, for example `sng_stg_raw` (from the config) |
| Tables | `rcm_pdf_catalog`, `rcm_scraping_audit_log` |
| Views | `v_rcm_run_history`, `v_rcm_scrape_coverage`, `v_rcm_scan_backlog`, `v_rcm_recent_failures` |
| Volume | `vol_logs` |
| Jobs | `job_sng_rcm_scrape_daily`, `job_sng_rcm_scan_verdict_daily`, `job_sng_rcm_scraper_setup` |
| Tasks | `<nn>_<process>`, for example `01_resolve_regulators` |
| Notebooks | `00_table_creation_rcm_scraper`, `01_view_creation_rcm_scraper`, `ingest_rcm_<purpose>` |

## Malware scan verdict

| Defender tag value | `malware_scan_verdict` | Next step |
|---|---|---|
| No tag yet | `PENDING` | Checked again on the next run. SQL alert after 24 hours |
| No threats found | `CLEAN` | Can be copied to the internal zone |
| Malicious | `MALICIOUS` | Never copied. The job fails (security alert) |
| Any other value | `SCAN_ERROR` | Never copied. The job fails, then checked again on every run |

The mapping is in the `malware_scan` block of the config. Matching ignores spaces and upper or lower case,
and any unknown value becomes `SCAN_ERROR`, never `CLEAN`. The original Defender text is kept in
`malware_scan_result_raw`.

## Logging

1. Every log line carries stage, regulator_id, run_id and the SGT time.
2. Each run writes one JSON lines file to `/Volumes/<catalog>/<schema>/vol_logs/scraper/<regulator_id>/<yyyy-mm-dd>/<run_id>.log`.
3. `rcm_scraping_audit_log` stays the main record. The log files hold the technical detail.

## Deploy and run

1. The AI Admin gives the catalog, schema, landing storage, service credential and job service principal. Fill `config/common_config_<env>.yaml` and the bundle variables.
2. Upload `scripts/playwright_init.sh` to an allowlisted volume, and set `init_script_path`.
3. The CI workflow runs `pytest`, then `databricks bundle validate`, `deploy` and `run job_sng_rcm_scraper_setup`.
4. The two daily jobs run on their schedules (07:00 and 08:00 SGT).

## Required access

| Identity | Needs |
|---|---|
| Access connector behind the landing service credential | Storage Blob Data Contributor on the landing container (write files and read blob index tags) |
| Job service principal | ACCESS on the service credential. USE CATALOG, USE SCHEMA, SELECT and MODIFY on the scraper schema, READ VOLUME and WRITE VOLUME on `vol_logs`, READ VOLUME on the init script volume |
| Workspace admin | Adds the init script volume path to the init script allowlist |
| Cluster network | See the header of `scripts/playwright_init.sh` (package sites at cluster start) and the regulator websites |

## Development

1. Work in a Git folder in your workspace. Deploy to your dev target with `databricks bundle deploy -t dev`, which builds and installs the wheel on the dev jobs.
2. Run the unit tests locally: `pip install -e .[test]` and then `pytest`.
