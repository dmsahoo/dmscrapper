# Regulatory Scraper (external workspace)

Collects regulatory documents (PDF files and HTML page snapshots) from the websites of Singapore regulators,
lands them in Azure Blob Storage for the Defender malware scan, and records every document and run in two
Unity Catalog tables. The internal workspace (repository regulatory_intelligence) copies only the clean documents.

## Repository layout

| Path | Content |
|---|---|
| `notebooks/00_run_ddl.py` | Creates the schema, the two tables and the monitoring views from the `ddl` folder |
| `notebooks/00_resolve_regulators.py` | First task of the scrape job: lists the enabled regulators for the next task |
| `notebooks/01_orchestrator.py` | Scrapes one regulator for the scan period and lands its documents |
| `notebooks/04_scan_verdict.py` | Reads the Defender verdict (blob index tag) of each landed document into `pdf_catalog` |
| `scrapers/` | Shared code: `base_scraper.py` (browser), `settings.py` (config), `tracking.py` (catalog, audit, storage) and one scraper per regulator |
| `config/common_config.yaml` | Catalog, schema, landing storage, scan defaults and the list of regulators |
| `config/regulators/` | One config per regulator: categories, selectors and pages |
| `ddl/` | SQL with the placeholders `${catalog}` and `${schema}`, filled from the config by `00_run_ddl` |
| `scripts/playwright_init.sh` | Cluster init script: installs Playwright and Microsoft Edge |

## Before the first run: fill in the config

Open `config/common_config.yaml` and set the values for the environment.

| Setting | DEV value | Note |
|---|---|---|
| `catalog` | `ucaiextdev` | External catalog, provided by the AI Admin |
| `schema` | `sng_rcm_ai` | P02 pattern `<branch>_<project>_ai`. Created by `00_run_ddl` if it does not exist |
| `landing.storage_account` | `<landing_storage_account>` | Blob Storage account with hierarchical namespace off, so Defender can write blob index tags |
| `landing.service_credential` | `<landing_service_credential>` | Unity Catalog service credential with write access and blob tag read access on the landing container |

For UAT and PROD, copy the file (for example `common_config_uat.yaml`), change only the values, and point the
`common_config_path` job parameter to it. Never put secrets in the config.

## Set up, in this order

1. **Cluster:** a classic single node job cluster (Playwright only needs the driver), with the init script
   `scripts/playwright_init.sh`. Optional cluster environment variables: `PLAYWRIGHT_VERSION` (default 1.47.0) and
   `PLAYWRIGHT_ARTIFACTS_DIR` (offline mode: Playwright wheels from a volume, Edge from the bank approved apt mirror).
2. **Libraries on the cluster:** `azure-storage-blob` and `PyYAML`, at the versions approved for SMBC.
3. **Tables:** run `notebooks/00_run_ddl` once, with `common_config_path` set to the config of the environment.
   It creates `<catalog>.<schema>` with `pdf_catalog`, `scraping_audit_log` and the monitoring views.
4. **Scrape job:** task 1 `00_resolve_regulators`, then task 2 `01_orchestrator` for each regulator (For Each).
5. **Scan verdict job:** `04_scan_verdict`, scheduled after the scrape job, once Defender has scanned the files.

## Network access needed by the cluster

Outbound HTTPS (port 443) to the regulator websites, to the landing storage account, and, when the init script
runs in online mode, to the package sources it uses (Ubuntu, PyPI and Microsoft for Edge). In offline mode only the
bank approved mirror and the volume with the Playwright wheels are needed.

## Required access

| Identity | Needs |
|---|---|
| Job service principal | USE CATALOG on the catalog; USE SCHEMA, CREATE TABLE and MODIFY on the schema; ACCESS on the landing service credential |
| Access connector behind the landing credential | Storage Blob Data Contributor on the landing container (write files, read blob index tags) |
| Internal workspace job identity | SELECT on `pdf_catalog` (read only) |
