# Regulatory Scraper (external workspace)

Collects regulatory publications from regulator websites (MAS first), saves each document in the landing
Azure Blob Storage, and records every document, version and malware scan verdict in Unity Catalog.
The internal workspace (repository regulatory_intelligence) copies only the clean files from here.

## How it works

1. **Scrape** (`notebooks/01_orchestrator.py`): for each enabled regulator, a headless Microsoft Edge browser
   (Playwright) reads the configured listing pages, opens each item page in the scan period, and downloads
   its PDF links (or saves an HTML snapshot for selected categories).
2. **Detect changes:** every file gets a SHA 256 fingerprint. A new link becomes version 1 (NEW). A known link
   with a different fingerprint becomes the next version (CHANGED). The same fingerprint is SKIPPED.
3. **Save:** each version is written to its own folder in the landing container, and recorded in `pdf_catalog`.
4. **Malware scan:** Microsoft Defender for Storage scans every upload and writes a blob index tag.
   `notebooks/04_scan_verdict.py` copies that verdict into `pdf_catalog`.
5. **Audit:** every run, category, item and file is recorded in `scraping_audit_log`.

## Folders

| Folder | Content |
|---|---|
| `config/` | `common_config.yaml` (catalog, schema, landing storage, scan defaults, regulator list) and `regulators/<regulator>_config.yaml` (website rules per regulator) |
| `scrapers/` | `settings.py` (reads and checks the config), `tracking.py` (catalog, audit and storage helpers), `base_scraper.py` (shared browser logic), `mas_scraper.py` (MAS listing pages) |
| `notebooks/` | `00_run_ddl` (creates the tables and views), `00_resolve_regulators`, `01_orchestrator` (the scrape), `04_scan_verdict` |
| `scripts/` | `playwright_init.sh`: cluster init script that installs Playwright and Microsoft Edge Stable |
| `ddl/` | Table and view definitions with `${catalog}.${schema}` placeholders |

## Configuration

Nothing is hardcoded in the code. All names come from `config/common_config.yaml`.
Values in `<angle brackets>` are placeholders to fill in. For another environment, copy the file
(for example `common_config_uat.yaml`) and set the job parameter `common_config_path` to it.
No secrets are stored in the repository: storage access uses a Unity Catalog service credential (managed identity).

## Setup and run order

1. Fill in `config/common_config.yaml`.
2. Run `notebooks/00_run_ddl` once (creates the schema, tables and monitoring views).
3. Scrape job: task `00_resolve_regulators`, then a for each task running `01_orchestrator` per regulator,
   on a classic job cluster with `scripts/playwright_init.sh` as init script.
   Parameters: `scan_mode` (INCREMENTAL, LAST_N_DAYS or CUSTOM), `scan_start_date`, `scan_end_date`, `regulators`, `common_config_path`.
4. Scan verdict job: `04_scan_verdict`, on a schedule after the scrape.

## Required access

| Identity | Needs |
|---|---|
| Access connector behind the scraper service credential | Write files and read blob index tags on the landing storage account |
| Job run as identity | ACCESS on the service credential, and USE, SELECT and MODIFY on the scraper schema |
| Cluster network | Outbound to pypi.org, files.pythonhosted.org, packages.microsoft.com, archive.ubuntu.com and security.ubuntu.com (init script), and to the regulator websites (scrape) |
