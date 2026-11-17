'''
rcm_scraper: shared code of the Regulatory Change Management (RCM) scraper.

Installed on the job clusters as a wheel by the deployment bundle, so notebooks import it
directly (no folder paths, no os.getcwd).

Modules
  settings        environment, config files and the regulator list
  logging_setup   per regulator logging and run log files
  common          status values, time, scan period, hashing, Delta retry
  blob_storage    landing Blob Storage (no HNS) reads and verified writes
  catalog         rcm_pdf_catalog: duplicate decisions and writes
  audit           rcm_scraping_audit_log writes
  run_guard       stops overlapping runs
  scan_verdict    Defender tag text to PENDING, CLEAN, MALICIOUS or SCAN_ERROR
  ddl_runner      runs the SQL files in the ddl folder
  base_scraper    browser logic shared by every regulator
  mas_scraper     MAS listing pages
'''

__version__ = '2.0.0'
