/* =====================================================================
   view_01_rcm_monitoring_views.sql
   Views for the operations dashboard and the Databricks SQL alerts.
   Run by notebooks/01_view_creation_rcm_scraper. Safe to run again (CREATE OR REPLACE VIEW).
   Timestamps are stored in SGT (the notebooks set the Spark session time zone to
   Asia/Singapore), so comparisons use current_timestamp().
   ===================================================================== */


/* 1. v_rcm_run_history: one row per run with its final status (RUNNING when no final row yet) */
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_rcm_run_history
COMMENT 'One row per scraper or scan verdict run, with its final status and counts'
AS
WITH runs AS (
    SELECT run_id, stage, regulator_id,
           min(event_time_sgt)                                        AS started_sgt,
           max_by(status, event_time_sgt)                             AS final_status,
           max(CASE WHEN status <> 'RUNNING' THEN end_time_sgt END)   AS ended_sgt,
           max(scan_start_date)                                       AS scan_start_date,
           max(scan_end_date)                                         AS scan_end_date,
           max(items_found)                                           AS items_found,
           max(documents_found)                                       AS documents_found,
           max(documents_new)                                         AS documents_new,
           max(documents_changed)                                     AS documents_changed,
           max(documents_skipped)                                     AS documents_skipped,
           max(documents_failed)                                      AS documents_failed,
           max_by(error_details, event_time_sgt)                      AS error_details
    FROM ${catalog}.${schema}.${table_audit_log}
    WHERE record_type = 'RUN'
    GROUP BY run_id, stage, regulator_id
)
SELECT *,
       round((unix_timestamp(ended_sgt) - unix_timestamp(started_sgt)) / 60.0, 1) AS duration_minutes
FROM runs;


/* 2. v_rcm_scrape_coverage: Coverage Log of the latest finished scrape run per regulator */
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_rcm_scrape_coverage
COMMENT 'Coverage Log: categories of the latest finished scrape run per regulator'
AS
WITH latest AS (
    SELECT regulator_id, max_by(run_id, started_sgt) AS run_id
    FROM ${catalog}.${schema}.v_rcm_run_history
    WHERE stage = 'SCRAPE' AND final_status IN ('SUCCESS', 'PARTIAL_SUCCESS', 'FAILED')
    GROUP BY regulator_id
)
SELECT a.regulator_id, a.run_id, a.scan_start_date, a.scan_end_date,
       a.category_id, a.category_name, a.status, a.items_found, a.documents_found,
       a.documents_new, a.documents_changed, a.documents_skipped, a.documents_failed,
       CASE WHEN a.status = 'SUCCESS' AND a.items_found = 0 THEN 'No items found in scan period'
            WHEN a.status = 'FAILED' THEN concat('Category failed: ', coalesce(a.error_details, ''))
            ELSE NULL END AS remarks
FROM ${catalog}.${schema}.${table_audit_log} a
JOIN latest l ON a.run_id = l.run_id
WHERE a.record_type = 'CATEGORY';


/* 3. v_rcm_scan_backlog: documents that cannot be copied yet (PENDING) or never (MALICIOUS,
      SCAN_ERROR), with their age. Rows from before the verdict mapping (verdict empty) show as PENDING. */
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_rcm_scan_backlog
COMMENT 'Documents with a verdict other than CLEAN, with age in hours, for alerts'
AS
SELECT regulator_id, category_id, document_id, file_name, current_version,
       coalesce(malware_scan_verdict, 'PENDING')                        AS malware_scan_verdict,
       malware_scan_result_raw, scan_timestamp_sgt, last_changed_sgt,
       round((unix_timestamp(current_timestamp())
              - unix_timestamp(last_changed_sgt)) / 3600.0, 1)          AS age_hours
FROM ${catalog}.${schema}.${table_pdf_catalog}
WHERE coalesce(malware_scan_verdict, 'PENDING') <> 'CLEAN';


/* 4. v_rcm_recent_failures: failures in the last 14 days, all stages */
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_rcm_recent_failures
COMMENT 'Failed or abandoned runs, categories, items and files in the last 14 days'
AS
SELECT event_time_sgt, stage, record_type, run_id, regulator_id, category_id, item_title,
       document_url, file_name, status, error_category, error_details
FROM ${catalog}.${schema}.${table_audit_log}
WHERE status IN ('FAILED', 'ABANDONED')
  AND event_time_sgt >= current_timestamp() - INTERVAL 14 DAYS;


/* =====================================================================
   Alert queries (Databricks SQL, Alerts). Each returns one row with
   value; trigger the alert when value > 0. Replace the placeholders.

   A1. No successful scrape in the last 2 days (daily schedule plus margin)
       SELECT CASE WHEN max(started_sgt) IS NULL
                     OR max(started_sgt) < current_timestamp() - INTERVAL 2 DAYS
                   THEN 1 ELSE 0 END AS value
       FROM <catalog>.<schema>.v_rcm_run_history
       WHERE stage = 'SCRAPE' AND final_status IN ('SUCCESS', 'PARTIAL_SUCCESS');

   A2. Scan verdict PENDING for more than 24 hours (malware_scan.pending_alert_hours)
       SELECT count(*) AS value FROM <catalog>.<schema>.v_rcm_scan_backlog
       WHERE malware_scan_verdict = 'PENDING' AND age_hours > 24;

   A3. Security alert: any MALICIOUS or SCAN_ERROR document
       SELECT count(*) AS value FROM <catalog>.<schema>.v_rcm_scan_backlog
       WHERE malware_scan_verdict IN ('MALICIOUS', 'SCAN_ERROR');

   A4. Latest scrape found zero items in every category (possible silent source change)
       SELECT CASE WHEN sum(items_found) = 0 THEN 1 ELSE 0 END AS value
       FROM <catalog>.<schema>.v_rcm_scrape_coverage;

   A5. Failed or abandoned runs in the last 24 hours
       SELECT count(*) AS value FROM <catalog>.<schema>.v_rcm_run_history
       WHERE final_status IN ('FAILED', 'ABANDONED')
         AND started_sgt >= current_timestamp() - INTERVAL 24 HOURS;
   ===================================================================== */
