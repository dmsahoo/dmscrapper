/* =====================================================================
   08_get_changes_function.sql
   get_changes: the tool the Q and A Agent uses for questions like
   what changed for MAS in October ? It reads the validated CPDAP CPL records
   from v_app_updates (the same data the app shows). Works for all regulators.
   ===================================================================== */

CREATE OR REPLACE FUNCTION ${catalog}.${schema}.get_changes(
    from_date  DATE   DEFAULT NULL COMMENT 'First date to include (yyyy-mm-dd), or empty for no start limit',
    to_date    DATE   DEFAULT NULL COMMENT 'Last date to include (yyyy-mm-dd), or empty for no end limit',
    regulator  STRING DEFAULT NULL COMMENT 'Regulator id such as MAS, or empty for all regulators'
)
RETURNS TABLE (
    document_id               STRING,
    regulator_id              STRING,
    law_or_regulation         STRING,
    change_date               DATE,
    date_introduced           STRING,
    effective_date            STRING,
    summary_key_items         STRING,
    compliance_risk_category  STRING,
    importance                STRING,
    relevance                 STRING,
    source_url                STRING
)
COMMENT 'Lists the regulatory changes published between two dates, newest first (maximum 50). Uses the regulator website date, or the date collected when the website gives no date.'
RETURN
    WITH changes AS (
        SELECT document_id, regulator_id, law_or_regulation,
               -- the date of the change: the regulator website date, or the date we collected it
               coalesce(published_date, info_collected_date) AS change_date,
               date_introduced, effective_date, summary_key_items,
               compliance_risk_category, importance, relevance, source_url
        FROM ${catalog}.${schema}.v_app_updates
        WHERE get_changes.regulator IS NULL OR regulator_id = get_changes.regulator
    ),
    ranked AS (
        SELECT *, row_number() OVER (ORDER BY change_date DESC, law_or_regulation) AS rn
        FROM changes
        WHERE (get_changes.from_date IS NULL OR change_date >= get_changes.from_date)
          AND (get_changes.to_date   IS NULL OR change_date <= get_changes.to_date)
    )
    -- newest first, at most 50 rows so the answer stays short for the model
    SELECT document_id, regulator_id, law_or_regulation, change_date, date_introduced, effective_date,
           summary_key_items, compliance_risk_category, importance, relevance, source_url
    FROM ranked
    WHERE rn <= 50;
