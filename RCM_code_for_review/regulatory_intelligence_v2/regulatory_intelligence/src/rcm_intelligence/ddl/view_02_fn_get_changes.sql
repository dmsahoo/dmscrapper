/* =====================================================================
   view_02_fn_get_changes.sql   (ai schema)
   fn_get_changes: the tool the Q and A Agent uses for questions such as
   what changed for MAS in October ? It reads the validated records from v_rcm_app_updates
   (the same data the app shows). Works for every regulator. Read only.
   ===================================================================== */

CREATE OR REPLACE FUNCTION ${function_get_changes}(
    from_date  DATE   DEFAULT NULL COMMENT 'First date to include (yyyy-mm-dd), or empty for no start limit',
    to_date    DATE   DEFAULT NULL COMMENT 'Last date to include (yyyy-mm-dd), or empty for no end limit',
    regulator  STRING DEFAULT NULL COMMENT 'Regulator id such as MAS, or empty for all regulators'
)
RETURNS TABLE (
    document_id               STRING,
    regulator_id              STRING,
    law_or_regulation         STRING,
    change_date               DATE,
    change_type               STRING,
    date_introduced           STRING,
    effective_date            STRING,
    summary_key_items         STRING,
    compliance_risk_category  STRING,
    importance                STRING,
    relevance                 STRING,
    source_url                STRING
)
COMMENT 'Lists the regulatory changes published between two dates, newest first (maximum 50). Uses the regulator website date, or the date collected when the website gives no date. Read only.'
RETURN
    WITH changes AS (
        SELECT document_id, regulator_id, law_or_regulation,
               coalesce(published_date, info_collected_date) AS change_date,
               change_type, date_introduced, effective_date, summary_key_items,
               compliance_risk_category, importance, relevance, source_url
        FROM ${view_app_updates}
        WHERE regulator IS NULL OR regulator_id = regulator
    ),
    ranked AS (
        SELECT *, row_number() OVER (ORDER BY change_date DESC, law_or_regulation) AS rn
        FROM changes
        WHERE (from_date IS NULL OR change_date >= from_date)
          AND (to_date   IS NULL OR change_date <= to_date)
    )
    SELECT document_id, regulator_id, law_or_regulation, change_date, change_type, date_introduced,
           effective_date, summary_key_items, compliance_risk_category, importance, relevance, source_url
    FROM ranked
    WHERE rn <= 50;
