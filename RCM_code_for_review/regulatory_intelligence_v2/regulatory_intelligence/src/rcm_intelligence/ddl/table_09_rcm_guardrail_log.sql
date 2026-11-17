/* =====================================================================
   table_09_rcm_guardrail_log.sql   (ai schema)
   One row per guardrail check of the Q and A Agent (G1 on the question, G2 on the answer).
   The question is stored only as a SHA 256 hash. The answer and the blocked text are never stored.
   ===================================================================== */

CREATE TABLE IF NOT EXISTS ${table_guardrail_log} (
    log_id          STRING    NOT NULL COMMENT 'Unique identifier of the row',
    request_id      STRING    NOT NULL COMMENT 'Request in rcm_user_request_log',
    check_point     STRING    NOT NULL COMMENT 'G1 (question) or G2 (answer)',
    check_name      STRING    NOT NULL COMMENT 'PROMPT_SHIELD, HARMFUL_CONTENT, PII, SOURCES or SERVICE',
    result          STRING    NOT NULL COMMENT 'PASSED, BLOCKED, MASKED or ERROR',
    category        STRING             COMMENT 'Category found, for example Violence 4 or PhoneNumber',
    question_hash   STRING             COMMENT 'SHA 256 of the question, never the plain text',
    trace_id        STRING             COMMENT 'MLflow trace',
    agent_name      STRING             COMMENT 'Agent that ran the check',
    created_at_sgt  TIMESTAMP          COMMENT 'Check time (SGT)'
)
USING DELTA
COMMENT 'Guardrail results of the Q and A Agent, without question or answer text';
