/* =====================================================================
   table_10_rcm_user_request_log.sql   (ai schema)
   One row per question asked in the AI Assistant: who asked, when, and the outcome.
   Holds the user identity, so access is restricted to the support and audit groups.
   The question is stored only as a SHA 256 hash. It links to the MLflow trace through trace_id.
   ===================================================================== */

CREATE TABLE IF NOT EXISTS ${table_user_request_log} (
    request_id      STRING    NOT NULL COMMENT 'Unique identifier of the request',
    user_email      STRING             COMMENT 'Signed in user (from the app), empty for notebook tests',
    session_id      STRING             COMMENT 'App session',
    question_hash   STRING             COMMENT 'SHA 256 of the question, never the plain text',
    status          STRING             COMMENT 'ANSWERED, NOT_FOUND, BLOCKED_QUESTION or BLOCKED_ANSWER',
    tools_used      STRING             COMMENT 'Tools called, with their arguments',
    prompt_version  STRING             COMMENT 'Version of the Q and A prompt',
    release_tag     STRING             COMMENT 'Release that answered',
    trace_id        STRING             COMMENT 'MLflow trace',
    created_at_sgt  TIMESTAMP          COMMENT 'Request time (SGT)',
    CONSTRAINT pk_rcm_user_request_log PRIMARY KEY (request_id) NOT ENFORCED
)
USING DELTA
COMMENT 'AI Assistant requests: user, outcome and trace link (restricted access)';
