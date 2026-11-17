/* =====================================================================
   table_08_rcm_agent_task.sql   (ai schema)
   One task per document version for the Regulator Agent, and its state.
     QUEUED     waiting for analysis
     ANALYSED   the model answered, the code checks are running
     VALIDATED  the record passed the checks
     FLAGGED    the record failed the checks after the rework limit (human review)
     ERROR      a technical failure (model or table); picked up again by the next run
   ===================================================================== */

CREATE TABLE IF NOT EXISTS ${table_agent_task} (
    document_id      STRING    NOT NULL COMMENT 'Document identifier',
    version          INT       NOT NULL COMMENT 'Document version being processed',
    regulator_id     STRING    NOT NULL COMMENT 'Selects the regulator prompt module',
    agent_name       STRING             COMMENT 'Agent that processes the task',
    agent_version    STRING             COMMENT 'Version of the rcm_intelligence package',
    state            STRING    NOT NULL COMMENT 'QUEUED, ANALYSED, VALIDATED, FLAGGED or ERROR',
    rework_count     INT                COMMENT 'Rework rounds used',
    last_step_output STRING             COMMENT 'Short note of the last completed step',
    failure_reason   STRING             COMMENT 'Reason for the last failure',
    run_id           STRING             COMMENT 'Analysis run',
    trace_id         STRING             COMMENT 'MLflow trace',
    updated_at_sgt   TIMESTAMP          COMMENT 'Last state change (SGT)',
    CONSTRAINT pk_rcm_agent_task PRIMARY KEY (document_id, version) NOT ENFORCED
)
USING DELTA
COMMENT 'Regulator Agent task state per document version';
