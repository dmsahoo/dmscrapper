# Databricks notebook source
# MAGIC %md
# MAGIC # 05_agent_rcm_regulator
# MAGIC
# MAGIC | Item | Detail |
# MAGIC |---|---|
# MAGIC | **Purpose** | The Regulator Agent: reads each new document version and fills the 12 CPDAP CPL fields. Code checks every answer (up to `max_rework` reworks) before it is saved. For an updated document, code first finds the exact wording changes, and the agent explains them. |
# MAGIC | **Job and task** | `job_sng_rcm_ingest_daily`, task `03_regulator_analysis` |
# MAGIC | **Parameters** | `max_documents` (default 100), `document_id` (optional: one specific document, for a rerun). Optional `env` and `config_dir`. |
# MAGIC | **Inputs** | `rcm_doc_chunk` (Gold), `rcm_doc_content` (Silver, for version differences), external `rcm_pdf_catalog` (document details), prompts `rcm_regulator_base.md` and `rcm_regulator_<regulator>.md` (inside the wheel), the model service |
# MAGIC | **Outputs** | `rcm_doc_analysis` (VALIDATED or FLAGGED), `rcm_doc_diff`, `rcm_agent_task` (task state), MLflow traces, one run log file |
# MAGIC | **Compute** | Single node classic job cluster, Databricks Runtime 17.3 LTS, pinned libraries `openai`, `mlflow`, `databricks-sdk` (from the job definition, no pip install in the notebook) |
# MAGIC | **Dependencies** | Wheel `rcm_intelligence`, cluster environment variables `RCM_ENV`, `RCM_CONFIG_DIR` and `RCM_RELEASE_TAG`, EXECUTE on the model service |
# MAGIC | **Fails when** | A document ends in task state ERROR (technical failure). FLAGGED records are a normal outcome for human review. |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Import the package
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Load the shared code. The agent rules that need no model (differences, the user message, the checks) are in `analysis.py` and are unit tested. |
# MAGIC | **Input** | Wheel `rcm_intelligence`, libraries `openai`, `mlflow`, `databricks-sdk` |
# MAGIC | **Output** | Imported functions, the logger. The Spark session time zone is set to Singapore. |

# COMMAND ----------

import json
from datetime import datetime

import mlflow
from databricks.sdk import WorkspaceClient
from openai import OpenAI

from rcm_intelligence.analysis import (ANALYSED, ERROR, FLAGGED, QUEUED, VALIDATED, build_user_message,
                                       check_record, event_type_of, find_differences, parse_json)
from rcm_intelligence.common import SGT, agent_version, current_trace_id, new_run_id, now_sgt, release_tag, run_sql_with_retry
from rcm_intelligence.logging_setup import RunLog, get_logger, set_log_context
from rcm_intelligence.prompts_loader import regulator_system_prompt
from rcm_intelligence.settings import external_pdf_catalog, full_name, load_config, log_root

spark.conf.set('spark.sql.session.timeZone', 'Asia/Singapore')
logger = get_logger()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Read the parameters and the config
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Load the table names, the model service and the agent limits from the config, set up MLflow tracing, and start the run log. |
# MAGIC | **Input** | Widgets `max_documents`, `document_id`, `env`, `config_dir`; `common_config_<env>.yaml` |
# MAGIC | **Output** | Table names, `MODEL`, agent limits, `RUN_ID`, `run_log`, the MLflow experiment |

# COMMAND ----------

dbutils.widgets.text('max_documents', '100')
dbutils.widgets.text('document_id', '')        # empty: every document version with no analysis yet
dbutils.widgets.text('env', '')                # empty: use the cluster environment variable RCM_ENV
dbutils.widgets.text('config_dir', '')         # empty: use the cluster environment variable RCM_CONFIG_DIR

MAX_DOCUMENTS = int(dbutils.widgets.get('max_documents').strip() or 100)
DOCUMENT_ID = dbutils.widgets.get('document_id').strip()
CFG = load_config(dbutils.widgets.get('env').strip() or None, dbutils.widgets.get('config_dir').strip() or None)

CHUNK_TABLE = full_name(CFG, 'tables', 'doc_chunk')
CONTENT_TABLE = full_name(CFG, 'tables', 'doc_content')
ANALYSIS_TABLE = full_name(CFG, 'tables', 'doc_analysis')
DIFF_TABLE = full_name(CFG, 'tables', 'doc_diff')
TASK_TABLE = full_name(CFG, 'tables', 'agent_task')
PDF_CATALOG = external_pdf_catalog(CFG)
MODEL = CFG['model_service']
AGENT = CFG['agent']
AGENT_NAME = AGENT['regulator_agent_name']
MAX_REWORK = int(AGENT['max_rework'])
MAX_OUTPUT_TOKENS = int(AGENT['max_output_tokens'])
NAME_OF_OFFICE = CFG['name_of_office']

RUN_ID = new_run_id('analysis')
RUN_STARTED = now_sgt()
set_log_context(stage='ANALYSIS', regulator_id='ALL', run_id=RUN_ID)
run_log = RunLog(log_root(CFG, 'analysis'), 'ANALYSIS', 'ALL', RUN_ID).start()

# Record every model call (prompt, answer, tokens, time) in MLflow for audit and debugging
WorkspaceClient().workspace.mkdirs(CFG['mlflow_experiment_root'])
mlflow.set_experiment(f"{CFG['mlflow_experiment_root']}/{AGENT_NAME}")
mlflow.openai.autolog()
logger.info('Environment %s | model %s | agent %s %s | release %s',
            CFG['_env'], MODEL, AGENT_NAME, agent_version(), release_tag())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Build the work list and queue the tasks
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Pick the document versions in Gold that have no analysis record yet (or the one given `document_id`), and record a QUEUED task for each in `rcm_agent_task`. Versions left in ERROR by an earlier run have no record either, so they are picked up again. |
# MAGIC | **Input** | `rcm_doc_chunk`, `rcm_doc_analysis` |
# MAGIC | **Output** | `todo`, QUEUED rows in `rcm_agent_task` |

# COMMAND ----------

where = 'c.document_id = :document_id' if DOCUMENT_ID else 'a.document_id IS NULL'
todo = spark.sql(f'''
    SELECT DISTINCT c.document_id, c.version, c.regulator_id, c.file_name, c.source_url
    FROM {CHUNK_TABLE} c
    LEFT JOIN {ANALYSIS_TABLE} a ON a.document_id = c.document_id AND a.version = c.version
    WHERE {where}
    ORDER BY c.document_id
    LIMIT {MAX_DOCUMENTS}
''', args={'document_id': DOCUMENT_ID} if DOCUMENT_ID else {}).collect()
logger.info('%s document versions to analyse', len(todo))

SET_TASK = f'''
    MERGE INTO {TASK_TABLE} t
    USING (SELECT :document_id AS document_id, :version AS version) s
    ON t.document_id = s.document_id AND t.version = s.version
    WHEN MATCHED THEN UPDATE SET state = :state, rework_count = :rework_count, last_step_output = :note,
        failure_reason = :reason, run_id = :run_id, trace_id = :trace_id, agent_version = :agent_version,
        updated_at_sgt = :now
    WHEN NOT MATCHED THEN INSERT (document_id, version, regulator_id, agent_name, agent_version, state,
        rework_count, last_step_output, failure_reason, run_id, trace_id, updated_at_sgt)
    VALUES (:document_id, :version, :regulator_id, :agent_name, :agent_version, :state,
        :rework_count, :note, :reason, :run_id, :trace_id, :now)
'''


def set_task(doc, state, note=None, reason=None, rework_count=0, trace_id=None):
    '''Record the state of one task in rcm_agent_task.'''
    run_sql_with_retry(spark, SET_TASK, {
        'document_id': doc['document_id'], 'version': doc['version'], 'regulator_id': doc['regulator_id'],
        'agent_name': AGENT_NAME, 'agent_version': agent_version(), 'state': state,
        'rework_count': rework_count, 'note': note, 'reason': (reason or '')[:4000] or None,
        'run_id': RUN_ID, 'trace_id': trace_id, 'now': now_sgt()}, what='agent task')


for doc in todo:
    set_task(doc, QUEUED, note='Queued for analysis')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Define the agent steps
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Read the chunks and the document details, and for an update the previous record and the exact differences. Ask the model, check the answer in code, and send it back for rework with the list of problems, up to `max_rework` times. Each analysis is one MLflow trace. |
# MAGIC | **Input** | One document version |
# MAGIC | **Output** | Function `analyse` returning the record, the problems, the details, the previous record, the differences, the rework count and the prompt version |

# COMMAND ----------

def read_chunks(document_id, version):
    '''The chunks of one document version, in order.'''
    rows = spark.sql(f'''
        SELECT chunk_id, section_title, page_from, chunk_text FROM {CHUNK_TABLE}
        WHERE document_id = :doc AND version = :ver ORDER BY chunk_index
    ''', args={'doc': document_id, 'ver': version}).collect()
    return [r.asDict() for r in rows]


def read_details(document_id):
    '''Document details from the scraper catalog: title, date, category, link, when it was collected.'''
    row = spark.sql(f'''
        SELECT item_title, item_date, item_date_label, category_id, source_url, last_changed_sgt, link_text, file_name
        FROM {PDF_CATALOG} WHERE document_id = :doc
    ''', args={'doc': document_id}).first()
    return row.asDict() if row else {}


def read_previous_record(document_id, version):
    '''The validated record of the previous version, or an empty dictionary for a new document.'''
    row = spark.sql(f'''
        SELECT version, law_or_regulation, date_introduced, effective_date, summary_key_items
        FROM {ANALYSIS_TABLE}
        WHERE document_id = :doc AND version < :ver AND analysis_status = 'VALIDATED'
        ORDER BY version DESC LIMIT 1
    ''', args={'doc': document_id, 'ver': version}).first()
    return row.asDict() if row else {}


def read_full_text(document_id, version):
    '''Full text of one version from Silver, or None if that version was never parsed.'''
    row = spark.sql(f'''
        SELECT content_text FROM {CONTENT_TABLE}
        WHERE document_id = :doc AND version = :ver AND parse_status = 'PARSED'
    ''', args={'doc': document_id, 'ver': version}).first()
    return row['content_text'] if row else None


def model_client():
    '''Model client signed in with the job identity (no stored secrets). Created per document,
    so a long batch never uses an expired token.'''
    w = WorkspaceClient()
    token = w.config.authenticate()['Authorization'].split(' ')[1]
    return OpenAI(base_url=f'{w.config.host}/ai-gateway/mlflow/v1', api_key=token)


@mlflow.trace(name='regulator_analysis', span_type='AGENT')
def analyse(doc):
    chunks = read_chunks(doc['document_id'], doc['version'])
    details = read_details(doc['document_id'])
    valid_ids = {c['chunk_id'] for c in chunks}

    previous, differences = {}, []
    if doc['version'] > 1:
        previous = read_previous_record(doc['document_id'], doc['version'])
        old_text = read_full_text(doc['document_id'], previous.get('version', doc['version'] - 1))
        new_text = read_full_text(doc['document_id'], doc['version'])
        if old_text and new_text:
            differences = find_differences(old_text, new_text)
        logger.info('  update of version %s: %s differences found', previous.get('version', '?'), len(differences))

    system_prompt, prompt_version = regulator_system_prompt(doc['regulator_id'])
    messages = [{'role': 'system', 'content': system_prompt},
                {'role': 'user', 'content': build_user_message(details, chunks, previous, differences,
                                                               AGENT['max_diff_passages'], AGENT['max_diff_chars'])}]
    client = model_client()
    record, problems, attempt = {}, ['no answer yet'], 0
    for attempt in range(MAX_REWORK + 1):
        response = client.chat.completions.create(model=MODEL, messages=messages,
                                                  max_tokens=MAX_OUTPUT_TOKENS, temperature=0)
        answer = response.choices[0].message.content or ''
        try:
            record = parse_json(answer)
            problems = check_record(record, valid_ids, is_update=bool(previous or differences))
        except (ValueError, json.JSONDecodeError) as error:
            problems = [f'answer was not valid JSON: {error}']
        logger.info('  attempt %s: %s', attempt + 1, 'passed' if not problems else '; '.join(problems))
        if not problems:
            break
        messages += [{'role': 'assistant', 'content': answer},
                     {'role': 'user', 'content': 'Your answer has these problems:\n- ' + '\n- '.join(problems)
                                                 + '\nReturn the corrected JSON object only.'}]
    return {'record': record, 'problems': problems, 'details': details, 'previous': previous,
            'differences': differences, 'rework_count': attempt, 'prompt_version': prompt_version,
            'trace_id': current_trace_id()}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Define how one result is saved
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Build the CPDAP CPL row (fields 1, 2 and 4 and the version fields from code, the rest from the model, plus the traceability fields) and save it with its exact differences. Each document is saved on its own, so a crash never loses finished work. |
# MAGIC | **Input** | One document and its analysis result |
# MAGIC | **Output** | Functions `build_row` and `save_result` |

# COMMAND ----------

ANALYSIS_SCHEMA = spark.table(ANALYSIS_TABLE).schema
DIFF_SCHEMA = spark.table(DIFF_TABLE).schema


def build_row(doc, result):
    record, details, previous = result['record'], result['details'], result['previous']
    collected = details.get('last_changed_sgt')
    return {
        'document_id': doc['document_id'], 'version': doc['version'], 'regulator_id': doc['regulator_id'],
        'category_id': details.get('category_id'),
        'name_of_office': NAME_OF_OFFICE,
        'assessment_date': datetime.now(SGT).date(),
        'law_or_regulation': record.get('law_or_regulation'),
        'info_collected_date': collected.date() if collected else None,
        'source_url': details.get('source_url') or doc['source_url'],
        'published_date': details.get('item_date'),
        'date_introduced': record.get('date_introduced'),
        'effective_date': record.get('effective_date'),
        'intention_background': record.get('intention_background'),
        'summary_key_items': record.get('summary_key_items'),
        'areas_of_impact': record.get('areas_of_impact'),
        'proposed_action_timeline': record.get('proposed_action_timeline'),
        'compliance_risk_category': record.get('compliance_risk_category'),
        'importance': record.get('importance'),
        'change_type': 'NEW' if doc['version'] == 1 else 'UPDATED',
        'event_type': event_type_of(details),
        'previous_version': previous.get('version') or (doc['version'] - 1 if doc['version'] > 1 else None),
        'what_changed': record.get('what_changed') or None,
        'relevance': record.get('relevance'),
        'relevance_reason': record.get('relevance_reason'),
        'citations': [str(c) for c in (record.get('citations') or [])],
        'analysis_status': VALIDATED if not result['problems'] else FLAGGED,
        'check_problems': '; '.join(result['problems']) if result['problems'] else None,
        'agent_name': AGENT_NAME,
        'agent_version': agent_version(),
        'prompt_version': result['prompt_version'],
        'model_endpoint': MODEL,
        'release_tag': release_tag(),
        'trace_id': result['trace_id'],
        'created_at_sgt': now_sgt(),
    }


def save_result(doc, row, result):
    '''Save the record (replacing an earlier one for the same version) and its differences.'''
    spark.createDataFrame([tuple(row[f.name] for f in ANALYSIS_SCHEMA.fields)], ANALYSIS_SCHEMA) \
         .createOrReplaceTempView('new_analysis')
    run_sql_with_retry(spark, f'''MERGE INTO {ANALYSIS_TABLE} t USING new_analysis s
                                  ON t.document_id = s.document_id AND t.version = s.version
                                  WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *''', what='analysis')
    run_sql_with_retry(spark, f'DELETE FROM {DIFF_TABLE} WHERE document_id = :doc AND version = :ver',
                       {'doc': doc['document_id'], 'ver': doc['version']}, what='diff delete')
    differences = result['differences']
    if differences:
        previous_version = row['previous_version']
        rows = [(doc['document_id'], doc['version'], previous_version, n, d['change_kind'], d['old_text'],
                 d['new_text'], now_sgt()) for n, d in enumerate(differences, start=1)]
        spark.createDataFrame(rows, DIFF_SCHEMA).createOrReplaceTempView('new_diff')
        run_sql_with_retry(spark, f'INSERT INTO {DIFF_TABLE} SELECT * FROM new_diff', what='diff insert')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6: Analyse and save each document
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Run the agent for each queued task, save the result, and move the task to VALIDATED, FLAGGED or ERROR. A technical failure on one document does not stop the others. |
# MAGIC | **Input** | `todo` |
# MAGIC | **Output** | Rows in `rcm_doc_analysis` and `rcm_doc_diff`, task states, counts per outcome |

# COMMAND ----------

counts = {VALIDATED: 0, FLAGGED: 0, ERROR: 0}
for doc in todo:
    set_log_context(regulator_id=doc['regulator_id'])
    logger.info('Analysing %s v%s', doc['file_name'], doc['version'])
    try:
        result = analyse(doc)
        set_task(doc, ANALYSED, note='Model answered, code checks done', rework_count=result['rework_count'],
                 trace_id=result['trace_id'])
        row = build_row(doc, result)
        save_result(doc, row, result)
        state = row['analysis_status']
        set_task(doc, state, note=f'Saved as {state}', reason=row['check_problems'],
                 rework_count=result['rework_count'], trace_id=result['trace_id'])
        counts[state] += 1
    except Exception as error:
        counts[ERROR] += 1
        logger.exception('ERROR on %s v%s', doc['file_name'], doc['version'])
        try:
            set_task(doc, ERROR, note='Technical failure', reason=f'{type(error).__name__}: {error}')
        except Exception as task_error:
            logger.error('Could not record the task error: %s', task_error)

set_log_context(regulator_id='ALL')
logger.info('%s analysed: %s VALIDATED, %s FLAGGED, %s ERROR', len(todo), counts[VALIDATED], counts[FLAGGED], counts[ERROR])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7: Show the results, save the run log and set the final status
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Show the records written by this run for a quick review, copy the run log to the logs volume, and fail the task if any document ended in ERROR (it is retried by the next run). |
# MAGIC | **Input** | `counts`, `rcm_doc_analysis`, `run_log` |
# MAGIC | **Output** | A table in the notebook output, log file `vol_logs/analysis/ALL/<day>/<run_id>.log`, task result |

# COMMAND ----------

if todo:
    display(spark.sql(f'''
        SELECT law_or_regulation, version, change_type, event_type, analysis_status, check_problems,
               compliance_risk_category, importance, relevance, prompt_version, release_tag
        FROM {ANALYSIS_TABLE}
        WHERE created_at_sgt >= :since
        ORDER BY created_at_sgt DESC
    ''', args={'since': RUN_STARTED}))

run_log.publish()
if counts[ERROR]:
    raise RuntimeError(f'{counts[ERROR]} documents ended in ERROR. See {TASK_TABLE} where state = ERROR.')
dbutils.notebook.exit(f"{RUN_ID} validated={counts[VALIDATED]} flagged={counts[FLAGGED]} error={counts[ERROR]}")
