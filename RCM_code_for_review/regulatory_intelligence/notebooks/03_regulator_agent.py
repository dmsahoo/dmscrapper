# Databricks notebook source
# MAGIC %md
# MAGIC # Regulator Agent (C6, Internal ADB workspace)
# MAGIC
# MAGIC For each document that has no analysis yet:
# MAGIC 1. Reads its chunks from `rcm_doc_chunk` (Gold) and its details from `pdf_catalog`.
# MAGIC 2. Sends them to the model service with the system prompt (base prompt plus regulator module).
# MAGIC 3. Checks the answer in code: required fields, allowed values, date format, citations. Up to 2 reworks.
# MAGIC 4. Saves one CPDAP CPL record to `rcm_doc_analysis` (VALIDATED or FLAGGED).
# MAGIC
# MAGIC Versions: version 1 is a NEW document. A higher version is an UPDATED document (same link, new content).
# MAGIC For an update, code first compares the full text of the old and new version from Silver (`rcm_doc_content`),
# MAGIC saves the exact differences in `rcm_doc_diff`, and the agent explains them in the field `what_changed`.
# MAGIC
# MAGIC Parameters: `env` (default dev), `document_id` (optional, one specific document) and `max_documents` (default 1).
# MAGIC Tables, model, experiment and limits come from config/<env>.yaml through common/config.py.
# MAGIC Set `max_documents` to 100 to run as the batch job. Compute: serverless.

# COMMAND ----------

# Install the libraries for the model client and tracing (on serverless, once per session)
%pip install --quiet databricks-openai "mlflow>=3"
dbutils.library.restartPython()

# COMMAND ----------

import difflib
import json
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import mlflow
from databricks.sdk import WorkspaceClient
from openai import OpenAI

spark.conf.set('spark.sql.session.timeZone', 'Asia/Singapore')
SGT = ZoneInfo('Asia/Singapore')

# COMMAND ----------

# Load the settings for the chosen environment from config/<env>.yaml, so nothing is hardcoded here.
# The env widget (or job parameter) picks the file: dev, uat or prod.
import os
import sys
_repo_root = os.path.dirname(os.getcwd()) if os.path.basename(os.getcwd()) == 'notebooks' else os.getcwd()
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)
from common.config import load_config, table, external_table, volume_path

dbutils.widgets.text('env', 'dev')
CFG = load_config(dbutils.widgets.get('env'))
print('Settings from', CFG['_path'])

# Settings: tables, model and limits, all from the config file
CHUNK_TABLE = table(CFG, 'rcm_doc_chunk')
CONTENT_TABLE = table(CFG, 'rcm_doc_content')                            # Silver: full text of every version
ANALYSIS_TABLE = table(CFG, 'rcm_doc_analysis')
DIFF_TABLE = table(CFG, 'rcm_doc_diff')                                  # exact differences between versions
PDF_CATALOG = external_table(CFG, 'pdf_catalog')                         # external, read only
MODEL = CFG['model_service']                                             # model service from the config
PROMPT_VERSION = 'v0.2'                                                  # v0.2: versions and what_changed
MAX_REWORK = CFG['agent']['max_rework']                                  # extra attempts when the checks fail
MAX_OUTPUT_TOKENS = CFG['agent']['max_output_tokens']                    # room for reasoning plus the JSON answer
NAME_OF_OFFICE = CFG['name_of_office']
MAX_DIFF_PASSAGES = CFG['agent']['max_diff_passages']                    # changed passages given to the agent
MAX_DIFF_CHARS = CFG['agent']['max_diff_chars']                          # keeps the prompt a sensible size

# Allowed values, from the CPDAP CPL field list
RISK_CATEGORIES = {'AML', 'CFT', 'Sanctions', 'Data Protection', 'Conduct',
                   'Operational Risk', 'Cybersecurity', 'Other'}
IMPORTANCE = {'High', 'Medium', 'Low', 'Pending Review'}
RELEVANCE = {'RELEVANT', 'NOT_RELEVANT', 'EXCLUDED'}
TEXT_FIELDS = ['law_or_regulation', 'intention_background', 'summary_key_items',
               'areas_of_impact', 'proposed_action_timeline', 'relevance_reason']
DATE_PATTERN = re.compile(r'^\d{1,2} [A-Z][a-z]{2} \d{4}$')             # for example 05 Mar 2026

# Parameters
dbutils.widgets.text('document_id', '')                                  # empty means: pick documents with no analysis
dbutils.widgets.text('max_documents', '1')
DOCUMENT_ID = dbutils.widgets.get('document_id').strip()
MAX_DOCUMENTS = int(dbutils.widgets.get('max_documents').strip() or 1)

# Record every model call (prompt, answer, tokens, time) in MLflow for audit and debugging
WorkspaceClient().workspace.mkdirs(CFG['mlflow_experiment_root'])        # the experiment folder must exist
mlflow.set_experiment(f"{CFG['mlflow_experiment_root']}/rcm_regulator_analysis")
mlflow.openai.autolog()

# COMMAND ----------

# Load the system prompt: the shared base prompt, followed by the regulator module

def load_system_prompt(regulator_id):
    with open(os.path.join(_repo_root, 'prompts', 'base_prompt.md')) as f:
        base = f.read()
    with open(os.path.join(_repo_root, 'prompts', 'regulators', f'{regulator_id.lower()}.md')) as f:
        module = f.read()
    return base + '\n\n' + module


# Create a model client that signs in with the notebook's own identity (no stored secrets).
# Called again for each document, so a long batch never uses an expired token.
def model_client():
    w = WorkspaceClient()
    token = w.config.authenticate()['Authorization'].split(' ')[1]
    return OpenAI(base_url=f'{w.config.host}/ai-gateway/mlflow/v1', api_key=token)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Work list

# COMMAND ----------

# Pick the documents to analyse: one given document, or documents with no analysis record yet
if DOCUMENT_ID:
    where = f"c.document_id = '{DOCUMENT_ID}'"
else:
    where = 'a.document_id IS NULL'

todo = spark.sql(f'''
    SELECT DISTINCT c.document_id, c.version, c.regulator_id, c.file_name, c.source_url
    FROM {CHUNK_TABLE} c
    LEFT JOIN {ANALYSIS_TABLE} a ON a.document_id = c.document_id AND a.version = c.version
    WHERE {where}
    ORDER BY c.document_id
    LIMIT {MAX_DOCUMENTS}
''').collect()
print(f'{len(todo)} documents to analyse')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Agent steps

# COMMAND ----------

# Read the chunks of one document version, in order
def read_chunks(document_id, version):
    rows = spark.sql(f'''
        SELECT chunk_id, section_title, page_number, chunk_text
        FROM {CHUNK_TABLE}
        WHERE document_id = :doc AND version = :ver
        ORDER BY chunk_index
    ''', args={'doc': document_id, 'ver': version}).collect()
    return [r.asDict() for r in rows]


# Read the document details the agent needs from pdf_catalog (title, date, URL, when it was collected)
def read_details(document_id):
    row = spark.sql(f'''
        SELECT item_title, item_date, item_date_label, category_id, source_url, last_changed_sgt,
               link_text, file_name
        FROM {PDF_CATALOG}
        WHERE document_id = :doc
    ''', args={'doc': document_id}).first()
    return row.asDict() if row else {}


# Work out the kind of change from the file and the website category.
# A file whose link text or name contains the word amendment is an Amendment.
# Otherwise the category name is used, for example 1_NOTICES becomes Notices.
# This works for every regulator, because it only uses names from the regulator's own config.
def event_type_of(details):
    file_words = f"{details.get('link_text') or ''} {details.get('file_name') or ''}".lower()
    if 'amendment' in file_words:
        return 'Amendment'
    category = str(details.get('category_id') or '')
    return category.split('_', 1)[-1].replace('_', ' ').title() or 'Other'


# Read the record of the previous version of the same document.
# Version 1 has no previous version, so this returns an empty dictionary for new documents.
def read_previous_record(document_id, version):
    row = spark.sql(f'''
        SELECT version, law_or_regulation, date_introduced, effective_date, summary_key_items
        FROM {ANALYSIS_TABLE}
        WHERE document_id = :doc AND version < :ver AND analysis_status = 'VALIDATED'
        ORDER BY version DESC
        LIMIT 1
    ''', args={'doc': document_id, 'ver': version}).first()
    return row.asDict() if row else {}


# Read the full text of one version from Silver (None if that version was never parsed)
def read_full_text(document_id, version):
    row = spark.sql(f'''
        SELECT content_text FROM {CONTENT_TABLE}
        WHERE document_id = :doc AND version = :ver AND parse_status = 'PARSED'
    ''', args={'doc': document_id, 'ver': version}).first()
    return row['content_text'] if row else None


# Split a text into paragraphs and tidy the spaces, so that a changed line break is not seen as a change
def paragraphs_of(text):
    parts = re.split(r'\n\s*\n', text or '')
    return [re.sub(r'\s+', ' ', p).strip() for p in parts if p.strip()]


# Compare the old and new full text, paragraph by paragraph, and list every difference.
# This is plain code (Python difflib), not AI, so it is exact and gives the same result every time.
# Each difference is ADDED (only in the new version), REMOVED (only in the old) or CHANGED (old became new).
def find_differences(old_text, new_text):
    old_paras, new_paras = paragraphs_of(old_text), paragraphs_of(new_text)
    matcher = difflib.SequenceMatcher(a=old_paras, b=new_paras, autojunk=False)
    differences = []
    for kind, a1, a2, b1, b2 in matcher.get_opcodes():
        if kind == 'equal':
            continue                                                    # same paragraphs, nothing to report
        old_part = '\n\n'.join(old_paras[a1:a2]) or None
        new_part = '\n\n'.join(new_paras[b1:b2]) or None
        change_kind = {'insert': 'ADDED', 'delete': 'REMOVED', 'replace': 'CHANGED'}[kind]
        differences.append({'change_kind': change_kind, 'old_text': old_part, 'new_text': new_part})
    return differences


# Turn the differences into short text for the agent, within the size limits above
def differences_for_prompt(differences):
    lines, used = [], 0
    for n, d in enumerate(differences[:MAX_DIFF_PASSAGES], start=1):
        piece = (f"Change {n} ({d['change_kind']}):\n"
                 + (f"  OLD: {d['old_text']}\n" if d['old_text'] else '')
                 + (f"  NEW: {d['new_text']}\n" if d['new_text'] else ''))
        if used + len(piece) > MAX_DIFF_CHARS:
            lines.append(f'... {len(differences) - n + 1} more changes not shown')
            break
        lines.append(piece)
        used += len(piece)
    return '\n'.join(lines)


# Build the user message: document details, then (for an update) the previous version and the exact
# differences, then every chunk of the current version with its chunk_id
def build_user_message(details, chunks, previous=None, differences=None):
    header = (f"Document title on the website: {details.get('item_title')}\n"
              f"Date on the website: {details.get('item_date')} ({details.get('item_date_label')})\n"
              f"Category: {details.get('category_id')}\n"
              f"Source URL: {details.get('source_url')}\n\n")

    if previous or differences:
        # Updated document: show the previous record (if there is one) and what code found to be different
        header += "This is an UPDATED version of a document analysed before.\n"
        if previous:
            header += (f"Previous version {previous['version']}:\n"
                       f"  Title: {previous['law_or_regulation']}\n"
                       f"  Date introduced: {previous['date_introduced']}\n"
                       f"  Effective date: {previous['effective_date']}\n"
                       f"  Summary of key items: {previous['summary_key_items']}\n\n")
        if differences:
            header += ("Exact differences between the previous and this version, found by comparing the full texts:\n"
                       + differences_for_prompt(differences) + "\n\n")
        header += ("Also return the field what_changed: 2 to 4 short sentences on what is different in this "
                   "version, based on the differences above and the chunks below. Mention changed figures, "
                   "dates and obligations. Ignore changes that are only formatting.\n\n")
    else:
        # New document: nothing to compare
        header += "This is a NEW document. Return the field what_changed as an empty string.\n\n"

    body = '\n\n'.join(f"[{c['chunk_id']}] (section: {c['section_title'] or '-'}, page: {c['page_number'] or '-'})\n"
                       f"{c['chunk_text']}" for c in chunks)
    return header + "Document chunks:\n" + body


# Take the JSON object out of the model's answer (ignores any text before or after it)
def parse_json(text):
    start, end = (text or '').find('{'), (text or '').rfind('}')
    if start < 0 or end <= start:
        raise ValueError('No JSON object found in the answer')
    return json.loads(text[start:end + 1])


# Check the answer in code. Returns a list of problems; an empty list means the record passes.
def check_record(record, valid_chunk_ids, is_update=False):
    problems = []
    for field in TEXT_FIELDS:
        if not str(record.get(field) or '').strip():
            problems.append(f'{field} is empty')
    if record.get('compliance_risk_category') not in RISK_CATEGORIES:
        problems.append(f"compliance_risk_category must be one of {sorted(RISK_CATEGORIES)}")
    if record.get('importance') not in IMPORTANCE:
        problems.append(f"importance must be one of {sorted(IMPORTANCE)}")
    if record.get('relevance') not in RELEVANCE:
        problems.append(f"relevance must be one of {sorted(RELEVANCE)}")
    if not (record.get('date_introduced') == 'N.A.' or DATE_PATTERN.match(str(record.get('date_introduced')))):
        problems.append('date_introduced must be DD Mon YYYY or N.A.')
    if not (record.get('effective_date') in ('N.A.', 'Not Stated') or DATE_PATTERN.match(str(record.get('effective_date')))):
        problems.append('effective_date must be DD Mon YYYY, Not Stated or N.A.')
    citations = record.get('citations') or []
    if not isinstance(citations, list) or not citations:
        problems.append('citations must list at least one chunk_id')
    else:
        unknown = [c for c in citations if c not in valid_chunk_ids]
        if unknown:
            problems.append(f'these citations are not chunk_id values of this document: {unknown[:5]}')
    # An updated document must say what changed. A new document does not need it.
    if is_update and not str(record.get('what_changed') or '').strip():
        problems.append('what_changed is empty, but this is an updated version')
    return problems


# Run the agent for one document: ask, check, and rework up to MAX_REWORK times
def analyse(doc):
    chunks = read_chunks(doc['document_id'], doc['version'])
    details = read_details(doc['document_id'])
    valid_ids = {c['chunk_id'] for c in chunks}

    # For an updated document: read the previous record, and compare the old and new full text from Silver
    previous, differences = {}, []
    if doc['version'] > 1:
        previous = read_previous_record(doc['document_id'], doc['version'])
        old_text = read_full_text(doc['document_id'], previous.get('version', doc['version'] - 1))
        new_text = read_full_text(doc['document_id'], doc['version'])
        if old_text and new_text:
            differences = find_differences(old_text, new_text)
        print(f"  update of version {previous.get('version', '?')}: {len(differences)} differences found")

    messages = [{'role': 'system', 'content': load_system_prompt(doc['regulator_id'])},
                {'role': 'user', 'content': build_user_message(details, chunks, previous, differences)}]
    client = model_client()
    record, problems = {}, ['no answer yet']

    for attempt in range(MAX_REWORK + 1):
        # Ask the model
        response = client.chat.completions.create(model=MODEL, messages=messages,
                                                  max_tokens=MAX_OUTPUT_TOKENS, temperature=0)
        answer = response.choices[0].message.content or ''
        # Read and check the answer
        try:
            record = parse_json(answer)
            problems = check_record(record, valid_ids, is_update=bool(previous or differences))
        except (ValueError, json.JSONDecodeError) as e:
            problems = [f'answer was not valid JSON: {e}']
        print(f"  attempt {attempt + 1}: {'passed' if not problems else '; '.join(problems)}")
        if not problems:
            break
        # Rework: send the problems back to the model and ask for a corrected JSON
        messages += [{'role': 'assistant', 'content': answer},
                     {'role': 'user', 'content': 'Your answer has these problems:\n- ' + '\n- '.join(problems)
                                                 + '\nReturn the corrected JSON object only.'}]
    return record, problems, details, previous, differences

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run and save

# COMMAND ----------

# Analyse each document and build one CPDAP CPL row per document
rows, diff_rows = [], []
for doc in todo:
    print(f"Analysing {doc['file_name']} v{doc['version']}")
    record, problems, details, previous, differences = analyse(doc)

    # Keep the exact differences, so the app and the Q and A Agent can show the old and new wording
    for n, d in enumerate(differences, start=1):
        diff_rows.append((doc['document_id'], doc['version'], previous.get('version') or doc['version'] - 1, n, d['change_kind'],
                          d['old_text'], d['new_text'], datetime.now(SGT).replace(tzinfo=None)))

    collected = details.get('last_changed_sgt')
    rows.append({
        'document_id': doc['document_id'], 'version': doc['version'], 'regulator_id': doc['regulator_id'],
        # Fields 1, 2 and 4 come from code, not from the model
        'name_of_office': NAME_OF_OFFICE,
        'assessment_date': datetime.now(SGT).date(),
        'info_collected_date': collected.date() if collected else None,
        'source_url': details.get('source_url') or doc['source_url'],
        'published_date': details.get('item_date'),                     # the regulator's date for the item, from pdf_catalog
        # Version information, worked out in code (no AI needed for these)
        'change_type': 'NEW' if doc['version'] == 1 else 'UPDATED',      # first version, or a newer copy at the same link
        'event_type': event_type_of(details),                            # Notices, Amendment, Consultations, ...
        'previous_version': previous.get('version') or (doc['version'] - 1 if doc['version'] > 1 else None),
        'what_changed': record.get('what_changed') or None,              # written by the agent, only for updates
        # Fields 3 and 5 to 12 come from the model
        'law_or_regulation': record.get('law_or_regulation'),
        'date_introduced': record.get('date_introduced'),
        'effective_date': record.get('effective_date'),
        'intention_background': record.get('intention_background'),
        'summary_key_items': record.get('summary_key_items'),
        'areas_of_impact': record.get('areas_of_impact'),
        'proposed_action_timeline': record.get('proposed_action_timeline'),
        'compliance_risk_category': record.get('compliance_risk_category'),
        'importance': record.get('importance'),
        # Supporting fields
        'relevance': record.get('relevance'),
        'relevance_reason': record.get('relevance_reason'),
        'citations': [str(c) for c in (record.get('citations') or [])],
        'analysis_status': 'VALIDATED' if not problems else 'FLAGGED',
        'check_problems': '; '.join(problems) if problems else None,
        'prompt_version': PROMPT_VERSION,
        'model_endpoint': MODEL,
        'created_at_sgt': datetime.now(SGT).replace(tzinfo=None),
    })

# COMMAND ----------

# Save the rows to rcm_doc_analysis: replace the record if this document version was analysed before
if rows:
    schema = spark.table(ANALYSIS_TABLE).schema
    spark.createDataFrame([tuple(r[f.name] for f in schema.fields) for r in rows], schema) \
         .createOrReplaceTempView('new_analysis')
    spark.sql(f'''
        MERGE INTO {ANALYSIS_TABLE} t USING new_analysis s
        ON t.document_id = s.document_id AND t.version = s.version
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    ''')

# Save the exact differences: replace any earlier result for the same document version (rerun safe)
if diff_rows:
    spark.createDataFrame(diff_rows, spark.table(DIFF_TABLE).schema).createOrReplaceTempView('new_diff')
    spark.sql(f'''
        MERGE INTO {DIFF_TABLE} t
        USING (SELECT DISTINCT document_id, version FROM new_diff) k
        ON t.document_id = k.document_id AND t.version = k.version
        WHEN MATCHED THEN DELETE
    ''')
    spark.sql(f'INSERT INTO {DIFF_TABLE} SELECT * FROM new_diff')

validated = sum(r['analysis_status'] == 'VALIDATED' for r in rows)
print(f'{len(rows)} analysed: {validated} VALIDATED, {len(rows) - validated} FLAGGED')

# COMMAND ----------

# Show what was saved, for a quick review
display(spark.sql(f'''
    SELECT law_or_regulation, version, change_type, event_type, what_changed, date_introduced, effective_date,
           compliance_risk_category, importance, relevance, analysis_status, check_problems, summary_key_items
    FROM {ANALYSIS_TABLE}
    ORDER BY created_at_sgt DESC
    LIMIT {max(len(rows), 1)}
'''))
