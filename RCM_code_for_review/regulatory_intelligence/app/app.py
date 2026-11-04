"""
Regulatory Scanner (C11): Streamlit app on Databricks Apps.

Tab 1, Latest changes: the CPDAP CPL results from the Regulator Agent, with filters,
a table, and the full summary of the selected item. Read only.
One row per document (its newest version). Each row is labelled New (first version) or
Updated (a newer copy at the same link). For an update, the detail panel shows what changed,
the exact wording changes, and the previous versions.
Tab 2, AI Assistant: questions answered by the Q and A Agent (agents/qa_agent.py), with
guardrails G1 and G2 (guardrails/guardrails.py). Answers come only from the regulatory documents.

The app is deployed from the repository root (app.yaml there), so it can use the shared
agents, guardrails, prompts and config folders.
"""
import json
import os
import sys
from datetime import date

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

st.set_page_config(page_title='Regulatory Scanner', layout='wide')

# Make the repository folders importable (app.py lives in the app folder)
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT)
from agents.qa_agent import QAAgent                                    # noqa: E402
from common.config import load_config, table                           # noqa: E402
from guardrails.guardrails import Guardrails                           # noqa: E402

# Settings: names come from config/<env>.yaml, where env is RCM_ENV in app.yaml (nothing is hardcoded).
# The SQL warehouse comes from the app's sql-warehouse resource, also set in app.yaml.
CFG = load_config(os.getenv('RCM_ENV'))
WAREHOUSE_ID = os.getenv('DATABRICKS_WAREHOUSE_ID')
VIEW = table(CFG, 'v_app_updates')                                     # one row per document, newest version
HISTORY_VIEW = table(CFG, 'v_app_history')                             # every version, for Previous versions
DIFF_TABLE = table(CFG, 'rcm_doc_diff')                                # exact wording changes between versions
MODEL = CFG['model_service']
# Agent traces go to the same MLflow experiment as the notebook, taken from the config
os.environ.setdefault('MLFLOW_EXPERIMENT_NAME', f"{CFG['mlflow_experiment_root']}/rcm_qa_agent")

# Example questions shown next to the chat (change them to match the regulators that are loaded)
EXAMPLE_QUESTIONS = ['What changed for MAS in October 2026 ?',
                     'What does MAS Notice 656 say about limits on exposures to a single counterparty group ?',
                     'What is the effective date of the MAS Notice FHC-N652 amendment ?']

# How the agent's relevance values are shown on screen
RELEVANCE_LABEL = {'RELEVANT': 'Potentially Relevant', 'NOT_RELEVANT': 'Not Relevant',
                   'EXCLUDED': 'Not Relevant'}

# Cell colours for importance and relevance, like the design
COLOURS = {'High': '#FDE8E8', 'Medium': '#FEF3C7', 'Low': '#E8F1FD', 'Pending Review': '#F3F4F6',
           'Potentially Relevant': '#E7F6EC', 'Not Relevant': '#F3F4F6',
           'New': '#E7F6EC'}                                             # Updated rows get the amber colour below
UPDATED_COLOUR = '#FEF3C7'

# How each kind of difference is shown in the Exact wording changes list
CHANGE_KIND_LABEL = {'ADDED': 'Added', 'REMOVED': 'Removed', 'CHANGED': 'Changed'}


# Read the view through the SQL warehouse, signing in as the app's own identity.
# Results are kept for 5 minutes so the page stays fast.
@st.cache_data(ttl=300)
def load_updates():
    cfg = Config()
    with sql.connect(server_hostname=cfg.host,
                     http_path=f'/sql/1.0/warehouses/{WAREHOUSE_ID}',
                     credentials_provider=lambda: cfg.authenticate) as conn:
        with conn.cursor() as cur:
            cur.execute(f'SELECT * FROM {VIEW} ORDER BY info_collected_date DESC, law_or_regulation')
            df = cur.fetchall_arrow().to_pandas()
    df['relevance_label'] = df['relevance'].map(RELEVANCE_LABEL).fillna('Pending Review')
    # The sticker shown in the table: New, or Updated with its version number (for example Updated (v2))
    df['change_label'] = [f'Updated (v{v})' if c == 'UPDATED' else 'New'
                          for c, v in zip(df['change_type'], df['version'])]
    return df


# Run a small query with parameters through the SQL warehouse (values are passed safely, never pasted in)
def run_query(statement, params):
    cfg = Config()
    with sql.connect(server_hostname=cfg.host,
                     http_path=f'/sql/1.0/warehouses/{WAREHOUSE_ID}',
                     credentials_provider=lambda: cfg.authenticate) as conn:
        with conn.cursor() as cur:
            cur.execute(statement, params)
            return cur.fetchall_arrow().to_pandas()


# Every validated version of one document, newest first (for the Previous versions list)
@st.cache_data(ttl=300)
def load_history(document_id):
    return run_query('SELECT version, change_type, law_or_regulation, info_collected_date, published_date, '
                     f'effective_date, source_url FROM {HISTORY_VIEW} '
                     'WHERE document_id = :doc ORDER BY version DESC', {'doc': document_id})


# The exact wording changes between this version and the previous one (found by code, not AI)
@st.cache_data(ttl=300)
def load_diff(document_id, version):
    return run_query(f'SELECT change_no, change_kind, old_text, new_text FROM {DIFF_TABLE} '
                     'WHERE document_id = :doc AND version = :ver ORDER BY change_no',
                     {'doc': document_id, 'ver': int(version)})


# Create the Q and A Agent once and reuse it. The guardrails use keys from the app's secret resources,
# because the app's service principal cannot use the managed identity
@st.cache_resource
def get_agent():
    gc = CFG['guardrails']                                              # guardrail endpoints and limits from the config
    guard = Guardrails(gc['content_safety_endpoint'], gc['language_endpoint'],
                       content_safety_key=os.getenv('CONTENT_SAFETY_KEY'),   # keys only from the secret resources
                       language_key=os.getenv('LANGUAGE_KEY'),
                       block_severity=gc['block_severity'], timeout_seconds=gc['timeout_seconds'])
    with open(os.path.join(ROOT, 'prompts', 'qa_prompt.md')) as f:
        prompt = f.read()
    return QAAgent(warehouse_id=WAREHOUSE_ID, model=MODEL, guard=guard, prompt_template=prompt,
                   functions=f"{CFG['catalog']}.{CFG['schema']}",
                   search_top_k=CFG['agent']['search_top_k'],
                   max_tool_rounds=CFG['agent']['qa_max_tool_rounds'],
                   max_output_tokens=CFG['agent']['qa_max_output_tokens'])


# Build the 12 CPDAP CPL fields in Law Tracker order, for the copy boxes
def law_tracker_fields(r):
    field4 = f"{r['info_collected_date']}; {r['source_url']}"
    return [
        ('Name of Office', r['name_of_office']),
        ('Assessment Date', r['assessment_date']),
        ('Law / Regulation', r['law_or_regulation']),
        ('Date information collected and source', field4),
        ('Date Introduced', r['date_introduced']),
        ('Effective Date', r['effective_date']),
        ('Intention / Background of change', r['intention_background']),
        ('Summary of Key items', r['summary_key_items']),
        ('Areas of Impact for the bank', r['areas_of_impact']),
        ('Proposed Action to be taken and estimated timeline', r['proposed_action_timeline']),
        ('Compliance Risk Category', r['compliance_risk_category']),
        ('Importance', r['importance']),
    ]


# One tab separated row for pasting into Excel (line breaks inside a field become semicolons)
def excel_row(r):
    return '\t'.join(str(v or '').replace('\r', ' ').replace('\n', '; ') for _, v in law_tracker_fields(r))


# Readable text for pasting into an email
def email_text(r):
    return '\n'.join(f'{name}: {value or ""}' for name, value in law_tracker_fields(r))


# Colour the importance and relevance cells
def colour_cells(value):
    if str(value).startswith('Updated'):
        return f'background-color: {UPDATED_COLOUR}'
    return f'background-color: {COLOURS[value]}' if value in COLOURS else ''


# ---------------------------------------------------------------------------
# Header: title, last updated time and the signed in user
# ---------------------------------------------------------------------------
df = load_updates()
user = st.context.headers.get('X-Forwarded-Email', 'CPDAP CPL User')
left, right = st.columns([3, 1])
left.title('Regulatory Scanner')
last = df['created_at_sgt'].max() if not df.empty else None
right.caption(f"Last updated: {last:%d %b %Y, %H:%M} SGT" if last is not None else 'No results yet')
right.caption(f'Signed in as {user}')

tab_changes, tab_assistant = st.tabs(['Latest changes', 'AI Assistant'])

# ---------------------------------------------------------------------------
# Tab 1: Latest changes
# ---------------------------------------------------------------------------
with tab_changes:
    # Filters (Clear filters resets them all)
    if st.session_state.get('clear'):
        for key in ['search', 'regulator', 'change', 'category', 'importance', 'relevance', 'dates']:
            st.session_state.pop(key, None)
        st.session_state['clear'] = False

    f1, f2, f2b, f3, f4, f5, f6, f7 = st.columns([2.6, 1.2, 1.2, 1.7, 1.2, 1.6, 2.1, 1])
    search = f1.text_input('Search', placeholder='Search title, regulator or keyword', key='search')
    regulator = f2.selectbox('Regulator', ['All'] + sorted(df['regulator_id'].dropna().unique()), key='regulator')
    change = f2b.selectbox('Change', ['All', 'New', 'Updated'], key='change')        # New or Updated documents
    category = f3.selectbox('Compliance risk category',
                            ['All'] + sorted(df['compliance_risk_category'].dropna().unique()), key='category')
    importance = f4.selectbox('Importance', ['All', 'High', 'Medium', 'Low', 'Pending Review'], key='importance')
    relevance = f5.selectbox('Relevance', ['All', 'Potentially Relevant', 'Not Relevant'], key='relevance')
    collected = pd.to_datetime(df['info_collected_date'])
    min_d = collected.min().date() if collected.notna().any() else date.today()
    max_d = collected.max().date() if collected.notna().any() else date.today()
    dates = f6.date_input('Date collected', value=(min_d, max_d), key='dates')
    f7.write('')
    f7.button('Clear filters', on_click=lambda: st.session_state.update(clear=True))

    # Apply the filters
    view = df.copy()
    if search:
        text = (view['law_or_regulation'].fillna('') + ' ' + view['regulator_id'].fillna('') + ' '
                + view['summary_key_items'].fillna(''))
        view = view[text.str.contains(search, case=False, regex=False)]
    if regulator != 'All':
        view = view[view['regulator_id'] == regulator]
    if change != 'All':
        view = view[view['change_type'] == change.upper()]
    if category != 'All':
        view = view[view['compliance_risk_category'] == category]
    if importance != 'All':
        view = view[view['importance'] == importance]
    if relevance != 'All':
        view = view[view['relevance_label'] == relevance]
    if isinstance(dates, tuple) and len(dates) == 2:
        d = pd.to_datetime(view['info_collected_date'])
        view = view[(d >= pd.Timestamp(dates[0])) & (d <= pd.Timestamp(dates[1]))]

    # Table, one page at a time
    p1, p2, p3 = st.columns([6, 1, 1])
    page_size = p3.selectbox('Show per page', [5, 10, 20], index=0)
    pages = max(1, -(-len(view) // page_size))
    page = p2.number_input('Page', min_value=1, max_value=pages, value=1)
    p1.caption(f'Showing {min(len(view), (page - 1) * page_size + 1)} to '
               f'{min(len(view), page * page_size)} of {len(view)} results')
    page_df = view.iloc[(page - 1) * page_size: page * page_size].reset_index(drop=True)

    table = page_df[['info_collected_date', 'law_or_regulation', 'regulator_id', 'event_type', 'change_label',
                     'date_introduced', 'compliance_risk_category', 'importance', 'relevance_label']].rename(columns={
        'info_collected_date': 'Date collected', 'law_or_regulation': 'Title / Publication',
        'regulator_id': 'Regulator', 'event_type': 'Type', 'change_label': 'Change',
        'date_introduced': 'Date introduced', 'compliance_risk_category': 'Compliance risk category',
        'importance': 'Importance', 'relevance_label': 'Relevance'})
    styled = table.style.map(colour_cells, subset=['Change', 'Importance', 'Relevance'])
    selection = st.dataframe(styled, hide_index=True, use_container_width=True,
                             on_select='rerun', selection_mode='single-row')

    # Detail panel for the selected row (the first row if nothing is selected)
    rows = selection.selection.rows if selection else []
    if page_df.empty:
        st.info('No results match the filters.')
    else:
        r = page_df.iloc[rows[0] if rows else 0]
        st.divider()
        h1, h2, h3, h4 = st.columns([4, 1.3, 1.3, 1.2])
        h1.subheader(r['law_or_regulation'])
        # Under the title: is this a new document, or an update of an earlier version ?
        if r['change_type'] == 'UPDATED':
            prev = int(r['previous_version']) if pd.notna(r['previous_version']) else int(r['version']) - 1
            h1.caption(f"Updated: version {int(r['version'])}, compared with version {prev} · {r['event_type']}")
        else:
            h1.caption(f"New document · {r['event_type']}")
        with h2.popover('Copy for Excel (12 fields)'):
            st.caption('Click the copy icon, then paste into the Law Tracker row.')
            st.code(excel_row(r), language=None)
        with h3.popover('Copy for Email (formatted)'):
            st.code(email_text(r), language=None)
        h4.link_button('Open original source', r['source_url'] or '#')

        c1, c2, c3 = st.columns([1.1, 1.6, 1.3])
        # Left: the field list
        with c1:
            fields = [('Name of office', r['name_of_office']),
                      ('Law / regulation', r['law_or_regulation']),
                      ('Date information collected', r['info_collected_date']),
                      ('Source', f"[Open source]({r['source_url']})"),
                      ('Date introduced', r['date_introduced']),
                      ('Effective date', r['effective_date']),
                      ('Compliance risk category', r['compliance_risk_category']),
                      ('Importance', r['importance'])]
            st.markdown('\n'.join(f'**{k}**  \n{v}\n' for k, v in fields))
        # Middle: relevance, rationale, background and key items
        with c2:
            # For an update, show what changed first, because that is what compliance needs to see
            if r['change_type'] == 'UPDATED':
                st.markdown('**What changed since the previous version**')
                st.info(r['what_changed'] or 'Not available for this record.')
                diff = load_diff(r['document_id'], r['version'])
                with st.expander(f'Exact wording changes ({len(diff)})'):
                    st.caption('Found by comparing the full text of the two versions (code, not AI).')
                    for _, d in diff.iterrows():
                        st.markdown(f"**{d['change_no']}. {CHANGE_KIND_LABEL.get(d['change_kind'], d['change_kind'])}**")
                        if d['old_text']:
                            st.markdown(f"Old: {d['old_text']}")
                        if d['new_text']:
                            st.markdown(f"New: {d['new_text']}")
            st.markdown(f"**AI Relevance Assessment:** {r['relevance_label']}")
            st.markdown('**AI rationale**')
            st.write(r['relevance_reason'])
            st.markdown('**Intention / Background of change**')
            st.write(r['intention_background'])
            st.markdown('**Summary of Key items**')
            st.markdown(r['summary_key_items'] or '')
        # Right: impact, proposed action and supporting sources
        with c3:
            st.markdown('**Areas of impact for the bank**')
            st.write(r['areas_of_impact'])
            st.markdown('**Proposed action to be taken and estimated timeline**')
            st.write(r['proposed_action_timeline'])
            # Every earlier version of this document, so users can see its history
            if r['change_type'] == 'UPDATED':
                with st.expander('Previous versions'):
                    for _, h in load_history(r['document_id']).iterrows():
                        if h['version'] != r['version']:
                            st.markdown(f"Version {h['version']}: collected {h['info_collected_date']}, "
                                        f"effective {h['effective_date'] or 'Not Stated'}")
            with st.expander('Supporting source information'):
                cites = list(r['citations']) if r['citations'] is not None else []
                st.write(f'{len(cites)} supporting passages from the source document.')
                st.markdown(f"[Open original source]({r['source_url']})")

        st.caption('Draft assessment generated by AI. Subject to CPDAP CPL and SME confirmation. Not legal advice.')

# ---------------------------------------------------------------------------
# Tab 2: AI Assistant
# ---------------------------------------------------------------------------
with tab_assistant:
    a1, a2 = st.columns([1, 3])
    # Left: example questions (click to ask) and a button to start again
    with a1:
        st.markdown('**Example questions**')
        for i, q in enumerate(EXAMPLE_QUESTIONS):
            if st.button(q, key=f'example_{i}', use_container_width=True):
                st.session_state['pending_question'] = q
        if st.button('Clear chat', key='clear_chat'):
            st.session_state['messages'] = []

    # Right: the conversation
    with a2:
        st.markdown('**AI Assistant**')
        st.caption('Ask about the collected regulatory documents. Answers come only from those documents, '
                   'with links to the sources. Each question is answered on its own.')
        messages = st.session_state.setdefault('messages', [])
        chat = st.container(height=520)
        with chat:
            for m in messages:
                with st.chat_message(m['role']):
                    st.markdown(m['content'])

        question = st.chat_input('Ask a question about regulatory updates') \
            or st.session_state.pop('pending_question', None)
        if question:
            messages.append({'role': 'user', 'content': question})
            with chat:
                with st.chat_message('user'):
                    st.markdown(question)
                with st.chat_message('assistant'):
                    with st.spinner('Searching the regulatory documents...'):
                        try:
                            result = get_agent().answer(question)
                            reply = result['answer']
                            print(f"AI Assistant: {result['status']} {result.get('reasons')}")  # app logs
                        except Exception as e:                       # show a clear message instead of a crash
                            reply = f'Sorry, the assistant is not available right now. ({type(e).__name__})'
                            print(f'AI Assistant error: {e}')         # visible in the app logs
                    st.markdown(reply)
            messages.append({'role': 'assistant', 'content': reply})

        st.caption('AI generated from the monitored regulatory documents. Check the linked sources. Not legal advice.')
