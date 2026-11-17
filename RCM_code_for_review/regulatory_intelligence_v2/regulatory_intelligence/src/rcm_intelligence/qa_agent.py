'''
Q and A Agent (C6): answers questions about regulatory changes, with citations.

1. G1 checks the question (prompt attack, harmful content, personal data).
2. The model decides which tool to call:
     get_changes    changes in a period (Unity Catalog function fn_get_changes, on the SQL warehouse)
     search_chunks  what a document says (Databricks Vector Search index, hybrid query)
3. The model writes the answer from the tool results only.
4. G2 checks the answer (source links must come from the tool results, harmful content).
5. Every guardrail result goes to rcm_guardrail_log, and every request to rcm_user_request_log.
   Neither table holds the question or the answer text, only a SHA 256 hash of the question.

Used by the 06_agent_rcm_qa notebook and the Databricks App. It signs in as whoever runs it:
the job identity in a notebook, the app service principal in the app.
'''

import json
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

import mlflow
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState
from openai import OpenAI

from rcm_intelligence import vector_index
from rcm_intelligence.common import current_trace_id, release_tag, sha256_text
from rcm_intelligence.guardrails import BLOCKED_ANSWER, BLOCKED_QUESTION, NOT_FOUND, clean_citations
from rcm_intelligence.logging_setup import get_logger
from rcm_intelligence.prompts_loader import qa_system_prompt
from rcm_intelligence.settings import full_name

SGT = ZoneInfo('Asia/Singapore')
FINAL_INSTRUCTION = ('Write the final answer now, using only the tool results above. '
                     'If they do not contain the answer, reply: ' + NOT_FOUND)

# The tools as the model sees them: name, purpose and inputs
TOOLS = [
    {'type': 'function', 'function': {
        'name': 'get_changes',
        'description': 'Lists regulatory changes published between two dates, newest first. '
                       'Use for questions about what is new or what changed in a period.',
        'parameters': {'type': 'object', 'properties': {
            'from_date': {'type': 'string', 'description': 'First date, format yyyy-mm-dd. Empty for no start limit.'},
            'to_date': {'type': 'string', 'description': 'Last date, format yyyy-mm-dd. Empty for no end limit.'},
            'regulator': {'type': 'string', 'description': 'Regulator id such as MAS. Empty for all regulators.'}}}}},
    {'type': 'function', 'function': {
        'name': 'search_chunks',
        'description': 'Finds the paragraphs in the regulatory documents that best match a question. '
                       'Use for questions about what a document says, requires or defines.',
        'parameters': {'type': 'object', 'properties': {
            'question': {'type': 'string', 'description': 'What to search for, in plain English.'},
            'regulator': {'type': 'string', 'description': 'Regulator id such as MAS. Empty for all regulators.'}},
            'required': ['question']}}},
]


class QAAgent:

    def __init__(self, config, guard, warehouse_id):
        '''config: the loaded common config. guard: a Guardrails object. warehouse_id: SQL warehouse
        that runs fn_get_changes and the log inserts.'''
        agent = config['agent']
        self.config = config
        self.guard = guard
        self.warehouse_id = warehouse_id
        self.model = config['model_service']
        self.agent_name = agent['qa_agent_name']
        self.search_top_k = int(agent['search_top_k'])
        self.max_tool_rounds = int(agent['qa_max_tool_rounds'])
        self.max_output_tokens = int(agent['qa_max_output_tokens'])
        self.get_changes_function = full_name(config, 'functions', 'get_changes')
        self.guardrail_log = full_name(config, 'tables', 'guardrail_log')
        self.request_log = full_name(config, 'tables', 'user_request_log')
        self.prompt_template, self.prompt_version = qa_system_prompt()
        self.tool_functions = {'get_changes': self.get_changes, 'search_chunks': self.search_chunks}

    def system_prompt(self):
        '''The prompt with today's date, so the model can turn last month into exact dates.'''
        return self.prompt_template.replace('{today}', datetime.now(SGT).strftime('%d %b %Y'))

    def model_client(self):
        '''Model client that signs in with the current identity (no stored secrets).'''
        w = WorkspaceClient()
        token = w.config.authenticate()['Authorization'].split(' ')[1]
        return OpenAI(base_url=f'{w.config.host}/ai-gateway/mlflow/v1', api_key=token)

    def run_sql(self, statement, params):
        '''Run SQL on the warehouse. Values are parameters, never pasted into the SQL text.'''
        response = WorkspaceClient().statement_execution.execute_statement(
            warehouse_id=self.warehouse_id, statement=statement, wait_timeout='50s',
            parameters=[StatementParameterListItem(name=k, value=None if v is None else str(v), type='STRING')
                        for k, v in params.items()])
        if response.status.state != StatementState.SUCCEEDED:
            raise RuntimeError(f'SQL failed: {response.status.error}')
        if not response.manifest or not response.manifest.schema or not response.manifest.schema.columns:
            return []
        columns = [c.name for c in response.manifest.schema.columns]
        return [dict(zip(columns, row)) for row in ((response.result.data_array if response.result else None) or [])]

    # ---- tools -------------------------------------------------------------------

    @mlflow.trace(span_type='TOOL')
    def get_changes(self, from_date=None, to_date=None, regulator=None):
        '''Tool 1: validated regulatory changes between two dates.'''
        return self.run_sql(
            f'SELECT * FROM {self.get_changes_function}(CAST(:from_date AS DATE), CAST(:to_date AS DATE), :regulator)',
            {'from_date': from_date or None, 'to_date': to_date or None,
             'regulator': regulator.upper() if regulator else None})

    @mlflow.trace(span_type='RETRIEVER')
    def search_chunks(self, question, regulator=None):
        '''Tool 2: the paragraphs that best match a question (Vector Search, hybrid).'''
        return vector_index.search(self.config, question, regulator, self.search_top_k)

    # ---- logs ----------------------------------------------------------------------

    def _write_logs(self, request_id, question, checks, status, tools_used, user, session_id, trace_id):
        '''Write the guardrail results and the request to the log tables. A log problem is reported
        in the application log but does not stop the answer.'''
        question_hash = sha256_text(question)
        try:
            if checks:
                rows, params = [], {}
                for n, c in enumerate(checks):
                    rows.append(f"(:id{n}, :request_id, :cp{n}, :cn{n}, :res{n}, :cat{n}, :question_hash, "
                                f":trace_id, :agent_name, from_utc_timestamp(current_timestamp(), 'Asia/Singapore'))")
                    params.update({f'id{n}': str(uuid.uuid4()), f'cp{n}': c['check_point'], f'cn{n}': c['check_name'],
                                   f'res{n}': c['result'], f'cat{n}': c.get('category')})
                params.update(request_id=request_id, question_hash=question_hash, trace_id=trace_id,
                              agent_name=self.agent_name)
                self.run_sql(f'INSERT INTO {self.guardrail_log} (log_id, request_id, check_point, check_name, result, '
                             f'category, question_hash, trace_id, agent_name, created_at_sgt) VALUES ' + ', '.join(rows),
                             params)
            self.run_sql(
                f"INSERT INTO {self.request_log} (request_id, user_email, session_id, question_hash, status, "
                f"tools_used, prompt_version, release_tag, trace_id, created_at_sgt) VALUES (:request_id, :user, "
                f":session_id, :question_hash, :status, :tools, :prompt_version, :release_tag, :trace_id, "
                f"from_utc_timestamp(current_timestamp(), 'Asia/Singapore'))",
                {'request_id': request_id, 'user': user, 'session_id': session_id, 'question_hash': question_hash,
                 'status': status, 'tools': '; '.join(tools_used)[:4000], 'prompt_version': self.prompt_version,
                 'release_tag': release_tag(), 'trace_id': trace_id})
        except Exception as error:
            get_logger().warning('Could not write the request and guardrail logs for %s: %s', request_id, error)

    # ---- answer ----------------------------------------------------------------------

    @mlflow.trace(span_type='AGENT')
    def answer(self, question, user=None, session_id=None):
        '''Answer one question. Returns answer, status, reasons, tools and request_id.'''
        request_id = str(uuid.uuid4())
        trace_id = current_trace_id()
        result = self._answer(question)
        self._write_logs(request_id, question, result.pop('checks', []), result['status'], result.get('tools', []),
                         user, session_id, trace_id)
        result['request_id'] = request_id
        return result

    def _answer(self, question):
        g1 = self.guard.check_question(question)
        checks = list(g1.get('checks') or [])
        if not g1['allowed']:
            return {'answer': BLOCKED_QUESTION, 'status': 'BLOCKED_QUESTION', 'reasons': g1['reasons'],
                    'tools': [], 'checks': checks}
        question = g1['question']                                       # the question with personal data removed

        client = self.model_client()
        messages = [{'role': 'system', 'content': self.system_prompt()},
                    {'role': 'user', 'content': question}]
        allowed_urls, tools_used = set(), []

        for round_no in range(self.max_tool_rounds + 1):
            # On the last round the model may not call tools: it must answer with what it has
            last_round = round_no == self.max_tool_rounds
            if last_round:
                messages.append({'role': 'user', 'content': FINAL_INSTRUCTION})
            response = client.chat.completions.create(model=self.model, messages=messages, tools=TOOLS,
                                                      tool_choice='none' if last_round else 'auto',
                                                      max_tokens=self.max_output_tokens, temperature=0)
            message = response.choices[0].message
            if last_round or not message.tool_calls:
                text = clean_citations(message.content or NOT_FOUND)
                g2 = self.guard.check_answer(text, allowed_urls)
                checks += g2.get('checks') or []
                if not g2['allowed']:
                    return {'answer': BLOCKED_ANSWER, 'status': 'BLOCKED_ANSWER',
                            'reasons': g1['reasons'] + g2['reasons'], 'tools': tools_used, 'checks': checks}
                return {'answer': text, 'status': 'NOT_FOUND' if NOT_FOUND in text else 'ANSWERED',
                        'reasons': g1['reasons'], 'tools': tools_used, 'checks': checks}

            # Run each tool the model asked for, and give it the results
            messages.append({'role': 'assistant', 'content': message.content or '',
                             'tool_calls': [{'id': t.id, 'type': 'function',
                                             'function': {'name': t.function.name, 'arguments': t.function.arguments}}
                                            for t in message.tool_calls]})
            for t in message.tool_calls:
                args = json.loads(t.function.arguments or '{}')
                tools_used.append(f'{t.function.name} {args}')
                try:
                    result = self.tool_functions[t.function.name](**args)
                except Exception as error:                              # tell the model, do not crash
                    result = {'error': f'{type(error).__name__}'}
                if isinstance(result, list):                            # remember the source links for G2
                    allowed_urls.update(r['source_url'] for r in result if r.get('source_url'))
                messages.append({'role': 'tool', 'tool_call_id': t.id, 'content': json.dumps(result, default=str)})
