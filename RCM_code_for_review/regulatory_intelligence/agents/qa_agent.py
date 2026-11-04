"""
Q and A Agent (C6): answers questions about regulatory changes, with citations.

1. G1 checks the question (prompt attack, harmful content, PII).
2. The model decides which tool to call: get_changes (changes in a period) or search_chunks (what a document says).
3. The tools run as Unity Catalog functions on the SQL warehouse.
4. The model writes the answer from the tool results only.
5. G2 checks the answer (source links, harmful content).

Same logic as the 05_qa_agent notebook. Used by the Databricks App (and later by a serving endpoint).
It signs in as whoever runs it: the app's service principal in the app.
"""
import json
from datetime import datetime
from zoneinfo import ZoneInfo

import mlflow
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState
from openai import OpenAI

from guardrails.guardrails import clean_citations, NOT_FOUND, BLOCKED_QUESTION, BLOCKED_ANSWER

SGT = ZoneInfo('Asia/Singapore')
FINAL_INSTRUCTION = ('Write the final answer now, using only the tool results above. '
                     'If they do not contain the answer, reply: ' + NOT_FOUND)

# Describe the tools to the model: name, purpose and inputs
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

    # Every name comes from the caller (read from config/<env>.yaml), nothing is hardcoded here
    def __init__(self, warehouse_id, model, guard, prompt_template, functions,
                 search_top_k=8, max_tool_rounds=4, max_output_tokens=4000):
        self.warehouse_id = warehouse_id                     # SQL warehouse that runs the tool functions
        self.model = model                                   # model service name from the config
        self.guard = guard                                   # Guardrails object (G1 and G2)
        self.prompt_template = prompt_template               # text of prompts/qa_prompt.md
        self.functions = functions                           # catalog.schema where get_changes and search_chunks live
        self.search_top_k = search_top_k
        self.max_tool_rounds = max_tool_rounds               # after this, the model must answer without tools
        self.max_output_tokens = max_output_tokens
        self.tool_functions = {'get_changes': self.get_changes, 'search_chunks': self.search_chunks}

    # The prompt with today's date, so the model can turn last month into exact dates
    def system_prompt(self):
        return self.prompt_template.replace('{today}', datetime.now(SGT).strftime('%d %b %Y'))

    # A model client that signs in with the current identity (no stored secrets)
    def model_client(self):
        w = WorkspaceClient()
        token = w.config.authenticate()['Authorization'].split(' ')[1]
        return OpenAI(base_url=f'{w.config.host}/ai-gateway/mlflow/v1', api_key=token)

    # Run a SQL statement on the SQL warehouse and return the rows as a list of dictionaries.
    # Values are passed as parameters, never pasted into the SQL text, so a question cannot change the query.
    def run_sql(self, statement, params):
        response = WorkspaceClient().statement_execution.execute_statement(
            warehouse_id=self.warehouse_id, statement=statement, wait_timeout='50s',
            parameters=[StatementParameterListItem(name=k, value=v, type='STRING') for k, v in params.items()])
        if response.status.state != StatementState.SUCCEEDED:
            raise RuntimeError(f'SQL failed: {response.status.error}')
        columns = [c.name for c in response.manifest.schema.columns]
        return [dict(zip(columns, row)) for row in (response.result.data_array or [])]

    # Tool 1: regulatory changes between two dates (from the validated CPDAP CPL records)
    @mlflow.trace(span_type='TOOL')
    def get_changes(self, from_date=None, to_date=None, regulator=None):
        return self.run_sql(
            f'SELECT * FROM {self.functions}.get_changes(CAST(:from_date AS DATE), CAST(:to_date AS DATE), :regulator)',
            {'from_date': from_date or None, 'to_date': to_date or None,
             'regulator': regulator.upper() if regulator else None})

    # Tool 2: the paragraphs that best match a question (meaning plus keywords)
    @mlflow.trace(span_type='TOOL')
    def search_chunks(self, question, regulator=None):
        return self.run_sql(
            f'SELECT document_id, regulator_id, section_title, published_date, source_url, chunk_text, score '
            f'FROM {self.functions}.search_chunks(:question, :regulator, {self.search_top_k})',
            {'question': question, 'regulator': regulator.upper() if regulator else None})

    # Answer one question. Returns the answer, a status and the reasons (for the screen and the logs)
    @mlflow.trace(span_type='AGENT')
    def answer(self, question):
        # G1: check the question before the model sees it
        g1 = self.guard.check_question(question)
        if not g1['allowed']:
            return {'answer': BLOCKED_QUESTION, 'status': 'BLOCKED_QUESTION', 'reasons': g1['reasons']}
        question = g1['question']                                       # the question with any PII removed

        client = self.model_client()
        messages = [{'role': 'system', 'content': self.system_prompt()},
                    {'role': 'user', 'content': question}]
        allowed_urls, tools_used = set(), []

        for round_no in range(self.max_tool_rounds + 1):
            # On the last round, the model may not call tools: it must answer from what it already has
            last_round = round_no == self.max_tool_rounds
            if last_round:
                messages.append({'role': 'user', 'content': FINAL_INSTRUCTION})

            # Ask the model: it either answers, or asks for one or more tool calls
            response = client.chat.completions.create(model=self.model, messages=messages, tools=TOOLS,
                                                      tool_choice='none' if last_round else 'auto',
                                                      max_tokens=self.max_output_tokens, temperature=0)
            message = response.choices[0].message
            if last_round or not message.tool_calls:
                text = clean_citations(message.content or NOT_FOUND)
                # G2: check the answer before the user sees it
                g2 = self.guard.check_answer(text, allowed_urls)
                if not g2['allowed']:
                    return {'answer': BLOCKED_ANSWER, 'status': 'BLOCKED_ANSWER',
                            'reasons': g1['reasons'] + g2['reasons'], 'tools': tools_used}
                return {'answer': text, 'status': 'NOT_FOUND' if NOT_FOUND in text else 'ANSWERED',
                        'reasons': g1['reasons'], 'tools': tools_used}

            # Run each tool the model asked for and give it the results
            messages.append({'role': 'assistant', 'content': message.content or '',
                             'tool_calls': [{'id': t.id, 'type': 'function',
                                             'function': {'name': t.function.name,
                                                          'arguments': t.function.arguments}}
                                            for t in message.tool_calls]})
            for t in message.tool_calls:
                args = json.loads(t.function.arguments or '{}')
                tools_used.append(f'{t.function.name} {args}')
                try:
                    result = self.tool_functions[t.function.name](**args)
                except Exception as e:                                   # tell the model the tool failed, do not crash
                    result = {'error': str(e)}
                if isinstance(result, list):                             # remember the source links for G2
                    allowed_urls.update(r['source_url'] for r in result if r.get('source_url'))
                messages.append({'role': 'tool', 'tool_call_id': t.id, 'content': json.dumps(result, default=str)})
