'''
Guardrails for the Q and A Agent.

G1 checks the user question before the model sees it:
    1. Prompt Shields (Azure AI Content Safety): blocks jailbreak and prompt attack attempts.
    2. Harmful content (Azure AI Content Safety): blocks hate, self harm, sexual and violent content.
    3. PII (Azure AI Language): removes personal data, such as NRIC or phone numbers, before the model sees it.
G2 checks the answer before the user sees it:
    1. Sources (code): the answer must link to sources that the tools really returned.
    2. Harmful content (Azure AI Content Safety).

Both fail closed: if a check cannot run, the question or the answer is blocked.
Shared by the 06_agent_rcm_qa notebook and the Databricks App (through qa_agent.py).
Settings come from the guardrails block of config/common_config_<env>.yaml.

Every check also returns a short structured result (check point, check name, result, category),
which the Q and A Agent writes to rcm_guardrail_log. The question or answer text is never logged.
'''
import re

import mlflow
import requests

SCOPE = 'https://cognitiveservices.azure.com/.default'                 # Entra ID scope for Azure AI services
API_SAFETY = '2024-09-01'                                              # Azure AI Content Safety API version
API_LANGUAGE = '2023-04-01'                                            # Azure AI Language API version
HARM_CATEGORIES = ['Hate', 'SelfHarm', 'Sexual', 'Violence']
# Personal data to remove from questions. Organizations and dates are kept, because questions
# name regulators (for example MAS) and periods (for example October 2026)
PII_CATEGORIES = ['Person', 'PhoneNumber', 'Email', 'Address', 'IPAddress', 'CreditCardNumber',
                  'InternationalBankingAccountNumber', 'SGNationalRegistrationIdentityCardNumber']

NOT_FOUND = 'I could not find this in the monitored regulatory documents.'
BLOCKED_QUESTION = ('This question cannot be answered. '
                    'Please ask a question about regulatory changes or regulatory documents.')
BLOCKED_ANSWER = ('I could not give an answer with verified sources. '
                  'Please rephrase the question, or check the Latest changes tab.')


# One structured guardrail result, as written to rcm_guardrail_log (never the checked text)
def check_result(check_point, check_name, result, category=None):
    return {'check_point': check_point, 'check_name': check_name, 'result': result, 'category': category}


# Turn citations in 【 】 brackets (a habit of some models) into normal Markdown links
def clean_citations(text):
    text = text.replace('‑', '-').replace('‐', '-')         # special hyphens become normal hyphens, so links work
    text = text.replace('\\[', '[').replace('\\]', ']')               # \[title\] becomes [title], so links work
    text = re.sub(r'【([^】]+)】\s*\(', r'[\1](', text)                # 【title】(url)  becomes  [title](url)
    text = re.sub(r'【(https?://[^】]+)】', r'[\1](\1)', text)         # 【url】         becomes  [url](url)
    return text


# Check the links in an answer: there must be at least one, and each must come from the tool results
def source_problems(text, allowed_urls):
    links = re.findall(r'\]\((https?://[^)\s]+)\)', text)
    if NOT_FOUND in text and not links:                                 # an honest not found answer is allowed
        return []
    problems = []
    if not links:
        problems.append('the answer has no source link')
    unknown = [u for u in links if u not in allowed_urls]
    if unknown:
        problems.append(f'links that are not in the tool results: {unknown}')
    return problems


class Guardrails:
    '''Calls Azure AI Content Safety and Azure AI Language.
    Sign in with an Entra ID token (get_token, used by notebooks with the managed identity),
    or with keys (used by the Databricks App in the lab, where the managed identity is not available).'''

    # Endpoints come from the config. Sign in with get_token (managed identity) or with keys (app, lab only)
    def __init__(self, content_safety_endpoint, language_endpoint, get_token=None,
                 content_safety_key=None, language_key=None, block_severity=2, timeout_seconds=10):
        self.safety_endpoint = content_safety_endpoint.rstrip('/')    # Azure AI Content Safety resource
        self.language_endpoint = language_endpoint.rstrip('/')        # Azure AI services or Language resource
        self.get_token = get_token                                      # function that returns a fresh access token
        self.safety_key = content_safety_key
        self.language_key = language_key
        self.block_severity = block_severity                            # 0 safe, 2 low, 4 medium, 6 high
        self.timeout = timeout_seconds

    # Send one request to an Azure endpoint and return the JSON answer (key if given, otherwise token)
    def _post(self, endpoint, key, path, api_version, body):
        headers = {'Ocp-Apim-Subscription-Key': key} if key else {'Authorization': f'Bearer {self.get_token()}'}
        response = requests.post(f'{endpoint}{path}?api-version={api_version}', json=body, headers=headers,
                                 timeout=self.timeout)
        if not response.ok:                                             # include Azure's reason in the error
            raise RuntimeError(f'{response.status_code} from {path}: {response.text[:300]}')
        return response.json()

    # Prompt Shields: True when the text tries to override the instructions (jailbreak)
    def prompt_attack(self, text):
        result = self._post(self.safety_endpoint, self.safety_key, '/contentsafety/text:shieldPrompt', API_SAFETY,
                            {'userPrompt': text[:10000]})
        return result['userPromptAnalysis']['attackDetected']

    # Harmful content: the categories at or above the block level, for example ['Violence 4']
    def harmful(self, text):
        result = self._post(self.safety_endpoint, self.safety_key, '/contentsafety/text:analyze', API_SAFETY,
                            {'text': text[:10000], 'categories': HARM_CATEGORIES,
                             'outputType': 'FourSeverityLevels'})
        return [f"{c['category']} {c['severity']}" for c in result['categoriesAnalysis']
                if c['severity'] >= self.block_severity]

    # PII: the text with personal data replaced by *****, and the kinds of PII found
    def redact_pii(self, text):
        result = self._post(self.language_endpoint, self.language_key, '/language/:analyze-text', API_LANGUAGE,
                            {'kind': 'PiiEntityRecognition',
                             'parameters': {'piiCategories': PII_CATEGORIES},
                             'analysisInput': {'documents': [{'id': '1', 'language': 'en', 'text': text}]}})
        doc = result['results']['documents'][0]
        return doc['redactedText'], sorted({e['category'] for e in doc['entities']})

    # G1: check the question. Returns allowed, the question to use (PII removed), the reasons
    # and the structured checks for rcm_guardrail_log
    @mlflow.trace(name='g1_input_check')
    def check_question(self, question):
        checks = []
        try:
            if self.prompt_attack(question):
                checks.append(check_result('G1', 'PROMPT_SHIELD', 'BLOCKED', 'prompt attack'))
                return {'allowed': False, 'question': question, 'reasons': ['prompt attack detected'], 'checks': checks}
            checks.append(check_result('G1', 'PROMPT_SHIELD', 'PASSED'))
            harm = self.harmful(question)
            if harm:
                checks.append(check_result('G1', 'HARMFUL_CONTENT', 'BLOCKED', ', '.join(harm)))
                return {'allowed': False, 'question': question, 'reasons': [f'harmful content: {harm}'], 'checks': checks}
            checks.append(check_result('G1', 'HARMFUL_CONTENT', 'PASSED'))
            clean, pii = self.redact_pii(question)
            checks.append(check_result('G1', 'PII', 'MASKED' if pii else 'PASSED', ', '.join(pii) or None))
            return {'allowed': True, 'question': clean,
                    'reasons': [f'PII removed: {pii}'] if pii else [], 'checks': checks}
        except Exception as e:                                          # fail closed
            checks.append(check_result('G1', 'SERVICE', 'ERROR', type(e).__name__))
            return {'allowed': False, 'question': question, 'reasons': [f'guardrail service error: {e}'],
                    'checks': checks}

    # G2: check the answer. Returns allowed, the reasons and the structured checks
    @mlflow.trace(name='g2_output_check')
    def check_answer(self, answer, allowed_urls):
        reasons = source_problems(answer, allowed_urls)
        checks = [check_result('G2', 'SOURCES', 'BLOCKED' if reasons else 'PASSED',
                               'unknown or missing link' if reasons else None)]
        if not reasons:
            try:
                harm = self.harmful(answer)
                if harm:
                    reasons.append(f'harmful content: {harm}')
                checks.append(check_result('G2', 'HARMFUL_CONTENT', 'BLOCKED' if harm else 'PASSED',
                                           ', '.join(harm) or None))
            except Exception as e:                                      # fail closed
                reasons.append(f'guardrail service error: {e}')
                checks.append(check_result('G2', 'SERVICE', 'ERROR', type(e).__name__))
        return {'allowed': not reasons, 'reasons': reasons, 'checks': checks}
