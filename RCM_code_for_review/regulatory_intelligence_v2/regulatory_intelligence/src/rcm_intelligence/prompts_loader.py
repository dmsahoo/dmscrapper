'''
Reads the prompt files that ship inside this package (folder prompts), with their version.

Names follow P02 Appendix A: prompts/<use_case>_<agent>_<role>.md
  rcm_regulator_base.md         shared rules of the Regulator Agent
  rcm_regulator_<regulator>.md  rules for one regulator, for example rcm_regulator_mas.md
  rcm_qa_system.md              system prompt of the Q and A Agent

Each file starts with a header line such as "# Version: v0.2". The version is stored on every
analysis record, so each record can be traced back to the exact prompt that produced it.
'''

import re
from importlib import resources

PROMPT_PACKAGE = 'rcm_intelligence.prompts'
_VERSION_LINE = re.compile(r'^#\s*Version:\s*(\S+)', re.MULTILINE)


def load_prompt(file_name):
    '''Text and version of one prompt file. Fails if the file has no version line.'''
    path = resources.files(PROMPT_PACKAGE).joinpath(file_name)
    if not path.is_file():
        raise FileNotFoundError(f'Prompt file {file_name} not found in the package')
    text = path.read_text(encoding='utf8')
    match = _VERSION_LINE.search(text)
    if not match:
        raise ValueError(f'Prompt file {file_name} has no "# Version:" line')
    return text, match.group(1)


def regulator_system_prompt(regulator_id):
    '''System prompt of the Regulator Agent for one regulator: the base prompt, then the regulator
    module. Returns (text, version), where version names both parts, for example base v0.2, mas v0.1.'''
    base, base_version = load_prompt('rcm_regulator_base.md')
    module_name = f'rcm_regulator_{regulator_id.lower()}.md'
    module, module_version = load_prompt(module_name)
    return base + '\n\n' + module, f'base {base_version}, {regulator_id.lower()} {module_version}'


def qa_system_prompt():
    '''System prompt of the Q and A Agent, as (text, version).'''
    return load_prompt('rcm_qa_system.md')
