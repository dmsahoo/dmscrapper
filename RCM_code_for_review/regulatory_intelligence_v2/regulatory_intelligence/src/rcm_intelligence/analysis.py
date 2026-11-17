'''
Regulator Agent logic that does not need Spark or a model (pure functions, unit tested).

1. Allowed values of the CPDAP CPL fields.
2. event_type_of: kind of change from the file and the category.
3. find_differences: exact differences between two versions (code, not AI).
4. build_user_message: the message sent to the model for one document.
5. parse_json and check_record: read the model answer and check it in code.
'''

import difflib
import json
import re

RISK_CATEGORIES = {'AML', 'CFT', 'Sanctions', 'Data Protection', 'Conduct',
                   'Operational Risk', 'Cybersecurity', 'Other'}
IMPORTANCE = {'High', 'Medium', 'Low', 'Pending Review'}
RELEVANCE = {'RELEVANT', 'NOT_RELEVANT', 'EXCLUDED'}
TEXT_FIELDS = ['law_or_regulation', 'intention_background', 'summary_key_items',
               'areas_of_impact', 'proposed_action_timeline', 'relevance_reason']
DATE_PATTERN = re.compile(r'^\d{1,2} [A-Z][a-z]{2} \d{4}$')            # for example 05 Mar 2026

# Task states in rcm_agent_task
QUEUED, ANALYSED, VALIDATED, FLAGGED, ERROR = 'QUEUED', 'ANALYSED', 'VALIDATED', 'FLAGGED', 'ERROR'


def event_type_of(details):
    '''Kind of change. A file whose link text or name contains the word amendment is an Amendment,
    unless the category is a consultation (a consultation on proposed amendments is not an amendment).
    Otherwise the category name is used, for example 1_NOTICES becomes Notices.'''
    category = str(details.get('category_id') or '')
    category_name = category.split('_', 1)[-1].replace('_', ' ').title() or 'Other'
    file_words = f"{details.get('link_text') or ''} {details.get('file_name') or ''}".lower()
    if 'amendment' in file_words and 'consultation' not in category.lower():
        return 'Amendment'
    return category_name


def paragraphs_of(text):
    '''Paragraphs with tidy spaces, so a changed line break is not seen as a change.'''
    parts = re.split(r'\n\s*\n', text or '')
    return [re.sub(r'\s+', ' ', p).strip() for p in parts if p.strip()]


def find_differences(old_text, new_text):
    '''Every difference between two versions, paragraph by paragraph (Python difflib, exact and
    repeatable). Each one is ADDED, REMOVED or CHANGED, with the old and the new wording.'''
    old_paras, new_paras = paragraphs_of(old_text), paragraphs_of(new_text)
    matcher = difflib.SequenceMatcher(a=old_paras, b=new_paras, autojunk=False)
    differences = []
    for kind, a1, a2, b1, b2 in matcher.get_opcodes():
        if kind == 'equal':
            continue
        differences.append({
            'change_kind': {'insert': 'ADDED', 'delete': 'REMOVED', 'replace': 'CHANGED'}[kind],
            'old_text': '\n\n'.join(old_paras[a1:a2]) or None,
            'new_text': '\n\n'.join(new_paras[b1:b2]) or None,
        })
    return differences


def differences_for_prompt(differences, max_passages, max_chars):
    '''Short text of the differences for the model, within the size limits from the config.'''
    lines, used = [], 0
    for n, d in enumerate(differences[:max_passages], start=1):
        piece = (f"Change {n} ({d['change_kind']}):\n"
                 + (f"  OLD: {d['old_text']}\n" if d['old_text'] else '')
                 + (f"  NEW: {d['new_text']}\n" if d['new_text'] else ''))
        if used + len(piece) > max_chars:
            lines.append(f'... {len(differences) - n + 1} more changes not shown')
            break
        lines.append(piece)
        used += len(piece)
    return '\n'.join(lines)


def build_user_message(details, chunks, previous=None, differences=None, max_passages=40, max_chars=15000):
    '''Document details, then (for an update) the previous record and the exact differences,
    then every chunk of the current version with its chunk_id.'''
    header = (f"Document title on the website: {details.get('item_title')}\n"
              f"Date on the website: {details.get('item_date')} ({details.get('item_date_label')})\n"
              f"Category: {details.get('category_id')}\n"
              f"Source URL: {details.get('source_url')}\n\n")
    if previous or differences:
        header += 'This is an UPDATED version of a document analysed before.\n'
        if previous:
            header += (f"Previous version {previous['version']}:\n"
                       f"  Title: {previous['law_or_regulation']}\n"
                       f"  Date introduced: {previous['date_introduced']}\n"
                       f"  Effective date: {previous['effective_date']}\n"
                       f"  Summary of key items: {previous['summary_key_items']}\n\n")
        if differences:
            header += ('Exact differences between the previous and this version, found by comparing the full texts:\n'
                       + differences_for_prompt(differences, max_passages, max_chars) + '\n\n')
        header += ('Also return the field what_changed: 2 to 4 short sentences on what is different in this '
                   'version, based on the differences above and the chunks below. Mention changed figures, '
                   'dates and obligations. Ignore changes that are only formatting.\n\n')
    else:
        header += 'This is a NEW document. Return the field what_changed as an empty string.\n\n'
    body = '\n\n'.join(f"[{c['chunk_id']}] (section: {c.get('section_title') or '-'}, "
                       f"page: {c.get('page_from') or '-'})\n{c['chunk_text']}" for c in chunks)
    return header + 'Document chunks:\n' + body


def parse_json(text):
    '''The JSON object in the model answer (any text before or after it is ignored).'''
    start, end = (text or '').find('{'), (text or '').rfind('}')
    if start < 0 or end <= start:
        raise ValueError('No JSON object found in the answer')
    return json.loads(text[start:end + 1])


def check_record(record, valid_chunk_ids, is_update=False):
    '''Code checks of one answer. Returns a list of problems; empty means the record passes.'''
    problems = []
    for field in TEXT_FIELDS:
        if not str(record.get(field) or '').strip():
            problems.append(f'{field} is empty')
    if record.get('compliance_risk_category') not in RISK_CATEGORIES:
        problems.append(f'compliance_risk_category must be one of {sorted(RISK_CATEGORIES)}')
    if record.get('importance') not in IMPORTANCE:
        problems.append(f'importance must be one of {sorted(IMPORTANCE)}')
    if record.get('relevance') not in RELEVANCE:
        problems.append(f'relevance must be one of {sorted(RELEVANCE)}')
    if not (record.get('date_introduced') == 'N.A.' or DATE_PATTERN.match(str(record.get('date_introduced')))):
        problems.append('date_introduced must be DD Mon YYYY or N.A.')
    if not (record.get('effective_date') in ('N.A.', 'Not Stated')
            or DATE_PATTERN.match(str(record.get('effective_date')))):
        problems.append('effective_date must be DD Mon YYYY, Not Stated or N.A.')
    citations = record.get('citations') or []
    if not isinstance(citations, list) or not citations:
        problems.append('citations must list at least one chunk_id')
    else:
        unknown = [c for c in citations if c not in valid_chunk_ids]
        if unknown:
            problems.append(f'these citations are not chunk_id values of this document: {unknown[:5]}')
    if is_update and not str(record.get('what_changed') or '').strip():
        problems.append('what_changed is empty, but this is an updated version')
    return problems
