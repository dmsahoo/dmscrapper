'''Regulator Agent rules that need no model, and the prompt files.'''
from rcm_intelligence.analysis import check_record, event_type_of, find_differences, parse_json
from rcm_intelligence.prompts_loader import qa_system_prompt, regulator_system_prompt


def test_event_type():
    assert event_type_of({'category_id': '1_NOTICES', 'file_name': 'notice-656.pdf'}) == 'Notices'
    assert event_type_of({'category_id': '1_NOTICES', 'link_text': 'Amendment to Notice 656'}) == 'Amendment'
    # a consultation on proposed amendments is a consultation, not an amendment
    assert event_type_of({'category_id': '2_CONSULTATION_PAPERS',
                          'link_text': 'Proposed Amendments to Notices'}) == 'Consultation Papers'


def test_differences():
    old = 'Para one.\n\nLimit is 25 percent.\n\nPara three.'
    new = 'Para one.\n\nLimit is 20 percent.\n\nPara three.\n\nNew para.'
    kinds = [d['change_kind'] for d in find_differences(old, new)]
    assert kinds == ['CHANGED', 'ADDED']


def test_check_record():
    record = parse_json('text before {"law_or_regulation": "Notice 656", "intention_background": "x", '
                        '"summary_key_items": "x", "areas_of_impact": "x", "proposed_action_timeline": "x", '
                        '"relevance_reason": "x", "compliance_risk_category": "Conduct", "importance": "High", '
                        '"relevance": "RELEVANT", "date_introduced": "05 Mar 2026", "effective_date": "Not Stated", '
                        '"citations": ["c1"]} after')
    assert check_record(record, {'c1'}) == []
    assert check_record(record, {'c1'}, is_update=True) == ['what_changed is empty, but this is an updated version']
    record['citations'] = ['zz']
    assert any('citations' in p for p in check_record(record, {'c1'}))


def test_prompts_have_versions():
    text, version = regulator_system_prompt('MAS')
    assert version == 'base v0.2, mas v0.1' and 'Regulator module: MAS' in text
    assert qa_system_prompt()[1] == 'v0.4'
