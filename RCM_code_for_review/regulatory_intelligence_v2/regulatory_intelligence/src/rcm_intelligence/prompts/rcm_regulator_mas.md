# Regulator module: MAS (Monetary Authority of Singapore)
# Version: v0.1

## Source
Publications from the Monetary Authority of Singapore (www.mas.gov.sg), for example Notices,
Consultation Papers, Responses to Feedback, Guidelines, Information Papers, Circulars,
Enforcement Actions, Speeches and Media Releases.

## MAS specific rules
1. Consultation Papers: date_introduced is the consultation date. effective_date is "N.A.".
   proposed_action_timeline should mention the feedback deadline if the document states one.
2. Responses to Feedback: describe what MAS decided compared with the consultation, if stated.
3. Notices and Guidelines: use the notice or guideline number in law_or_regulation where given
   (for example MAS Notice 626). Take effective_date only from the document.
4. Amendments: say clearly in summary_key_items what is amended.
5. Enforcement actions, speeches and media releases: usually lower importance unless they set out
   new expectations for banks.
