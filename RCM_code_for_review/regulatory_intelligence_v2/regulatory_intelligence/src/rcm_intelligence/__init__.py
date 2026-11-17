'''
rcm_intelligence: shared code of the Regulatory Change Management (RCM) internal zone.

Installed on the job clusters as a wheel by the deployment bundle, and in the Databricks App
from the repository root, so no code needs folder paths (no os.getcwd, no sys.path).

Modules
  settings          environment, config file, every object name (SMBC ADP layers)
  logging_setup     labelled logging and run log files in the logs volume
  common            time, run ids, fingerprints, release information, Delta retry
  text_processing   text extraction and chunking
  vector_index      Databricks Vector Search index: create, sync, validate, search
  analysis          Regulator Agent rules: differences, user message, code checks
  prompts_loader    prompt files and their versions
  guardrails        G1 and G2 (Azure AI Content Safety, Azure AI Language)
  qa_agent          Q and A Agent with its tools and logs
  ddl_runner        runs the SQL files in the ddl folder
'''

__version__ = '2.0.0'
