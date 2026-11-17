'''
Logging for the internal jobs, the agents and the app.

How it works
  1. Every log line carries the same labels: stage (COPY, INGEST, INDEX, ANALYSIS or QA), regulator_id, run_id
     and the time in Singapore time (SGT).
  2. The notebook output shows a readable line:
       20261117 10:15:02 SGT | INFO | COPY | ALL | copy_20261117_101500_a1b2c3 | message
  3. The same lines are also written as JSON (one object per line) to a file for the run.
     At the end of the run the file is copied to a Unity Catalog volume, one folder per
     regulator and per day:
       <log_root>/<regulator_id>/<yyyy-mm-dd>/<run_id>.log
     So all logs of one regulator are found in one folder, and one run is one file.

The tables (rcm_promotion_log, rcm_agent_task, rcm_guardrail_log) stay the main record. These logs
hold the technical detail for troubleshooting.

Why the file is written locally first: Unity Catalog volumes do not support appending to an
open file, so the run writes to the local disk of the cluster and copies the finished file.
'''

import json
import logging
import os
import shutil
import tempfile
from datetime import datetime
from zoneinfo import ZoneInfo

LOGGER_NAME = 'rcm_intelligence'
_SGT = ZoneInfo('Asia/Singapore')

# Labels added to every log line. Set once per run with set_log_context().
_context = {'stage': '-', 'regulator_id': 'ALL', 'run_id': '-'}


def set_log_context(stage=None, regulator_id=None, run_id=None):
    '''Set the labels that every following log line carries.'''
    if stage is not None:
        _context['stage'] = stage
    if regulator_id is not None:
        _context['regulator_id'] = regulator_id
    if run_id is not None:
        _context['run_id'] = run_id


class _ContextFilter(logging.Filter):
    '''Adds stage, regulator_id and run_id to each log record.'''
    def filter(self, record):
        for key, value in _context.items():
            if not hasattr(record, key):
                setattr(record, key, value)
        return True


def _sgt_text(record, pattern):
    return datetime.fromtimestamp(record.created, _SGT).strftime(pattern)


class _ReadableFormatter(logging.Formatter):
    '''One readable line for the notebook and job output.'''
    def format(self, record):
        line = (f'{_sgt_text(record, "%Y%m%d %H:%M:%S")} SGT | {record.levelname} | '
                f'{record.stage} | {record.regulator_id} | {record.run_id} | {record.getMessage()}')
        if record.exc_info:
            line += '\n' + self.formatException(record.exc_info)
        return line


class JsonFormatter(logging.Formatter):
    '''One JSON object per line for the log file, easy to search and to load into a table.'''
    def format(self, record):
        entry = {
            'time_sgt': _sgt_text(record, '%Y-%m-%d %H:%M:%S'),
            'level': record.levelname,
            'stage': record.stage,
            'regulator_id': record.regulator_id,
            'run_id': record.run_id,
            'module': record.module,
            'message': record.getMessage(),
        }
        if record.exc_info:
            entry['exception'] = self.formatException(record.exc_info)
        return json.dumps(entry, ensure_ascii=False, default=str)


def get_logger():
    '''The shared logger. Writes readable lines to the notebook and job output.
    The level comes from the cluster environment variable RCM_LOG_LEVEL (default INFO).'''
    logger = logging.getLogger(LOGGER_NAME)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.addFilter(_ContextFilter())
        handler.setFormatter(_ReadableFormatter())
        logger.addHandler(handler)
        logger.setLevel(os.environ.get('RCM_LOG_LEVEL', 'INFO').upper())
        logger.propagate = False
    return logger


def run_log_path(log_root, regulator_id, run_id, day):
    '''Where the log file of one run is stored: one folder per regulator and per day.'''
    return f'{log_root.rstrip("/")}/{regulator_id}/{day.strftime("%Y-%m-%d")}/{run_id}.log'


class RunLog:
    '''The log file of one run. start() begins writing JSON lines to a local file,
    publish() copies it to the volume. Call publish() in a finally block so the log is
    kept even when the run fails.'''

    def __init__(self, log_root, stage, regulator_id, run_id):
        self.log_root = log_root
        self.regulator_id = regulator_id
        self.run_id = run_id
        set_log_context(stage=stage, regulator_id=regulator_id, run_id=run_id)
        self.local_path = os.path.join(tempfile.gettempdir(), 'rcm_intelligence_logs', f'{run_id}.log')
        self.handler = None
        self.target_path = None

    def start(self):
        '''Start writing the JSON log file for this run.'''
        os.makedirs(os.path.dirname(self.local_path), exist_ok=True)
        self.handler = logging.FileHandler(self.local_path, encoding='utf8')
        self.handler.addFilter(_ContextFilter())
        self.handler.setFormatter(JsonFormatter())
        get_logger().addHandler(self.handler)
        return self

    def publish(self):
        '''Stop writing and copy the file to <log_root>/<regulator>/<day>/<run_id>.log.
        Returns the target path, or None if the copy failed (a log problem never fails a run).'''
        logger = get_logger()
        if self.handler is None:
            return self.target_path
        logger.removeHandler(self.handler)
        self.handler.close()
        self.handler = None
        try:
            from rcm_intelligence.common import now_sgt
            self.target_path = run_log_path(self.log_root, self.regulator_id, self.run_id, now_sgt())
            os.makedirs(os.path.dirname(self.target_path), exist_ok=True)
            shutil.copyfile(self.local_path, self.target_path)
            logger.info('Run log saved to %s', self.target_path)
        except Exception as error:
            logger.warning('Could not save the run log to the volume: %s', error)
            self.target_path = None
        return self.target_path
