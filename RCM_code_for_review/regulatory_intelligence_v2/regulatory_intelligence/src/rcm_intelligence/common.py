'''
Small shared helpers: Singapore time, run identifiers, fingerprints, a retry for Delta write
conflicts, and the release information stamped on every record.
'''

import hashlib
import os
import time
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

from rcm_intelligence import __version__
from rcm_intelligence.logging_setup import get_logger

SGT = ZoneInfo('Asia/Singapore')
RELEASE_TAG_VARIABLE = 'RCM_RELEASE_TAG'


def now_sgt():
    '''Current Singapore wall clock time, without a time zone (as stored in the tables).'''
    return datetime.now(SGT).replace(tzinfo=None)


def new_run_id(prefix):
    '''Readable and unique run identifier, for example copy_20261117_101500_a1b2c3.'''
    return f'{prefix}_{now_sgt().strftime("%Y%m%d_%H%M%S")}_{uuid.uuid4().hex[:6]}'


def sha256_bytes(data):
    '''Fingerprint (SHA 256) of some bytes.'''
    return hashlib.sha256(data).hexdigest()


def sha256_text(text):
    '''Fingerprint of a text. Used to store a question as a hash, never as plain text.'''
    return sha256_bytes((text or '').encode('utf8'))


def release_tag():
    '''Release (Git tag) that produced a record. Set on the cluster and the app by the bundle.'''
    return os.environ.get(RELEASE_TAG_VARIABLE) or 'unreleased'


def agent_version():
    '''Version of this package, stamped on every analysis record.'''
    return __version__


def current_trace_id():
    '''Identifier of the active MLflow trace, or None when tracing is not active.'''
    try:
        import mlflow
        span = mlflow.get_current_active_span()
        return span.trace_id if span is not None else None
    except Exception:
        return None


def run_sql_with_retry(spark, statement, args=None, what='sql', attempts=4, base_delay_seconds=3):
    '''Run a SQL statement. Retries only Delta concurrent write conflicts (two jobs writing the
    same table at the same moment). Any other error is raised at once.'''
    logger = get_logger()
    for attempt in range(1, attempts + 1):
        try:
            return spark.sql(statement, args=args or {})
        except Exception as error:
            text = f'{type(error).__name__} {error}'
            if not ('Concurrent' in text or 'DELTA_CONCURRENT' in text) or attempt == attempts:
                raise
            delay = base_delay_seconds * attempt
            logger.warning('Delta conflict on %s, attempt %s of %s, retrying in %ss', what, attempt, attempts, delay)
            time.sleep(delay)
