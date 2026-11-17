'''
Databricks Vector Search index on the Gold chunk table (rcm_doc_chunk).

Design (P02 6.5 and 6.6)
  1. Delta Sync index with managed embeddings: the index reads the chunk table through its
     Change Data Feed and computes the embeddings itself (embedding model from the config).
  2. Sync mode TRIGGERED: the index is updated only when the ingest job asks, after new chunks
     are written, so there is no always running sync cost.
  3. Queries are HYBRID (meaning plus keyword), with an optional filter on the regulator.

Uses the Databricks SDK (WorkspaceClient), signed in as the job or the app identity.
'''

import json
import time

from rcm_intelligence.logging_setup import get_logger
from rcm_intelligence.settings import full_name, plain_name


def _client():
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient()


def index_exists(index_name, client=None):
    '''True when the index exists.'''
    from databricks.sdk.errors import NotFound
    try:
        (client or _client()).vector_search_indexes.get_index(index_name)
        return True
    except NotFound:
        return False


def create_index_if_missing(config, client=None):
    '''Create the Delta Sync index on rcm_doc_chunk if it does not exist yet. Returns True if created.'''
    from databricks.sdk.service.vectorsearch import (DeltaSyncVectorIndexSpecRequest, EmbeddingSourceColumn,
                                                     PipelineType, VectorIndexType)
    client = client or _client()
    index_name = plain_name(config, 'indexes', 'doc_chunk')
    if index_exists(index_name, client):
        return False
    vs = config['vector_search']
    client.vector_search_indexes.create_index(
        name=index_name,
        endpoint_name=vs['endpoint'],
        primary_key='chunk_id',
        index_type=VectorIndexType.DELTA_SYNC,
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table=plain_name(config, 'tables', 'doc_chunk'),
            pipeline_type=PipelineType.TRIGGERED,
            embedding_source_columns=[EmbeddingSourceColumn(
                name='chunk_text', embedding_model_endpoint_name=vs['embedding_endpoint'])],
            columns_to_sync=list(vs['columns_to_sync'])))
    get_logger().info('Created Vector Search index %s on endpoint %s', index_name, vs['endpoint'])
    return True


def wait_until_ready(index_name, timeout_minutes, client=None, poll_seconds=30):
    '''Wait until the index reports ready. Returns its status. Raises after the timeout.'''
    client = client or _client()
    deadline = time.time() + timeout_minutes * 60
    while True:
        status = client.vector_search_indexes.get_index(index_name).status
        if status is not None and status.ready:
            return status
        if time.time() > deadline:
            raise TimeoutError(f'Index {index_name} not ready after {timeout_minutes} minutes: '
                               f'{status.message if status else "no status"}')
        time.sleep(poll_seconds)


def sync_and_validate(spark, config, client=None, poll_seconds=30):
    '''Trigger a sync, then wait until the index holds as many rows as the chunk table.
    Returns (indexed rows, chunk rows). Raises after the timeout; the index then keeps serving
    its previous content.'''
    client = client or _client()
    logger = get_logger()
    index_name = plain_name(config, 'indexes', 'doc_chunk')
    timeout = float(config['vector_search'].get('sync_timeout_minutes', 60))
    wait_until_ready(index_name, timeout, client)
    chunk_rows = spark.table(full_name(config, 'tables', 'doc_chunk')).count()
    client.vector_search_indexes.sync_index(index_name)
    logger.info('Sync started for %s, %s chunk rows to match', index_name, chunk_rows)

    deadline = time.time() + timeout * 60
    indexed = None
    while time.time() < deadline:
        time.sleep(poll_seconds)
        status = client.vector_search_indexes.get_index(index_name).status
        indexed = status.indexed_row_count if status else None
        if status is not None and status.ready and indexed == chunk_rows:
            logger.info('Index %s ready: %s indexed rows, %s chunk rows', index_name, indexed, chunk_rows)
            return indexed, chunk_rows
    raise RuntimeError(f'Index {index_name} has {indexed} rows after {timeout:g} minutes, but the chunk table '
                       f'has {chunk_rows}. The previous index content keeps serving; check the sync pipeline.')


SEARCH_COLUMNS = ['chunk_id', 'document_id', 'version', 'regulator_id', 'item_title', 'item_date',
                  'section_title', 'page_from', 'source_url', 'chunk_text']


def search(config, question, regulator=None, top_k=8, client=None):
    '''Hybrid search on the index. Returns a list of dictionaries with the SEARCH_COLUMNS and score.'''
    client = client or _client()
    filters = json.dumps({'regulator_id': regulator.upper()}) if regulator else None
    response = client.vector_search_indexes.query_index(
        index_name=plain_name(config, 'indexes', 'doc_chunk'),
        columns=SEARCH_COLUMNS,
        query_text=question,
        query_type=config['vector_search'].get('query_type', 'HYBRID'),
        num_results=int(top_k),
        filters_json=filters)
    names = [c.name for c in (response.manifest.columns or [])]
    rows = (response.result.data_array if response.result else None) or []
    return [dict(zip(names, row)) for row in rows]
