'''
RunGuard: stops two runs of the same stage and regulator from running at the same time,
and closes runs that stopped without writing their final RUN row.

The job setting max_concurrent_runs = 1 is the first protection. This guard also covers
manual runs, a second job pointing at the same tables, and crashed runs.
'''

from datetime import timedelta

from rcm_scraper.audit import AuditWriter
from rcm_scraper.common import ABANDONED, now_sgt
from rcm_scraper.logging_setup import get_logger


class RunGuard:

    def __init__(self, spark, audit_table, regulator_id, stage, stale_after_hours):
        self.spark = spark
        self.audit_table = audit_table
        self.regulator_id = regulator_id
        self.stage = stage
        self.stale_after = timedelta(hours=float(stale_after_hours))

    def open_runs(self):
        '''Runs with a RUNNING row and no final row.'''
        rows = self.spark.sql(f'''
            SELECT run_id, min(event_time_sgt) AS started_sgt
            FROM {self.audit_table}
            WHERE record_type = 'RUN' AND stage = :stage AND regulator_id = :regulator_id
            GROUP BY run_id
            HAVING max(CASE WHEN status IN ('SUCCESS', 'PARTIAL_SUCCESS', 'FAILED', 'ABANDONED')
                            THEN 1 ELSE 0 END) = 0
        ''', args={'stage': self.stage, 'regulator_id': self.regulator_id}).collect()
        return [(row['run_id'], row['started_sgt']) for row in rows]

    def close_stale(self, closed_by):
        '''Mark open runs older than stale_after as ABANDONED.
        Returns (closed run ids, runs that are still open and recent).'''
        logger = get_logger()
        now = now_sgt()
        closed, recent = [], []
        for run_id, started in self.open_runs():
            if run_id == closed_by:
                continue
            if started is not None and now - started < self.stale_after:
                recent.append((run_id, started))
                continue
            logger.warning('Closing abandoned %s run %s started %s', self.stage, run_id, started)
            writer = AuditWriter(self.spark, self.audit_table, run_id, self.regulator_id, self.stage)
            writer.add('RUN', ABANDONED, start_time_sgt=started, end_time_sgt=now,
                       error_category='ABANDONED_RUN',
                       error_details=f'No final status after {self.stale_after}. Closed by {closed_by}.')
            writer.flush()
            closed.append(run_id)
        return closed, recent

    def check_can_start(self, current_run_id):
        '''Call before writing the RUNNING row of a new run.'''
        _, recent = self.close_stale(closed_by=current_run_id)
        if recent:
            listed = ', '.join(f'{run_id} (started {started})' for run_id, started in recent)
            raise RuntimeError(f'Another {self.stage} run for {self.regulator_id} is still in progress: '
                               f'{listed}. If it is dead it will be closed as ABANDONED after '
                               f'{self.stale_after}.')
