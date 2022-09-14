from datetime import date, datetime, timedelta
from logging import Logger, getLogger
from typing import Callable, List, Optional, Set
from airflow.models.param import ParamsDict
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.context import Context
from airflow.utils.types import DagRunType
from airflow.models.dagrun import DagRun

DEFAULT_DAG_ID: str = "default_dag"

logger: Logger = getLogger(__name__)


class BackfillOperator(TriggerDagRunOperator):
    def __init__(self, **kwargs):
        super().__init__(trigger_dag_id=DEFAULT_DAG_ID, **kwargs)
    
    @staticmethod
    def construct_backfill_dates(
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        backfill_dates: Optional[List[datetime]] = None,
    ) -> List[datetime]:
        if all([start_date, backfill_dates]):
            raise ValueError("Either start_date or backfill_date need to be filled")
        if end_date is None:
            end_date = datetime.combine(date.today(), datetime.min.time())

        date_range: List[datetime] = [
            start_date + timedelta(days=i)
            for i in range((end_date - start_date).days + 1)
        ]
        backfill_dates.extend(date_range)
        backfill_dates.sort()
        seen: Set[datetime] = set()
        seen_add: Callable[[datetime], None] = seen.add
        backfill_dates = [
            backfill_date
            for backfill_date in backfill_dates
            if not (backfill_date in seen or seen_add(backfill_date))
        ]
        return backfill_dates

    def execute(self, context: Context):
        params: ParamsDict = context["params"]

        self.trigger_dag_id = params["dag_id"]
        self.conf = params["dag_run_params"]
        self.reset_dag_run = params["reset_dag_run"]
        self.wait_for_completion = params["wait_for_completion"]
        self.poke_interval = params["poke_interval"]
        self.allowed_states = params["allowed_states"]
        self.failed_states = params["failed_states"]

        start_date: Optional[datetime] = params["start_date"]
        end_date: Optional[datetime] = params["end_date"]
        backfill_dates: Optional[List[datetime]] = params["backfill_dates"]
        backfill_dates = self.construct_backfill_dates(start_date, end_date, backfill_dates)

        for backfill_date in backfill_dates:
            self.execution_date = backfill_date
            self.trigger_run_id = params["run_id"] or DagRun.generate_run_id(DagRunType.BACKFILL_JOB, backfill_date)
            super().execute(context)
