import json
from datetime import date, datetime, timedelta
from logging import Logger, getLogger
from typing import Any, Dict, Iterator, List, Optional

from airflow import DAG
from airflow.models.dagrun import DagRun
from airflow.models.param import Param, ParamsDict
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.context import Context
from airflow.utils.state import State
from airflow.utils.types import DagRunType

DEFAULT_DAG_ID: str = "default_dag"
DEFAULT_POKE_INTERVAL: int = 60
DEFAULT_PARAMS: ParamsDict = ParamsDict(
    {
        "dag_id": Param(
            DEFAULT_DAG_ID, "DAG ID to be backfilled", type="string"
        ),
        "dag_run_params": Param(
            {},
            "Parameters to be passed to the backfilled DAG Run",
            type="object",
        ),
        "reset_dag_run": Param(
            False,
            "Whether or not clear existing dag run if already exists",
            type="boolean",
        ),
        "wait_for_completion": Param(
            False,
            "Whether or not wait for each dag run completion",
            type="boolean",
        ),
        "poke_interval": Param(
            DEFAULT_POKE_INTERVAL,
            "Poke interval to check dag run status to wait for completion",
            type="integer",
            minimum=0,
        ),
        "allowed_states": Param(
            [State.SUCCESS],
            "List of allowed states",
            type="array",
            items={"type": "string"},
        ),
        "failed_states": Param(
            [State.FAILED],
            "List of failed or dis-allowed states",
            type="array",
            items={"type": "string"},
        ),
        "start_date": Param(
            None,
            "start date of backfill to date range",
            type=["null", "string"],
            format="date",
        ),
        "end_date": Param(
            None,
            "start date of backfill to date range",
            type=["null", "string"],
            format="date",
        ),
        "dag_schedule_interval": Param(
            timedelta(days=1),
            "Schedule interval of the backfilled DAG",
            type="string",
            format="duration",
        ),
        "backfill_dates": Param(
            [],
            "List of dates to backfill",
            type="array",
            uniqueItems=True,
            items={"type": "string", "format": "date"},
        ),
    },
)

logger: Logger = getLogger(__name__)


def days_ago(days: int):
    return datetime.combine(
        date.today() - timedelta(days=days),
        datetime.min.time(),
    )


class BackfillOperator(TriggerDagRunOperator):
    ui_color: str = "#222222"
    ui_fgcolor: str = "#dddddd"

    def __init__(self, **kwargs):
        super().__init__(trigger_dag_id=DEFAULT_DAG_ID, **kwargs)

    @staticmethod
    def date_range(
        start_date: datetime,
        end_date: datetime = datetime.now(),
        interval: timedelta = timedelta(days=1),
    ) -> Iterator[datetime]:
        date_diff: timedelta = end_date - start_date
        microseconds_diff: int = int(date_diff.total_seconds() * 1e6)
        microseconds_interval: int = int(interval.total_seconds() * 1e6)
        for i in range(0, microseconds_diff + 1, microseconds_interval):
            yield start_date + timedelta(microseconds=i)

    @staticmethod
    def construct_backfill_dates(
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        backfill_dates: List[datetime] = [],
        dag_schedule_interval: timedelta = timedelta(days=1),
    ) -> List[datetime]:
        if not any([all([start_date, end_date]), backfill_dates]):
            raise ValueError(
                "Either both start_date and end_date "
                "or backfill_date cannot be empty"
            )
        if all(start_date, end_date):
            date_range: List[datetime] = list(
                date_range(start_date, end_date, dag_schedule_interval)
            )
            backfill_dates.extend(date_range)
        backfill_dates = list(set(backfill_dates))
        backfill_dates.sort()
        return backfill_dates

    def execute(self, context: Context):
        params: ParamsDict = context["params"]

        logger.debug(
            "Execute backfill with params %s",
            json.dumps(params, indent=4, sort_keys=True, default=str),
        )

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
        dag_schedule_interval: timedelta = params["dag_schedule_interval"]
        backfill_dates = self.construct_backfill_dates(
            start_date, end_date, backfill_dates, dag_schedule_interval
        )

        for backfill_date in backfill_dates:
            self.execution_date = backfill_date
            self.trigger_run_id = DagRun.generate_run_id(
                DagRunType.BACKFILL_JOB, backfill_date
            )
            logger.info(
                "Backfill DAG %s for %s with params %s",
                self.trigger_dag_id,
                self.execution_date.isoformat(),
                json.dumps(self.conf, indent=4, sort_keys=True, default=str),
            )
            super().execute(context)


default_args: Dict[str, Any] = (
    {
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 5,
        "retry_exponential_backoff": True,
    },
)

with DAG(
    dag_id="backfill",
    description="DAG to backfill other DAGs",
    default_args=default_args,
    params=DEFAULT_PARAMS,
) as dag:
    start: EmptyOperator = EmptyOperator(task_id="start")
    backfill: BackfillOperator = BackfillOperator(task_id="backfill")
    end: EmptyOperator = EmptyOperator(task_id="end")
    start >> backfill >> end
