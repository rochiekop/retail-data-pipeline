"""Daily Orders: Bronze -> Silver -> Gold facts -> Gold dimensions for one Business Date."""

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

# {{ ds }} is the logical date's UTC calendar day. Scheduled runs fall at Sao Paulo midnight (02:00 or 03:00
# UTC, the same day), so it is the Business Date; it also lets `airflow dags test daily_orders 2017-11-24` mean that day.
RUN_STEP = "/home/airflow/pipeline-venv/bin/python -m pipeline.run --business-date {{ ds }} --step "


@dag(
    dag_id="daily_orders",
    schedule="@daily",
    start_date=pendulum.datetime(2016, 9, 4, tz="America/Sao_Paulo"),
    end_date=pendulum.datetime(2018, 10, 17, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=4,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=2)},
)
def daily_orders():
    bronze = BashOperator(task_id="bronze", bash_command=RUN_STEP + "bronze")
    silver = BashOperator(task_id="silver", bash_command=RUN_STEP + "silver")
    facts = BashOperator(task_id="facts", bash_command=RUN_STEP + "facts")
    # Dimensions and report views are rebuilt whole; the 1-slot pool stops parallel runs colliding.
    dimensions = BashOperator(task_id="dimensions", bash_command=RUN_STEP + "dimensions", pool="dimensions")
    bronze >> silver >> facts >> dimensions


daily_orders()
