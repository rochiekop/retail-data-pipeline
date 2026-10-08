"""Daily Orders: Bronze -> Silver -> Gold facts -> Gold dimensions for one Business Date."""

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

# Runs fire at midnight Indonesia Western Time (WIB, Asia/Jakarta). The Business Date is the logical date's
# calendar day in that zone: plain {{ ds }} is the UTC day, which is the day before (00:00 WIB = 17:00 UTC).
# A UTC-midnight logical date such as `airflow dags test daily_orders 2017-11-24` is 07:00 WIB, the same day.
# The Business Date still selects Orders by their purchase timestamp as stored (Sao Paulo local time, CONTEXT.md).
TIMEZONE = "Asia/Jakarta"
BUSINESS_DATE = "{{ logical_date.in_timezone('" + TIMEZONE + "').strftime('%Y-%m-%d') }}"
RUN_STEP = "/home/airflow/pipeline-venv/bin/python -m pipeline.run --business-date " + BUSINESS_DATE + " --step "


@dag(
    dag_id="daily_orders",
    schedule="@daily",
    start_date=pendulum.datetime(2016, 9, 4, tz=TIMEZONE),
    end_date=pendulum.datetime(2018, 10, 17, tz=TIMEZONE),
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
