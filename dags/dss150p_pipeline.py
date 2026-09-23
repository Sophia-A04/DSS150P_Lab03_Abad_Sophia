from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator


PROJECT = "/opt/airflow/project"

# Airflow run IDs contain characters such as ":" and "+".
# Replace them so the same run ID can safely be used in
# run-specific directories on the Windows-mounted project.
PIPELINE_RUN_ID = "{{ run_id | replace(':', '_') | replace('+', '_') }}"


def failure_callback(context):
    task_instance = context.get("task_instance")
    exception = context.get("exception")

    print("AIRFLOW TASK FAILURE")
    print(
        "dag_id:",
        task_instance.dag_id if task_instance else None,
    )
    print(
        "task_id:",
        task_instance.task_id if task_instance else None,
    )
    print(
        "run_id:",
        context.get("run_id"),
    )
    print(
        "try_number:",
        task_instance.try_number if task_instance else None,
    )
    print(
        "exception:",
        repr(exception),
    )


DEFAULT_ARGS = {
    "owner": "dss150p",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": failure_callback,
}


with DAG(
    dag_id="dss150p_sales_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        "run_mode": Param(
            "full",
            enum=["full", "partition"],
        ),
        "year": Param(
            2026,
            type="integer",
        ),
        "month": Param(
            1,
            type="integer",
            minimum=1,
            maximum=12,
        ),
    },
    tags=["DSS150P"],
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command=(
            f'cd {PROJECT} && '
            f'PIPELINE_RUN_ID="{PIPELINE_RUN_ID}" '
            f"python -m src.cli extract"
        ),
    )

    transform = BashOperator(
        task_id="transform",
        bash_command=(
            f'cd {PROJECT} && '
            f'PIPELINE_RUN_ID="{PIPELINE_RUN_ID}" '
            f"python -m src.cli transform"
        ),
    )

    load = BashOperator(
        task_id="load",
        bash_command=(
            f'cd {PROJECT} && '
            f'PIPELINE_RUN_ID="{PIPELINE_RUN_ID}" '
            f"python -m src.cli load"
        ),
    )

    validate = BashOperator(
        task_id="validate",
        bash_command=(
            f'cd {PROJECT} && '
            f'PIPELINE_RUN_ID="{PIPELINE_RUN_ID}" '
            f"python -m src.cli validate"
        ),
    )

    extract >> transform >> load >> validate