import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.sensors.external_task import ExternalTaskSensor

from plugins.utils.load_sheet_configs import load_sheet_configs
from plugins.utils.is_latest_active_dagrun import is_latest_dagrun
from plugins.utils.send_harper_slack_notification import send_harper_failure_notification

from plugins.operators.postgres_to_google_sheet_operator import PostgresToGoogleSheetOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 7, 14),
    "schedule_interval": "@daily",
    "depends_on_past": False,
    "retry_delay": timedelta(minutes=5),
    "retries": 0,
    "on_failure_callback": [send_harper_failure_notification()],
}


dag = DAG(
    "60_create_google_sheets_dag",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,  # This ensures sequential execution
    template_searchpath="/usr/local/airflow/dags",
)

wait_for_task = ExternalTaskSensor(
    task_id="wait_for_reports_to_complete",
    external_dag_id="55_create_reports_dag",  # The ID of the DAG you're waiting for
    external_task_id=None,  # Set to None to wait for the entire DAG to complete
    allowed_states=["success"],  # You might need to customize this part
    dag=dag,
)

is_latest_dagrun_task = ShortCircuitOperator(
    task_id="skip_check",
    pool="sql_single_thread_pool",
    python_callable=is_latest_dagrun,
    depends_on_past=False,
    dag=dag,
)


sheets = "sheets"
sheets_abspath = os.path.join(os.path.dirname(os.path.abspath(__file__)), sheets)

sheets_configs = load_sheet_configs(sheets_abspath)

# Initialize an array to hold all tasks in the current group
sheet_tasks = []

for config in sheets_configs:
    id = config["table"]
    task = PostgresToGoogleSheetOperator(
        task_id=id,
        postgres_conn_id="postgres_datalake_conn_id",
        google_conn_id="google_sheet_account",
        schema="public",
        table=config["table"],
        spreadsheet_id=config["spreadsheet_id"],
        worksheet=config["worksheet"],
        dag=dag,
    )
    # Add the current task to the array
    sheet_tasks.append(task)

wait_for_task >> is_latest_dagrun_task
is_latest_dagrun_task >> sheet_tasks
