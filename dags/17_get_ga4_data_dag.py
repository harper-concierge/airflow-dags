import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.external_task import ExternalTaskSensor

from plugins.utils.load_sheet_configs import load_sheet_configs
from plugins.utils.send_harper_slack_notification import send_harper_failure_notification

from plugins.operators.ga4_to_google_sheet_operator import GA4ToGoogleSheetOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),  # Updated to start from Jan 2023
    "schedule_interval": "@daily",
    "depends_on_past": True,
    "retry_delay": timedelta(minutes=5),
    "retries": 3,  # Added retries
    "on_failure_callback": send_harper_failure_notification,
}


def is_latest_dagrun(**kwargs):
    # Implement the logic to check if this is the latest DAG run
    # Return True if it's the latest, False otherwise
    pass


with DAG(
    "ga4_to_google_sheets",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    template_searchpath="/usr/local/airflow/dags",
) as dag:

    start = DummyOperator(task_id="start")

    is_latest_dagrun_task = ShortCircuitOperator(
        task_id="skip_check",
        python_callable=is_latest_dagrun,
        depends_on_past=False,
    )

    wait_for_migrations = ExternalTaskSensor(
        task_id="wait_for_migrations_to_complete",
        external_dag_id="10_mongo_migrations_dag",
        external_task_id=None,
        allowed_states=["success"],
    )

    sheets_abspath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sheets")
    sheets_configs = load_sheet_configs(sheets_abspath)

    ga4_tasks = []
    for config in sheets_configs:
        task = GA4ToGoogleSheetOperator(
            task_id=f"ga4_to_sheets_{config['table']}",
            property_id="347125794",  # GA4 property ID
            spreadsheet_id=config["spreadsheet_id"],
            worksheet=config["worksheet"],
            google_conn_id="google_sheet_account",
            start_date="{{ ds }}",
            end_date="{{ ds }}",
        )
        ga4_tasks.append(task)

    end = DummyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    start >> is_latest_dagrun_task >> wait_for_migrations >> ga4_tasks >> end
