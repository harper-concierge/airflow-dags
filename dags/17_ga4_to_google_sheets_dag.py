import os
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.external_task import ExternalTaskSensor

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
    "17_ga4_to_google_sheets_dag",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    template_searchpath="/usr/local/airflow/dags",
) as dag:

    start = EmptyOperator(task_id="start")

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

    # Path to the ga4_sheets folder
    ga4_sheets_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ga4_sheets")

    # Path to the specific JSON file
    ga4_config_path = os.path.join(ga4_sheets_dir, "ga4_config.json")

    # Read the JSON file
    with open(ga4_config_path, "r") as config_file:
        ga4_config = json.load(config_file)

    # Create the GA4 task
    ga4_task = GA4ToGoogleSheetOperator(
        task_id="17_ga4_to_google_sheets_dag",
        property_id="347125794",  # GA4 property ID
        spreadsheet_id=ga4_config["spreadsheet_id"],
        worksheet=ga4_config["worksheet"],
        google_conn_id="google_sheet_account",
        start_date="{{ ds }}",
        end_date="{{ ds }}",
        dag=dag,
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    start >> is_latest_dagrun_task >> wait_for_migrations >> ga4_task >> end
