import os
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.external_task import ExternalTaskSensor

from plugins.utils.send_harper_slack_notification import send_harper_failure_notification

from plugins.operators.ga4_to_google_sheet_operator import GA4ToGoogleSheetOperator

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),  # Start date for the DAG
    "schedule_interval": "@daily",  # Daily schedule
    "depends_on_past": True,
    "retry_delay": timedelta(minutes=5),
    "retries": 3,  # Number of retries
    "on_failure_callback": [send_harper_failure_notification()],
}

# Define the DAG
dag = DAG(
    "18_ga4_to_google_sheets_dag",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    template_searchpath="/usr/local/airflow/dags",
)

# Wait for the external task to complete
wait_for_task = ExternalTaskSensor(
    task_id="wait_for_reports_to_complete",
    external_dag_id="15_get_shopify_data_dag",  # ID of the DAG to wait for
    external_task_id=None,  # Wait for the entire DAG to complete
    allowed_states=["success"],  # Only proceed if the external task is successful
    dag=dag,
)

# Define the absolute path for the GA4 config file
ga4_sheets = "ga4_sheets"
ga4_sheets_abspath = os.path.join(os.path.dirname(os.path.abspath(__file__)), ga4_sheets)
ga4_config_path = os.path.join(ga4_sheets_abspath, "ga4_conversion_data.json")

# Load the configuration file
with open(ga4_config_path, "r") as config_file:
    config = json.load(config_file)

# Create the GA4 task using the loaded configuration
ga4_task = GA4ToGoogleSheetOperator(
    task_id=config["table"],  # Use the 'table' key from the JSON file for the task_id
    property_id=config.get("property_id", "default_property_id"),  # Default value if not present
    spreadsheet_id=config["spreadsheet_id"],
    worksheet=config["worksheet"],
    google_sheets_conn_id="google_sheet_account",
    google_analytics_conn_id="google_sheet_ga4",
    # start_date=datetime(2023, 1, 1),
    end_date="{{ ds }}",
    dag=dag,
)

# Define the end task
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

# Set task dependencies
wait_for_task >> ga4_task >> end
