import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

from plugins.utils.load_slack_configs import load_slack_configs
from plugins.utils.send_harper_slack_notification import send_harper_failure_notification

from plugins.operators.sql_to_slack_operator import SqlToSlackWebhookOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 7, 14),
    "depends_on_past": False,
    "retry_delay": timedelta(minutes=5),
    "retries": 0,
    "on_failure_callback": send_harper_failure_notification,  # Removed brackets for the callback
}

dag = DAG(
    "70_send_slack_messages",
    catchup=False,
    default_args=default_args,
    schedule_interval="@daily",
    max_active_runs=1,  # Ensures sequential execution
    template_searchpath=["/usr/local/airflow/dags/slack"],
)

# Task: Wait for External Task to Complete
wait_for_task = ExternalTaskSensor(
    task_id="wait_for_reports_to_complete",
    external_dag_id="55_create_reports_dag",  # The ID of the DAG you're waiting for
    external_task_id=None,  # Set to None to wait for the entire DAG to complete
    allowed_states=["success"],  # States to trigger this sensor
    dag=dag,
)

# Directory where Slack configurations are stored
slack = "slack"
slack_abspath = os.path.join(os.path.dirname(os.path.abspath(__file__)), slack)

# Load Slack Configurations
slack_configs = load_slack_configs(slack_abspath)

# Initialize a list to store dynamically created tasks
slack_tasks = []

# Loop through configurations and create tasks
for config in slack_configs:
    id = config["id"]  # Changed to "directory" based on updated function
    task = SqlToSlackWebhookOperator(
        task_id=f"send_slack_message_{id}",
        sql_conn_id="postgres_datalake_conn_id",
        sql=config["query_file"],  # Path to query file
        slack_conn_id="slack_api_default",
        slack_config=config["slack_file"],  # Slack JSON configuration file
        dag=dag,
    )
    slack_tasks.append(task)

# Define task dependencies
wait_for_task >> slack_tasks
