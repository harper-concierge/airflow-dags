from datetime import datetime, timedelta

from airflow import DAG
from horizontal_reports.config import HORIZONTAL_REPORT_CONFIGS
from airflow.sensors.external_task import ExternalTaskSensor

from plugins.utils.run_dynamic_sql_task import run_dynamic_sql_task
from plugins.utils.send_harper_slack_notification import send_harper_failure_notification
from plugins.utils.get_recursive_entities_for_sql_types import get_recursive_entities_for_sql_types

from plugins.operators.horizontal_report_operator import HorizontalReportOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 7, 14),
    "schedule_interval": "@daily",
    "depends_on_past": False,
    "retry_delay": timedelta(minutes=5),
    "retries": 0,
}

sql_type = "reports"
add_table_columns_to_context = get_recursive_entities_for_sql_types(["dimensions", "cleansers"])

dag = DAG(
    f"55_create_{sql_type}_dag",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,  # This ensures sequential execution
    template_searchpath="/usr/local/airflow/dags",
)

# dag.user_defined_filters = {"prefix_columns": prefix_columns}

wait_for_task = ExternalTaskSensor(
    task_id="wait_for_cleansers_to_complete",
    external_dag_id="50_create_cleansers_dag",  # The ID of the DAG you're waiting for
    external_task_id=None,  # Set to None to wait for the entire DAG to complete
    allowed_states=["success"],  # You might need to customize this part
    on_failure_callback=[send_harper_failure_notification()],
    dag=dag,
)

# Run all reports using dynamic SQL task
reports_task = run_dynamic_sql_task(
    dag,
    wait_for_task,
    sql_type,
    add_table_columns_to_context=add_table_columns_to_context,
)

# RUN HORIZONTAL REPORTS (Dates as fields)

# Settings for horizontal reports
HORIZONTAL_REPORT_SETTINGS = {
    "postgres_conn_id": "postgres_datalake_conn_id",
    "source_schema": "public",
    "destination_schema": "public",
}

# Create horizontal reports sequentially
previous_task = reports_task
for config in HORIZONTAL_REPORT_CONFIGS:
    # Merge common settings with report-specific config
    task_config = {**HORIZONTAL_REPORT_SETTINGS, **config}

    task = HorizontalReportOperator(
        task_id=task_config["task_id"],
        postgres_conn_id=task_config["postgres_conn_id"],
        source_schema=task_config["source_schema"],
        source_table=task_config["source_table"],
        destination_schema=task_config["destination_schema"],
        destination_table=task_config["destination_table"],
        date_column=task_config["date_column"],
        metric_columns=task_config["metric_columns"],
        group_columns=task_config["group_columns"],
        on_failure_callback=[send_harper_failure_notification()],
        dag=dag,
    )

    # Set up sequential dependencies
    previous_task >> task
    previous_task = task
