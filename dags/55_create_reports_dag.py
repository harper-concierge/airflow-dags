from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

from plugins.utils.run_dynamic_sql_task import run_dynamic_sql_task
from plugins.utils.send_harper_slack_notification import send_harper_failure_notification
from plugins.utils.get_recursive_entities_for_sql_types import get_recursive_entities_for_sql_types

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

run_dynamic_sql_task(
    dag,
    wait_for_task,
    sql_type,
    add_table_columns_to_context=add_table_columns_to_context,
)
