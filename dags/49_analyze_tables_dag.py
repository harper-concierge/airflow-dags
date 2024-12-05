from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.utils.send_harper_slack_notification import send_harper_failure_notification

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 7, 14),
    "schedule_interval": "@daily",
    "depends_on_past": False,
    "retry_delay": timedelta(minutes=5),
    "retries": 0,
    "on_failure_callback": [send_harper_failure_notification()],
}

sql_type = "indexes"

dag = DAG(
    "49_analyze_tables_dag",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,  # This ensures sequential execution
    template_searchpath="/usr/local/airflow/dags",
)

wait_for_task = ExternalTaskSensor(
    task_id="wait_for_indexes_to_complete",
    external_dag_id="45_create_indexes_dag",  # The ID of the DAG you're waiting for
    external_task_id=None,  # Set to None to wait for the entire DAG to complete
    allowed_states=["success"],  # You might need to customize this part
    dag=dag,
)

analyze_all_tables = PostgresOperator(
    task_id="analyze_all_tables",
    postgres_conn_id="postgres_datalake_conn_id",
    sql="ANALYZE;",
    dag=dag,
)

wait_for_task >> analyze_all_tables
