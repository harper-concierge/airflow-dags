from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator

from plugins.operators.import_ga4_data import GA4ToPostgresOperator

# from airflow.operators.python import ShortCircuitOperator


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

dag = DAG(
    "19_ga4_to_postgres_dag",
    default_args=default_args,
    description="Load GA4 data into Postgres daily",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
)

start = DummyOperator(task_id="start", dag=dag)

# Import GA4 data
import_ga4_data = GA4ToPostgresOperator(
    task_id="import_ga4_data",
    property_id="{{ var.value.ga4_property_id }}",  # Set this in Airflow Variables
    postgres_conn_id="postgres_datalake_conn_id",
    google_analytics_conn_id="google_analytics_conn_id",
    destination_schema="analytics",
    destination_table="ga4_daily_metrics",
    start_date="2024-08-01",
    end_date="{{ ds }}",  # Current execution date
    dag=dag,
)

end = DummyOperator(task_id="end", dag=dag)

start >> import_ga4_data >> end
