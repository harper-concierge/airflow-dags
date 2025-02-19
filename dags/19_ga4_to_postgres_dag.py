from datetime import timedelta

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.sensors.external_task import ExternalTaskSensor

from plugins.utils.calculate_start_date import get_days_ago_start_date
from plugins.utils.is_latest_active_dagrun import is_latest_dagrun
from plugins.utils.found_records_to_process import found_records_to_process
from plugins.utils.send_harper_slack_notification import send_harper_failure_notification

from plugins.operators.import_ga4_data import GA4ToPostgresOperator
from plugins.operators.ensure_missing_columns import EnsureMissingPostgresColumnsOperator
from plugins.operators.ensure_datalake_table_exists import EnsurePostgresDatalakeTableExistsOperator
from plugins.operators.ensure_datalake_table_view_exists import EnsurePostgresDatalakeTableViewExistsOperator
from plugins.operators.append_transient_table_data_operator import AppendTransientTableDataOperator

default_args = {
    "owner": "airflow",
    "start_date": get_days_ago_start_date("STRIPE_START_DAYS_AGO", 2 * 365),
    "schedule_interval": "@daily",
    "depends_on_past": True,
    "retry_delay": timedelta(minutes=5),
    "retries": 0,
    "on_failure_callback": [send_harper_failure_notification()],
}

dag = DAG(
    "19_ga4_to_postgres_dag",
    catchup=False,
    default_args=default_args,
    start_date=get_days_ago_start_date("STRIPE_START_DAYS_AGO", 2 * 365),
    max_active_runs=1,
    template_searchpath="/usr/local/airflow/dags",
)

is_latest_dagrun_task = ShortCircuitOperator(
    task_id="skip_check",
    python_callable=is_latest_dagrun,
    depends_on_past=False,
    dag=dag,
)

wait_for_things_to_exist = ExternalTaskSensor(
    task_id="wait_for_things_to_exist",
    external_dag_id="01_ensure_things_exist",
    external_task_id=None,
    allowed_states=["success"],
    dag=dag,
)

ga4_task = GA4ToPostgresOperator(
    task_id="import_ga4_data_to_datalake",
    property_id="{{ var.value.ga4_property_id }}",
    postgres_conn_id="postgres_datalake_conn_id",
    google_analytics_conn_id="google_analytics_conn_id",
    destination_schema="transient_data",
    destination_table="ga4__daily_metrics",
    start_date=get_days_ago_start_date("STRIPE_START_DAYS_AGO", 2 * 365),
    end_date=get_days_ago_start_date("STRIPE_START_DAYS_AGO", 2 * 1),
    dag=dag,
)

task_id = "ga4_has_records_to_process"
ga4_has_records_to_process = ShortCircuitOperator(
    task_id=task_id,
    python_callable=found_records_to_process,
    op_kwargs={
        "parent_task_id": "import_ga4_data_to_datalake",
        "xcom_key": "documents_found",
    },
    dag=dag,
)

task_id = "ga4_ensure_datalake_table_exists"
ga4_ensure_datalake_table = EnsurePostgresDatalakeTableExistsOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="transient_data",
    source_table="ga4__daily_metrics",
    destination_schema="public",
    destination_table="raw__ga4__daily_metrics",
    dag=dag,
)

ga4_missing_columns_task_id = "ga4_ensure_public_columns_uptodate"
ga4_ensure_datalake_table_columns = EnsureMissingPostgresColumnsOperator(
    task_id=ga4_missing_columns_task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_table="ga4__daily_metrics",
    destination_table="raw__ga4__daily_metrics",
    dag=dag,
)

task_id = "ga4_append_to_datalake"
ga4_append_transient_table_data = AppendTransientTableDataOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="transient_data",
    source_table="ga4__daily_metrics",
    destination_schema="public",
    destination_table="raw__ga4__daily_metrics",
    dag=dag,
)

task_id = "ga4_ensure_datalake_table_view"
ga4_ensure_table_view_exists = EnsurePostgresDatalakeTableViewExistsOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="public",
    source_table="raw__ga4__daily_metrics",
    destination_schema="public",
    destination_table="ga4__daily_metrics",
    prev_task_id=ga4_missing_columns_task_id,
    append_fields=["sync_timestamp", "airflow_sync_ds"],
    prepend_fields=["date"],
    dag=dag,
)

(
    wait_for_things_to_exist
    >> is_latest_dagrun_task
    >> ga4_task
    >> ga4_has_records_to_process
    >> ga4_ensure_datalake_table
    >> ga4_ensure_datalake_table_columns
    >> ga4_append_transient_table_data
    >> ga4_ensure_table_view_exists
)
