from datetime import timedelta

from airflow import DAG
from airflow.models import Variable

# from airflow.utils.timezone import datetime as airflow_datetime  # Add this import
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.external_task import ExternalTaskSensor

# from plugins.utils.calculate_start_date import get_days_ago_start_date
from plugins.utils.is_latest_active_dagrun import is_latest_dagrun
from plugins.utils.found_records_to_process import found_records_to_process
from plugins.utils.send_harper_slack_notification import send_harper_failure_notification

from plugins.operators.drop_table import DropPostgresTableOperator
from plugins.operators.import_ga4_data import GA4ToPostgresOperator
from plugins.operators.ensure_missing_columns import EnsureMissingPostgresColumnsOperator
from plugins.operators.ensure_datalake_table_exists import EnsurePostgresDatalakeTableExistsOperator
from plugins.operators.ensure_datalake_table_view_exists import EnsurePostgresDatalakeTableViewExistsOperator
from plugins.operators.append_transient_table_data_operator import AppendTransientTableDataOperator

ga4_property_id = Variable.get("GA4_PROPERTY_ID", "")  # caps because its a variable name


rebuild = Variable.get("REBUILD_GA4_DATA", "False").lower() in ["true", "1", "yes"]
start_date = Variable.get("GA4_START_DATE", "2024-08-01")


def reset_rebuild_var():
    Variable.set("REBUILD_GA4_DATA", "False")


default_args = {
    "owner": "airflow",
    "start_date": start_date,
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
    google_conn_id="ga4_api_data",
    ga4_property_id=ga4_property_id,
    postgres_conn_id="postgres_datalake_conn_id",
    destination_schema="transient_data",
    destination_table="ga4__daily_metrics",
    rebuild=rebuild,
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
    primary_key_template="""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = '{{ destination_table }}_idx'
        ) THEN
            ALTER TABLE {{ destination_schema }}.{{ destination_table }}
            ADD CONSTRAINT {{ destination_table }}_idx PRIMARY KEY (id);
        END IF;
    END $$;
    """,
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

drop_ga4_transient_table = DropPostgresTableOperator(
    task_id="drop_ga4_transient_table",
    postgres_conn_id="postgres_datalake_conn_id",
    schema="transient_data",
    table="ga4__daily_metrics",
    depends_on_past=False,
    dag=dag,
)

drop_ga4_public_table = DropPostgresTableOperator(
    task_id="drop_ga4_public_table",
    postgres_conn_id="postgres_datalake_conn_id",
    schema="public",
    table="raw__ga4__daily_metrics",
    cascade=True,
    depends_on_past=False,
    skip=not rebuild,
    dag=dag,
)

reset_rebuild_var_task = PythonOperator(
    task_id="reset_rebuild_var_task",
    depends_on_past=False,
    python_callable=reset_rebuild_var,
    dag=dag,
)
base_tables_completed = DummyOperator(task_id="base_tables_completed", dag=dag, trigger_rule=TriggerRule.NONE_FAILED)

(
    wait_for_things_to_exist
    >> is_latest_dagrun_task
    >> drop_ga4_transient_table
    >> ga4_task
    >> ga4_has_records_to_process
    >> drop_ga4_public_table  # moved to drop when new table ready to rebuild
    >> ga4_ensure_datalake_table
    >> ga4_ensure_datalake_table_columns
    >> ga4_append_transient_table_data
    >> ga4_ensure_table_view_exists
    >> base_tables_completed
    >> reset_rebuild_var_task
)
