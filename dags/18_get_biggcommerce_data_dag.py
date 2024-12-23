from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.external_task import ExternalTaskSensor

from plugins.utils.is_latest_active_dagrun import is_latest_dagrun
from plugins.utils.found_records_to_process import found_records_to_process

from plugins.operators.drop_table import DropPostgresTableOperator
from plugins.operators.analyze_table import RefreshPostgresTableStatisticsOperator
from plugins.operators.ensure_missing_columns import EnsureMissingPostgresColumnsOperator
from plugins.operators.ensure_datalake_table_exists import EnsurePostgresDatalakeTableExistsOperator

# Import our BigCommerce operator
from plugins.operators.import_bigcommerce_data_operator import ImportBigCommerceDataOperator
from plugins.operators.ensure_datalake_table_view_exists import EnsurePostgresDatalakeTableViewExistsOperator
from plugins.operators.append_transient_table_data_operator import AppendTransientTableDataOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "schedule_interval": "@daily",
    "depends_on_past": True,
    "retry_delay": timedelta(minutes=5),
    "retries": 3,
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

dag = DAG(
    "get_bigcommerce_data_dag",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    template_searchpath="/usr/local/airflow/dags",
)

# Tasks
base_tables_completed = DummyOperator(task_id="base_tables_completed", dag=dag, trigger_rule=TriggerRule.NONE_FAILED)

is_latest_dagrun_task = ShortCircuitOperator(
    task_id="skip_check",
    python_callable=is_latest_dagrun,
    depends_on_past=False,
    dag=dag,
    doc="""Skip subsequent tasks if:
        a) the execution_date is in past
        b) there are multiple dag runs currently active""",
)

wait_for_things_to_exist = ExternalTaskSensor(
    task_id="wait_for_things_to_exist",
    external_dag_id="01_ensure_things_exist",
    external_task_id=None,
    allowed_states=["success"],
    dag=dag,
)

destination_table = "bigcommerce_partner_orders"

# Drop transient table
drop_transient_table = DropPostgresTableOperator(
    task_id="drop_bigcommerce_orders_transient_table",
    postgres_conn_id="postgres_datalake_conn_id",
    schema="transient_data",
    table=destination_table,
    dag=dag,
)

# Import BigCommerce data
import_bigcommerce_data = ImportBigCommerceDataOperator(
    task_id="import_bigcommerce_data",
    postgres_conn_id="postgres_datalake_conn_id",
    schema="public",
    destination_schema="transient_data",
    destination_table=destination_table,
    dag=dag,
)

# Check if records were processed
has_records_to_process = ShortCircuitOperator(
    task_id=f"{destination_table}_has_records_to_process",
    python_callable=found_records_to_process,
    op_kwargs={"parent_task_id": "import_bigcommerce_data", "xcom_key": "documents_found"},
    dag=dag,
)

# Refresh transient table statistics
refresh_transient_table = RefreshPostgresTableStatisticsOperator(
    task_id=f"{destination_table}_refresh_transient_table_stats",
    postgres_conn_id="postgres_datalake_conn_id",
    schema="transient_data",
    table=destination_table,
    dag=dag,
)

# Ensure datalake table exists
ensure_datalake_table = EnsurePostgresDatalakeTableExistsOperator(
    task_id=f"{destination_table}_ensure_datalake_table_exists",
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="transient_data",
    source_table=destination_table,
    destination_schema="public",
    destination_table=f"raw__{destination_table}",
    dag=dag,
)

# Refresh datalake table statistics
refresh_datalake_table = RefreshPostgresTableStatisticsOperator(
    task_id=f"{destination_table}_refresh_datalake_table_stats",
    postgres_conn_id="postgres_datalake_conn_id",
    schema="public",
    table=f"raw__{destination_table}",
    dag=dag,
)

# Ensure columns are up to date
ensure_datalake_table_columns = EnsureMissingPostgresColumnsOperator(
    task_id=f"{destination_table}_ensure_public_columns_uptodate",
    postgres_conn_id="postgres_datalake_conn_id",
    source_table=destination_table,
    destination_table=f"raw__{destination_table}",
    dag=dag,
)

# Append transient data
append_transient_table_data = AppendTransientTableDataOperator(
    task_id=f"{destination_table}_append_to_datalake",
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="transient_data",
    source_table=destination_table,
    destination_schema="public",
    destination_table=f"raw__{destination_table}",
    dag=dag,
)

# Ensure table view exists
ensure_table_view_exists = EnsurePostgresDatalakeTableViewExistsOperator(
    task_id=f"{destination_table}_ensure_datalake_table_view",
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="public",
    source_table=f"raw__{destination_table}",
    destination_schema="public",
    destination_table=destination_table,
    prev_task_id=f"{destination_table}_ensure_public_columns_uptodate",
    append_fields=["createdat", "updatedat", "airflow_sync_ds"],
    prepend_fields=["id"],
    dag=dag,
)

# Set up task dependencies
(
    wait_for_things_to_exist
    >> is_latest_dagrun_task
    >> drop_transient_table
    >> import_bigcommerce_data
    >> has_records_to_process
    >> refresh_transient_table
    >> ensure_datalake_table
    >> refresh_datalake_table
    >> ensure_datalake_table_columns
    >> append_transient_table_data
    >> ensure_table_view_exists
    >> base_tables_completed
)
