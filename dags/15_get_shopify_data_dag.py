from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.external_task import ExternalTaskSensor

from plugins.utils.calculate_start_date import fixed_date_start_date
from plugins.utils.is_latest_active_dagrun import is_latest_dagrun
from plugins.utils.found_records_to_process import found_records_to_process

from plugins.operators.drop_table import DropPostgresTableOperator
from plugins.operators.analyze_table import RefreshPostgresTableStatisticsOperator
from plugins.operators.ensure_missing_columns import EnsureMissingPostgresColumnsOperator
from plugins.operators.ensure_datalake_table_exists import EnsurePostgresDatalakeTableExistsOperator
from plugins.operators.import_shopify_data_operator import ImportShopifyPartnerDataOperator
from plugins.operators.ensure_datalake_table_view_exists import EnsurePostgresDatalakeTableViewExistsOperator
from plugins.operators.append_transient_table_data_operator import AppendTransientTableDataOperator

# from plugins.utils.send_harper_slack_notification import send_harper_failure_notification


default_args = {
    "owner": "airflow",
    "start_date": fixed_date_start_date("SHOPIFY_START_DATE", datetime(2024, 1, 1)),
    "schedule_interval": "@daily",
    "depends_on_past": True,
    "retry_delay": timedelta(minutes=5),  # airflows retry mechanism
    "retries": 3,  # Increased from 0 to 3
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    # "on_failure_callback": [send_harper_failure_notification()],
}


dag = DAG(
    "15_get_shopify_data_dag",
    catchup=False,
    default_args=default_args,
    start_date=fixed_date_start_date("SHOPIFY_START_DATE", datetime(2024, 1, 1)),
    max_active_runs=1,  # This ensures sequential execution
    template_searchpath="/usr/local/airflow/dags",
)

base_tables_completed = DummyOperator(task_id="base_tables_completed", dag=dag, trigger_rule=TriggerRule.NONE_FAILED)
# is_latest_dagrun_task = DummyOperator(task_id="start", dag=dag)
doc = """
Skip the subsequent tasks if
    a) the execution_date is in past
    b) there multiple dag runs are currently active
"""
is_latest_dagrun_task = ShortCircuitOperator(
    task_id="skip_check",
    python_callable=is_latest_dagrun,
    depends_on_past=False,
    dag=dag,
)
is_latest_dagrun_task.doc = doc

wait_for_things_to_exist = ExternalTaskSensor(
    task_id="wait_for_things_to_exist",
    external_dag_id="01_ensure_things_exist",  # The ID of the DAG you're waiting for
    external_task_id=None,  # Set to None to wait for the entire DAG to complete
    allowed_states=["success"],  # You might need to customize this part
    dag=dag,
)

partners = [
    "shrimps",
    "chinti_parker",
    "beckham",
    "jigsaw",
    "rixo",
    "cefinn",
    "temperley",
    "snicholson",
    "self-portrait",
    "lestrange",
    "ro-zo",
    "kitri",
    "live-unlimited",
    "needle-thread",
    # "nobodys-child",
    "fcuk",
    # "pangaia",
    # "represent",
    # "harper_production",
]

destination_table = "shopify_partner_orders"
drop_transient_table = DropPostgresTableOperator(
    task_id="drop_shopify_partner_orders_transient_table",
    postgres_conn_id="postgres_datalake_conn_id",
    schema="transient_data",
    table=destination_table,
    dag=dag,
)

migration_tasks = []
first_task = None
for partner in partners:
    task_id = f"get_{partner}_shopify_data_task"
    shopify_task = ImportShopifyPartnerDataOperator(
        task_id=task_id,
        postgres_conn_id="postgres_datalake_conn_id",
        schema="public",
        destination_schema="transient_data",
        destination_table=destination_table,
        partner_ref=partner,
        dag=dag,
        pool="shopify_import_pool",
    )
    # append_transient_table_data >> base_tables_completed
    # Do this so that the first task can run and create the "transient_data.destination_table"
    # and avoid a race condition whereby to df.to_sql try to create the table simultaneously.
    # Doing one first, ensure the table is created with a race condition
    # and subsequent df.to_sql will result in an append.

    if first_task:
        migration_tasks.append(shopify_task)
    else:
        first_task = shopify_task

previous_task_id = task_id
task_id = f"{destination_table}_has_records_to_process"
has_records_to_process = ShortCircuitOperator(
    task_id=task_id,
    python_callable=found_records_to_process,
    op_kwargs={"parent_task_id": previous_task_id, "xcom_key": "documents_found"},
)

task_id = f"{destination_table}_refresh_transient_table_stats"
refresh_transient_table = RefreshPostgresTableStatisticsOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    schema="transient_data",
    table=destination_table,
    dag=dag,
)

task_id = f"{destination_table}_ensure_datalake_table_exists"
ensure_datalake_table = EnsurePostgresDatalakeTableExistsOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="transient_data",
    source_table=destination_table,
    destination_schema="public",
    destination_table=f"raw__{destination_table}",
    dag=dag,
)

task_id = f"{destination_table}_refresh_datalake_table_stats"
refresh_datalake_table = RefreshPostgresTableStatisticsOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    schema="public",
    table=f"raw__{destination_table}",
    dag=dag,
)

missing_columns_task_id = f"{destination_table}_ensure_public_columns_uptodate"
ensure_datalake_table_columns = EnsureMissingPostgresColumnsOperator(
    task_id=missing_columns_task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_table=destination_table,
    destination_table=f"raw__{destination_table}",
    dag=dag,
)
task_id = f"{destination_table}_append_to_datalake"
append_transient_table_data = AppendTransientTableDataOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="transient_data",
    source_table=destination_table,
    destination_schema="public",
    destination_table=f"raw__{destination_table}",
    dag=dag,
)
task_id = f"{destination_table}_ensure_datalake_table_view"
ensure_table_view_exists = EnsurePostgresDatalakeTableViewExistsOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="public",
    source_table=f"raw__{destination_table}",
    destination_schema="public",
    destination_table=destination_table,
    prev_task_id=missing_columns_task_id,
    append_fields=["createdat", "updatedat", "airflow_sync_ds"],
    prepend_fields=["id"],
    dag=dag,
)

(
    wait_for_things_to_exist
    >> is_latest_dagrun_task
    >> drop_transient_table
    >> first_task
    >> migration_tasks
    >> has_records_to_process
    >> refresh_transient_table
    >> ensure_datalake_table
    >> refresh_datalake_table
    >> ensure_datalake_table_columns
    >> append_transient_table_data
    >> ensure_table_view_exists
    >> base_tables_completed
)
