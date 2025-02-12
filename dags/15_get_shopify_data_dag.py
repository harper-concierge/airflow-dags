import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.external_task import ExternalTaskSensor

# from plugins.utils.calculate_start_date import get_days_ago_start_date
from plugins.utils.is_latest_active_dagrun import is_latest_dagrun

from plugins.operators.drop_table import DropPostgresTableOperator
from plugins.operators.analyze_table import RefreshPostgresTableStatisticsOperator
from plugins.operators.ensure_missing_columns import EnsureMissingPostgresColumnsOperator
from plugins.operators.shopify_graphql_operator import ShopifyGraphQLPartnerDataOperator
from plugins.operators.ensure_datalake_table_exists import EnsurePostgresDatalakeTableExistsOperator
from plugins.operators.delete_from_postgres_operator import DeleteFromPostgresOperator
from plugins.operators.ensure_datalake_table_view_exists import EnsurePostgresDatalakeTableViewExistsOperator
from plugins.operators.append_transient_table_data_operator import AppendTransientTableDataOperator

# from plugins.utils.found_records_to_process import found_records_to_process


destination_table = "shopify_partner_orders"

rebuild = Variable.get("REBUILD_SHOPIFY_DATA", "False").lower() in ["true", "1", "yes"]

# Get SHOPIFY_START_DATE from environment variable with a fallback
SHOPIFY_START_DATE = os.getenv("SHOPIFY_START_DATE", "2024-01-01")

# Convert string to datetime
try:
    start_date = datetime.strptime(SHOPIFY_START_DATE, "%Y-%m-%d")
except ValueError:
    # Fallback to 90 days ago if env var is invalid
    start_date = "2024-01-01"

# Shopify partners env variable is a comma separated list of partner names

enabled_partners = Variable.get("SHOPIFY_PARTNERS", "harper_production,shrimps").split(",")
partners = [p.strip() for p in enabled_partners] or ["harper_production"]


def reset_rebuild_var():
    Variable.set("REBUILD_SHOPIFY_DATA", "False")


default_args = {
    "owner": "airflow",
    "start_date": start_date,
    "schedule_interval": "@daily",
    "depends_on_past": True,
    "retry_delay": timedelta(minutes=5),
    "retries": 2,
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

dag = DAG(
    "15_get_shopify_data_dag",
    catchup=False,
    default_args=default_args,
    start_date=start_date,
    max_active_runs=1,
    template_searchpath="/usr/local/airflow/dags",
)

base_tables_completed = DummyOperator(task_id="base_tables_completed", dag=dag, trigger_rule=TriggerRule.NONE_FAILED)

reset_rebuild_var_task = PythonOperator(
    task_id="reset_rebuild_var_task",
    depends_on_past=False,
    python_callable=reset_rebuild_var,
    dag=dag,
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


"""
destination_table = "shopify_partner_orders"
drop_transient_table = DropPostgresTableOperator(
    task_id="drop_shopify_partner_orders_transient_table",
    postgres_conn_id="postgres_datalake_conn_id",
    schema="transient_data",
    table=destination_table,
    dag=dag,
)"""


drop_shopify_partner_orders_transient_table = DropPostgresTableOperator(
    task_id="drop_shopify_partner_orders_transient_table",
    postgres_conn_id="postgres_datalake_conn_id",
    schema="transient_data",
    table="shopify_partner_orders",
    depends_on_past=False,
    skip=not rebuild,
    dag=dag,
)

drop_shopify_partner_orders_public_table = DropPostgresTableOperator(
    task_id="drop_shopify_partner_orders_public_table",
    postgres_conn_id="postgres_datalake_conn_id",
    schema="public",
    table="raw__shopify_partner_orders",
    cascade=True,
    skip=not rebuild,
    depends_on_past=False,
    dag=dag,
)

# Create tasks for each partner
migration_tasks = []
first_task = None
for partner in partners:
    task_id = f"get_{partner}_shopify_data_task"
    shopify_task = ShopifyGraphQLPartnerDataOperator(
        task_id=task_id,
        postgres_conn_id="postgres_datalake_conn_id",
        schema="public",
        destination_schema="transient_data",
        destination_table=destination_table,
        partner_ref=partner,
        rebuild=rebuild,
        dag=dag,
        pool="shopify_import_pool",
    )

    if first_task:
        migration_tasks.append(shopify_task)
    else:
        first_task = shopify_task


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

delete_task = DeleteFromPostgresOperator(
    task_id="delete_from_datalake",
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="transient_data",
    source_table="shopify_partner_orders",
    destination_schema="public",
    destination_table="raw__shopify_partner_orders",
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
    >> drop_shopify_partner_orders_transient_table
    >> drop_shopify_partner_orders_public_table
    >> first_task
    >> migration_tasks
    >> refresh_transient_table
    >> ensure_datalake_table
    >> refresh_datalake_table
    >> ensure_datalake_table_columns
    >> delete_task
    >> append_transient_table_data
    >> ensure_table_view_exists
    >> base_tables_completed
    >> reset_rebuild_var_task
)
