from datetime import timedelta

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.sensors.external_task import ExternalTaskSensor

from plugins.utils.calculate_start_date import get_days_ago_start_date
from plugins.utils.is_latest_active_dagrun import is_latest_dagrun
from plugins.utils.found_records_to_process import found_records_to_process
from plugins.utils.send_harper_slack_notification import send_harper_failure_notification

from plugins.operators.ensure_missing_columns import EnsureMissingPostgresColumnsOperator
from plugins.operators.stripe_to_postgres_operator import StripeToPostgresOperator
from plugins.operators.ensure_datalake_table_exists import EnsurePostgresDatalakeTableExistsOperator
from plugins.operators.ensure_datalake_table_view_exists import EnsurePostgresDatalakeTableViewExistsOperator
from plugins.operators.zettle_finance_to_postgres_operator import ZettleFinanceToPostgresOperator
from plugins.operators.append_transient_table_data_operator import AppendTransientTableDataOperator
from plugins.operators.zettle_purchases_to_postgres_operator import ZettlePurchasesToPostgresOperator

# from airflow.sensors.external_task import ExternalTaskSensor


default_args = {
    "owner": "airflow",
    "start_date": get_days_ago_start_date("ZETTLE_START_DAYS_AGO", 2 * 365),
    "schedule_interval": "@daily",
    "depends_on_past": True,
    "retry_delay": timedelta(minutes=5),
    "retries": 0,
    "on_failure_callback": [send_harper_failure_notification()],
}


dag = DAG(
    "20_import_financial_transactions_dag",
    catchup=False,
    default_args=default_args,
    start_date=get_days_ago_start_date("ZETTLE_START_DAYS_AGO", 2 * 365),
    max_active_runs=1,  # This ensures sequential execution
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
    external_dag_id="01_ensure_things_exist",  # The ID of the DAG you're waiting for
    external_task_id=None,  # Set to None to wait for the entire DAG to complete
    allowed_states=["success"],  # You might need to customize this part
    dag=dag,
)

stripe_balances_task = StripeToPostgresOperator(
    task_id="import_stripe_transactions_to_datalake",
    postgres_conn_id="postgres_datalake_conn_id",
    stripe_conn_id="stripe_conn_id",
    destination_schema="transient_data",
    destination_table="stripe__transactions",
    dag=dag,
)
task_id = "stripe_has_records_to_process"
stripe_has_records_to_process = ShortCircuitOperator(
    task_id=task_id,
    python_callable=found_records_to_process,
    op_kwargs={
        "parent_task_id": "import_stripe_transactions_to_datalake",
        "xcom_key": "documents_found",
    },
)

task_id = "stripe_ensure_datalake_table_exists"
stripe_ensure_datalake_table = EnsurePostgresDatalakeTableExistsOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="transient_data",
    source_table="stripe__transactions",
    destination_schema="public",
    destination_table="raw__stripe__transactions",
    dag=dag,
)

stripe_missing_columns_task_id = "stripe_ensure_public_columns_uptodate"
stripe_ensure_datalake_table_columns = EnsureMissingPostgresColumnsOperator(
    task_id=stripe_missing_columns_task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_table="stripe__transactions",
    destination_table="raw__stripe__transactions",
    dag=dag,
)
task_id = "stripe_append_to_datalake"
stripe_append_transient_table_data = AppendTransientTableDataOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="transient_data",
    source_table="stripe__transactions",
    destination_schema="public",
    destination_table="raw__stripe__transactions",
    dag=dag,
)

task_id = "stripe_ensure_datalake_table_view"
stripe_ensure_table_view_exists = EnsurePostgresDatalakeTableViewExistsOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="public",
    source_table="raw__stripe__transactions",
    destination_schema="public",
    destination_table="stripe__transactions",
    prev_task_id=stripe_missing_columns_task_id,  # not append_transient data!!
    append_fields=["createdat", "updatedat", "airflow_sync_ds"],
    prepend_fields=["id"],
    dag=dag,
)

(
    stripe_balances_task
    >> stripe_has_records_to_process
    >> stripe_ensure_datalake_table
    >> stripe_ensure_datalake_table_columns
    >> stripe_append_transient_table_data
    >> stripe_ensure_table_view_exists
)

zettle_transactions_task = ZettleFinanceToPostgresOperator(
    task_id="import_zettle_transactions_to_datalake",
    postgres_conn_id="postgres_datalake_conn_id",
    zettle_conn_id="zettle_conn_id",
    destination_schema="transient_data",
    destination_table="zettle__transactions",
    dag=dag,
)

task_id = "zettle_transactions_has_records_to_process"
zettle_has_records_to_process = ShortCircuitOperator(
    task_id=task_id,
    python_callable=found_records_to_process,
    op_kwargs={
        "parent_task_id": "import_zettle_transactions_to_datalake",
        "xcom_key": "documents_found",
    },
)

task_id = "zettle_transactions_ensure_datalake_table_exists"
zettle_ensure_datalake_table = EnsurePostgresDatalakeTableExistsOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="transient_data",
    source_table="zettle__transactions",
    destination_schema="public",
    destination_table="raw__zettle__transactions",
    primary_key_template="""
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_class ic ON ic.oid = i.indexrelid
        WHERE n.nspname = '{{ destination_schema }}'
        AND c.relname = '{{ destination_table }}'
        AND ic.relname = '{{ destination_table }}_idx'
    ) THEN
        ALTER TABLE {{ destination_schema }}.{{ destination_table}}
            ADD CONSTRAINT {{ destination_table }}_idx PRIMARY KEY (originatingtransactionuuid, originatortransactiontype, amount, timestamp);
    END IF;
END $$;
""",  # noqa
    dag=dag,
)

zettle_missing_columns_task_id = "zettle_transactions_ensure_public_columns_uptodate"
zettle_ensure_datalake_table_columns = EnsureMissingPostgresColumnsOperator(
    task_id=zettle_missing_columns_task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_table="zettle__transactions",
    destination_table="raw__zettle__transactions",
    dag=dag,
)
task_id = "zettle_transactions_append_to_datalake"
zettle_append_transient_table_data = AppendTransientTableDataOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="transient_data",
    source_table="zettle__transactions",
    destination_schema="public",
    destination_table="raw__zettle__transactions",
    dag=dag,
    delete_template="DELETE FROM {{ destination_schema }}.{{destination_table}} WHERE airflow_sync_ds='{{ ds }}'",
)

task_id = "zettle_transactions_ensure_datalake_table_view"
zettle_ensure_table_view_exists = EnsurePostgresDatalakeTableViewExistsOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="public",
    source_table="raw__zettle__transactions",
    destination_schema="public",
    destination_table="zettle__transactions",
    prev_task_id=zettle_missing_columns_task_id,  # not append_transient data!!
    append_fields=["createdat", "updatedat", "airflow_sync_ds"],
    prepend_fields=["id"],
    dag=dag,
)

(
    zettle_transactions_task
    >> zettle_has_records_to_process
    >> zettle_ensure_datalake_table
    >> zettle_ensure_datalake_table_columns
    >> zettle_append_transient_table_data
    >> zettle_ensure_table_view_exists
)

zettle_purchases_task = ZettlePurchasesToPostgresOperator(
    task_id="import_zettle_purchases_to_datalake",
    postgres_conn_id="postgres_datalake_conn_id",
    zettle_conn_id="zettle_conn_id",
    destination_schema="transient_data",
    destination_table="zettle__purchases",
    dag=dag,
)

task_id = "zettle_purchases_has_records_to_process"
zettle_purchases_has_records_to_process = ShortCircuitOperator(
    task_id=task_id,
    python_callable=found_records_to_process,
    op_kwargs={
        "parent_task_id": "import_zettle_purchases_to_datalake",
        "xcom_key": "documents_found",
    },
)

task_id = "zettle_purchases_ensure_datalake_table_exists"
zettle_purchases_ensure_datalake_table = EnsurePostgresDatalakeTableExistsOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="transient_data",
    source_table="zettle__purchases",
    destination_schema="public",
    destination_table="raw__zettle__purchases",
    dag=dag,
)

zettle_purchases_missing_columns_task_id = "zettle_purchases_ensure_public_columns_uptodate"
zettle_purchases_ensure_datalake_table_columns = EnsureMissingPostgresColumnsOperator(
    task_id=zettle_purchases_missing_columns_task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_table="zettle__purchases",
    destination_table="raw__zettle__purchases",
    dag=dag,
)
task_id = "zettle_purchases_append_to_datalake"
zettle_purchases_append_transient_table_data = AppendTransientTableDataOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="transient_data",
    source_table="zettle__purchases",
    destination_schema="public",
    destination_table="raw__zettle__purchases",
    dag=dag,
    delete_template="DELETE FROM {{ destination_schema }}.{{destination_table}} WHERE airflow_sync_ds='{{ ds }}'",
)

task_id = "zettle_ensure_datalake_table_view"
zettle_purchases_ensure_table_view_exists = EnsurePostgresDatalakeTableViewExistsOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="public",
    source_table="raw__zettle__purchases",
    destination_schema="public",
    destination_table="zettle__purchases",
    prev_task_id=zettle_missing_columns_task_id,  # not append_transient data!!
    append_fields=["createdat", "updatedat", "airflow_sync_ds"],
    prepend_fields=["id"],
    dag=dag,
)

(
    zettle_purchases_task
    >> zettle_purchases_has_records_to_process
    >> zettle_purchases_ensure_datalake_table
    >> zettle_purchases_ensure_datalake_table_columns
    >> zettle_purchases_append_transient_table_data
    >> zettle_purchases_ensure_table_view_exists
)


(
    wait_for_things_to_exist
    >> is_latest_dagrun_task
    >> [stripe_balances_task, zettle_purchases_task, zettle_transactions_task]
)
