from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator

from plugins.utils.is_latest_active_dagrun import is_latest_dagrun
from plugins.utils.found_records_to_process import found_records_to_process
from plugins.utils.send_harper_slack_notification import send_harper_failure_notification

from plugins.operators.ensure_schema_exists import EnsurePostgresSchemaExistsOperator
from plugins.operators.ensure_missing_columns import EnsureMissingPostgresColumnsOperator
from plugins.operators.stripe_invoices_to_postgres import StripeInvoicesToPostgresOperator
from plugins.operators.ensure_datalake_table_exists import EnsurePostgresDatalakeTableExistsOperator
from plugins.operators.ensure_missing_columns_function import EnsureMissingColumnsPostgresFunctionOperator
from plugins.operators.ensure_datalake_table_view_exists import EnsurePostgresDatalakeTableViewExistsOperator
from plugins.operators.stripe_charges_to_postgres_operator import StripeChargesToPostgresOperator
from plugins.operators.stripe_refunds_to_postgres_operator import StripeRefundsToPostgresOperator
from plugins.operators.append_transient_table_data_operator import AppendTransientTableDataOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 7, 14),
    "schedule_interval": "@daily",
    "depends_on_past": True,
    "retry_delay": timedelta(minutes=5),
    "retries": 0,
    "on_failure_callback": [send_harper_failure_notification()],
}


dag = DAG(
    "21_import_stripe_data",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,  # This ensures sequential execution
    template_searchpath="/usr/local/airflow/dags",
)

is_latest_dagrun_task = ShortCircuitOperator(
    task_id="skip_check",
    python_callable=is_latest_dagrun,
    depends_on_past=False,
    dag=dag,
)

# wait_for_migrations = ExternalTaskSensor(
#     task_id="wait_for_shopify_to_complete",
#     external_dag_id="15_get_shopify_data_dag",  # The ID of the DAG you're waiting for
#     external_task_id=None,  # Set to None to wait for the entire DAG to complete
#     allowed_states=["success"],  # You might need to customize this part
#     dag=dag,
# )

transient_schema_exists = EnsurePostgresSchemaExistsOperator(
    task_id="ensure_transient_schema_exists",
    schema="transient_data",
    postgres_conn_id="postgres_datalake_conn_id",
    dag=dag,
)
public_schema_exists = EnsurePostgresSchemaExistsOperator(
    task_id="ensure_public_schema_exists",
    schema="public",
    postgres_conn_id="postgres_datalake_conn_id",
    dag=dag,
)

ensure_missing_columns_function_exists = EnsureMissingColumnsPostgresFunctionOperator(
    task_id="ensure_missing_columns_function",
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="transient_data",
    destination_schema="public",
    dag=dag,
)


stripe_invoices_task = StripeInvoicesToPostgresOperator(
    task_id="import_stripe_invoices_to_datalake",
    postgres_conn_id="postgres_datalake_conn_id",
    stripe_conn_id="stripe_conn_id",
    destination_schema="transient_data",
    destination_table="stripe__invoices",
    dag=dag,
)
task_id = "stripe_invoices_has_records_to_process"
stripe_invoices_has_records_to_process = ShortCircuitOperator(
    task_id=task_id,
    python_callable=found_records_to_process,
    op_kwargs={
        "parent_task_id": "import_stripe_invoices_to_datalake",
        "xcom_key": "documents_found",
    },
)

task_id = "stripe_invoices_ensure_datalake_table_exists"
stripe_invoices_ensure_datalake_table = EnsurePostgresDatalakeTableExistsOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="transient_data",
    source_table="stripe__invoices",
    destination_schema="public",
    destination_table="raw__stripe__invoices",
    dag=dag,
)

stripe_invoices_missing_columns_task_id = "stripe_invoices_ensure_public_columns_uptodate"
stripe_invoices_ensure_datalake_table_columns = EnsureMissingPostgresColumnsOperator(
    task_id=stripe_invoices_missing_columns_task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_table="stripe__invoices",
    destination_table="raw__stripe__invoices",
    dag=dag,
)
task_id = "stripe_invoices_append_to_datalake"
stripe_invoices_append_transient_table_data = AppendTransientTableDataOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="transient_data",
    source_table="stripe__invoices",
    destination_schema="public",
    destination_table="raw__stripe__invoices",
    dag=dag,
)

task_id = "stripe_invoices_ensure_datalake_table_view"
stripe_invoices_ensure_table_view_exists = EnsurePostgresDatalakeTableViewExistsOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="public",
    source_table="raw__stripe__invoices",
    destination_schema="public",
    destination_table="stripe__invoices",
    prev_task_id=stripe_invoices_missing_columns_task_id,  # not append_transient data!!
    append_fields=["createdat", "updatedat", "airflow_sync_ds"],
    prepend_fields=["id"],
    dag=dag,
)

(
    stripe_invoices_task
    >> stripe_invoices_has_records_to_process
    >> stripe_invoices_ensure_datalake_table
    >> stripe_invoices_ensure_datalake_table_columns
    >> stripe_invoices_append_transient_table_data
    >> stripe_invoices_ensure_table_view_exists
)

stripe_charges_task = StripeChargesToPostgresOperator(
    task_id="import_stripe_charges_to_datalake",
    postgres_conn_id="postgres_datalake_conn_id",
    stripe_conn_id="stripe_conn_id",
    destination_schema="transient_data",
    destination_table="stripe__charges",
    dag=dag,
)
task_id = "stripe_charges_has_records_to_process"
stripe_charges_has_records_to_process = ShortCircuitOperator(
    task_id=task_id,
    python_callable=found_records_to_process,
    op_kwargs={
        "parent_task_id": "import_stripe_charges_to_datalake",
        "xcom_key": "documents_found",
    },
)

task_id = "stripe_charges_ensure_datalake_table_exists"
stripe_charges_ensure_datalake_table = EnsurePostgresDatalakeTableExistsOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="transient_data",
    source_table="stripe__charges",
    destination_schema="public",
    destination_table="raw__stripe__charges",
    dag=dag,
)

stripe_charges_missing_columns_task_id = "stripe_charges_ensure_public_columns_uptodate"
stripe_charges_ensure_datalake_table_columns = EnsureMissingPostgresColumnsOperator(
    task_id=stripe_charges_missing_columns_task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_table="stripe__charges",
    destination_table="raw__stripe__charges",
    dag=dag,
)
task_id = "stripe_charges_append_to_datalake"
stripe_charges_append_transient_table_data = AppendTransientTableDataOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="transient_data",
    source_table="stripe__charges",
    destination_schema="public",
    destination_table="raw__stripe__charges",
    dag=dag,
)

task_id = "stripe_charges_ensure_datalake_table_view"
stripe_charges_ensure_table_view_exists = EnsurePostgresDatalakeTableViewExistsOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="public",
    source_table="raw__stripe__charges",
    destination_schema="public",
    destination_table="stripe__charges",
    prev_task_id=stripe_charges_missing_columns_task_id,  # not append_transient data!!
    append_fields=["createdat", "updatedat", "airflow_sync_ds"],
    prepend_fields=["id"],
    dag=dag,
)

(
    stripe_charges_task
    >> stripe_charges_has_records_to_process
    >> stripe_charges_ensure_datalake_table
    >> stripe_charges_ensure_datalake_table_columns
    >> stripe_charges_append_transient_table_data
    >> stripe_charges_ensure_table_view_exists
)

stripe_refunds_task = StripeRefundsToPostgresOperator(
    task_id="import_stripe_refunds_to_datalake",
    postgres_conn_id="postgres_datalake_conn_id",
    stripe_conn_id="stripe_conn_id",
    destination_schema="transient_data",
    destination_table="stripe__refunds",
    dag=dag,
)
task_id = "stripe_refunds_has_records_to_process"
stripe_refunds_has_records_to_process = ShortCircuitOperator(
    task_id=task_id,
    python_callable=found_records_to_process,
    op_kwargs={
        "parent_task_id": "import_stripe_refunds_to_datalake",
        "xcom_key": "documents_found",
    },
)

task_id = "stripe_refunds_ensure_datalake_table_exists"
stripe_refunds_ensure_datalake_table = EnsurePostgresDatalakeTableExistsOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="transient_data",
    source_table="stripe__refunds",
    destination_schema="public",
    destination_table="raw__stripe__refunds",
    dag=dag,
)

stripe_refunds_missing_columns_task_id = "stripe_refunds_ensure_public_columns_uptodate"
stripe_refunds_ensure_datalake_table_columns = EnsureMissingPostgresColumnsOperator(
    task_id=stripe_refunds_missing_columns_task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_table="stripe__refunds",
    destination_table="raw__stripe__refunds",
    dag=dag,
)
task_id = "stripe_refunds_append_to_datalake"
stripe_refunds_append_transient_table_data = AppendTransientTableDataOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="transient_data",
    source_table="stripe__refunds",
    destination_schema="public",
    destination_table="raw__stripe__refunds",
    dag=dag,
)

task_id = "stripe_refunds_ensure_datalake_table_view"
stripe_refunds_ensure_table_view_exists = EnsurePostgresDatalakeTableViewExistsOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="public",
    source_table="raw__stripe__refunds",
    destination_schema="public",
    destination_table="stripe__refunds",
    prev_task_id=stripe_refunds_missing_columns_task_id,  # not append_transient data!!
    append_fields=["createdat", "updatedat", "airflow_sync_ds"],
    prepend_fields=["id"],
    dag=dag,
)

(
    stripe_refunds_task
    >> stripe_refunds_has_records_to_process
    >> stripe_refunds_ensure_datalake_table
    >> stripe_refunds_ensure_datalake_table_columns
    >> stripe_refunds_append_transient_table_data
    >> stripe_refunds_ensure_table_view_exists
)

(
    is_latest_dagrun_task
    >> transient_schema_exists
    >> public_schema_exists
    >> ensure_missing_columns_function_exists
    >> [stripe_invoices_task, stripe_charges_task, stripe_refunds_task]
)