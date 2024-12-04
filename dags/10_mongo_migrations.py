import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.external_task import ExternalTaskSensor

from plugins.utils.is_latest_active_dagrun import is_latest_dagrun
from plugins.utils.found_records_to_process import found_records_to_process
from plugins.utils.send_harper_slack_notification import send_harper_failure_notification

from plugins.operators.drop_table import DropPostgresTableOperator
from plugins.operators.analyze_table import RefreshPostgresTableStatisticsOperator
from plugins.operators.mongodb_to_postgres import MongoDBToPostgresViaDataframeOperator
from plugins.operators.ensure_missing_columns import EnsureMissingPostgresColumnsOperator
from plugins.operators.ensure_datalake_table_exists import EnsurePostgresDatalakeTableExistsOperator
from plugins.operators.ensure_datalake_table_view_exists import EnsurePostgresDatalakeTableViewExistsOperator
from plugins.operators.append_transient_table_data_operator import AppendTransientTableDataOperator

from data_migrations.aggregation_loader import load_aggregation_configs

rebuild = Variable.get("REBUILD_MONGO_DATA", "False").lower() in ["true", "1", "yes"]


def reset_rebuild_var():
    Variable.set("REBUILD_MONGO_DATA", False)


# Now load the migrations
migrations = load_aggregation_configs("aggregations")

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
    "10_mongo_migrations_dag",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,  # This ensures sequential execution
    template_searchpath="/usr/local/airflow/dags",
)

wait_for_things_to_exist = ExternalTaskSensor(
    task_id="wait_for_things_to_exist",
    external_dag_id="01_ensure_things_exist",  # The ID of the DAG you're waiting for
    external_task_id=None,  # Set to None to wait for the entire DAG to complete
    allowed_states=["success"],  # You might need to customize this part
    dag=dag,
)


# start_task = DummyOperator(task_id="start", dag=dag)
doc = """
Skip the subsequent tasks if
    a) the execution_date is in past
    b) there multiple dag runs are currently active
"""
start_task = ShortCircuitOperator(
    task_id="skip_check",
    python_callable=is_latest_dagrun,
    depends_on_past=False,
    dag=dag,
)
start_task.doc = doc

reset_rebuild_var_task = PythonOperator(
    task_id="reset_rebuild_var_task",
    depends_on_past=False,
    python_callable=reset_rebuild_var,
    dag=dag,
)


base_tables_completed = DummyOperator(task_id="base_tables_completed", dag=dag, trigger_rule=TriggerRule.NONE_FAILED)
exported_schemas_path = "../include/exportedSchemas/"
exported_schemas_abspath = os.path.join(os.path.dirname(os.path.abspath(__file__)), exported_schemas_path)

migration_tasks = []
for config in migrations:
    schema_path = os.path.join(exported_schemas_abspath, config["jsonschema"])
    task_id = f"{config['task_name']}_drop_transient_table_if_exists"
    drop_transient_table = DropPostgresTableOperator(
        task_id=task_id,
        postgres_conn_id="postgres_datalake_conn_id",
        schema="transient_data",
        table=config["destination_table"],
        dag=dag,
    )
    task_id = f"{config['task_name']}_drop_destination_table_if_exists"
    last_drop_table_task = drop_transient_table
    if rebuild:
        drop_destination_table = DropPostgresTableOperator(
            task_id=task_id,
            postgres_conn_id="postgres_datalake_conn_id",
            schema="public",
            table=f"raw_{config['destination_table']}",
            depends_on_past=False,
            dag=dag,
        )
        drop_transient_table >> drop_destination_table
        last_drop_table_task = drop_destination_table

    task_id = f"{config['task_name']}_migrate_to_postgres"
    mongo_to_postgres = MongoDBToPostgresViaDataframeOperator(
        task_id=task_id,
        mongo_conn_id="mongo_db_conn_id",
        postgres_conn_id="postgres_datalake_conn_id",
        preoperation=config.get("preoperation", None),
        aggregation_query=config["aggregation_query"],
        source_collection=config["source_collection"],
        source_database="harper-production",
        jsonschema=schema_path,
        destination_schema="transient_data",
        destination_table=config["destination_table"],
        unwind=config.get("unwind"),
        preserve_fields=config.get("preserve_fields", {}),
        discard_fields=config.get("discard_fields", []),
        convert_fields=config.get("convert_fields", []),
        rebuild=rebuild,
        dag=dag,
    )
    previous_task_id = task_id
    task_id = f"{config['task_name']}_has_records_to_process"
    has_records_to_process = ShortCircuitOperator(
        task_id=task_id,
        python_callable=found_records_to_process,
        op_kwargs={"parent_task_id": previous_task_id, "xcom_key": "documents_found"},
    )

    task_id = f"{config['task_name']}_refresh_transient_table_stats"
    refresh_transient_table = RefreshPostgresTableStatisticsOperator(
        task_id=task_id,
        postgres_conn_id="postgres_datalake_conn_id",
        schema="transient_data",
        table=config["destination_table"],
        dag=dag,
    )

    task_id = f"{config['task_name']}_ensure_datalake_table_exists"
    ensure_datalake_table = EnsurePostgresDatalakeTableExistsOperator(
        task_id=task_id,
        postgres_conn_id="postgres_datalake_conn_id",
        source_schema="transient_data",
        source_table=config["destination_table"],
        destination_schema="public",
        destination_table=f"raw__{config['destination_table']}",
        dag=dag,
    )

    task_id = f"{config['task_name']}_refresh_datalake_table_stats"
    refresh_datalake_table = RefreshPostgresTableStatisticsOperator(
        task_id=task_id,
        postgres_conn_id="postgres_datalake_conn_id",
        schema="public",
        table=f"raw__{config['destination_table']}",
        dag=dag,
    )

    missing_columns_task_id = f"{config['task_name']}_ensure_public_columns_uptodate"
    ensure_datalake_table_columns = EnsureMissingPostgresColumnsOperator(
        task_id=missing_columns_task_id,
        postgres_conn_id="postgres_datalake_conn_id",
        source_table=config["destination_table"],
        destination_table=f"raw__{config['destination_table']}",
        dag=dag,
    )
    task_id = f"{config['task_name']}_append_to_datalake"
    append_transient_table_data = AppendTransientTableDataOperator(
        task_id=task_id,
        postgres_conn_id="postgres_datalake_conn_id",
        source_schema="transient_data",
        source_table=config["destination_table"],
        destination_schema="public",
        destination_table=f"raw__{config['destination_table']}",
        dag=dag,
    )
    task_id = f"{config['task_name']}_ensure_datalake_table_view"
    ensure_table_view_exists = EnsurePostgresDatalakeTableViewExistsOperator(
        task_id=task_id,
        postgres_conn_id="postgres_datalake_conn_id",
        source_schema="public",
        source_table=f"raw__{config['destination_table']}",
        destination_schema="public",
        destination_table=config["destination_table"],
        prev_task_id=missing_columns_task_id,
        append_fields=config.get("append_fields", ["createdat", "updatedat", "airflow_sync_ds"]),
        prepend_fields=config.get("prepend_fields", ["id"]),
        dag=dag,
    )
    (
        last_drop_table_task
        >> mongo_to_postgres
        >> has_records_to_process
        >> refresh_transient_table
        >> ensure_datalake_table
        >> refresh_datalake_table
        >> ensure_datalake_table_columns
        >> append_transient_table_data
        >> ensure_table_view_exists
        >> base_tables_completed
        >> reset_rebuild_var_task
    )
    # append_transient_table_data >> base_tables_completed
    migration_tasks.append(drop_transient_table)

(wait_for_things_to_exist >> start_task >> migration_tasks)
