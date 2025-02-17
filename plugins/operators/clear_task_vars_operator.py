import re

from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook


class ClearTaskVarsOperator(BaseOperator):
    """
    Generic operator for clearing task variables based on task_id pattern.
    """

    def __init__(
        self,
        postgres_conn_id: str = "postgres_datalake_conn_id",
        rebuild: bool = False,
        task_id_pattern: str = None,  # New parameter
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.rebuild = rebuild
        # If no pattern provided, use the task_id with wildcard prefix
        self.task_id_pattern = task_id_pattern or f"%{self.task_id}"

    def clear_task_vars(self, conn, context):
        dag_id = context["dag_run"].dag_id

        self.log.info(f"Clearing vars for dag_id: {dag_id} matching pattern: {self.task_id_pattern}")

        sql = """
        DELETE FROM transient_data.dag_run_task_comms
        WHERE dag_id = %s
        AND task_id LIKE %s;
        """
        result = conn.execute(sql, (dag_id, self.task_id_pattern))
        self.log.info(f"Deleted {result.rowcount} rows")

    def get_postgres_sqlalchemy_engine(self, hook, engine_kwargs=None):
        """
        Get an sqlalchemy_engine object.
        """
        if engine_kwargs is None:
            engine_kwargs = {}
        conn_uri = hook.get_uri().replace("postgres:/", "postgresql:/")
        conn_uri = re.sub(r"\?.*$", "", conn_uri)
        return create_engine(conn_uri, **engine_kwargs)

    def execute(self, context):
        """
        Clear all matching task variables if rebuild is True.
        """
        if not self.rebuild:
            self.log.info("Skipping task variable cleanup - not a rebuild")
            return

        hook = BaseHook.get_hook(self.postgres_conn_id)
        engine = self.get_postgres_sqlalchemy_engine(hook)

        with engine.connect() as conn:
            dag_id = context["dag_run"].dag_id

            self.log.info(f"DAG ID: {dag_id}")
            self.log.info(f"Task pattern: {self.task_id_pattern}")

            # Check if table exists
            check_table_sql = """
            SELECT EXISTS (
                SELECT FROM pg_tables
                WHERE schemaname = 'transient_data'
                AND tablename = 'dag_run_task_comms'
            );
            """
            table_exists = conn.execute(check_table_sql).scalar()
            self.log.info(f"Communications table exists: {table_exists}")

            # Log matching records
            self.log.info("Fetching matching records from comms table...")
            matching_records_sql = """
            SELECT dag_id, task_id, run_id, variable_key, variable_value, updatedat, createdat
            FROM transient_data.dag_run_task_comms
            WHERE dag_id = %s
            AND task_id LIKE %s;
            """
            records = conn.execute(matching_records_sql, (dag_id, self.task_id_pattern)).fetchall()
            self.log.info(f"Found {len(records)} matching records:")
            for record in records:
                self.log.info(f"Record: {record}")

            if records:
                self.clear_task_vars(conn, context)
            else:
                self.log.info("No matching records to clear")

            self.log.info("Successfully processed task variables")
