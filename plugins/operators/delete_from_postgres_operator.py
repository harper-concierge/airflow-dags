# import logging

from jinja2 import Template
from sqlalchemy import text, create_engine
from airflow.models import BaseOperator
from sqlalchemy.exc import SQLAlchemyError
from airflow.hooks.base import BaseHook
from airflow.utils.decorators import apply_defaults


class DeleteFromPostgresOperator(BaseOperator):
    """
    Operator that deletes records from a destination table based on matching IDs
    from a source table. Includes schema verification and transaction handling.
    """

    @apply_defaults
    def __init__(
        self,
        postgres_conn_id: str,
        source_schema: str,
        source_table: str,
        destination_schema: str,
        destination_table: str,
        *args,
        **kwargs,
    ):
        super(DeleteFromPostgresOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.source_schema = source_schema
        self.source_table = source_table
        self.destination_schema = destination_schema
        self.destination_table = destination_table

        # Template with proper schema qualification
        self.delete_template = """
        DELETE FROM {{ destination_schema }}.{{ destination_table }}
        WHERE id IN (
            SELECT DISTINCT id
            FROM {{ source_schema }}.{{ source_table }}
        );
        """

        # Template for schema/table existence check
        self.verify_template = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = :schema
            AND table_name = :table
        );
        """

    def should_skip_task(self, conn) -> bool:
        """
        Check if the task should be skipped based on destination table data existence
        Returns True if task should be skipped, False otherwise
        """
        try:
            # Check if destination table exists and has data
            sql = f"""
                SELECT EXISTS (
                    SELECT 1
                    FROM {self.destination_schema}.{self.destination_table}
                    LIMIT 1
                );
            """
            has_data = conn.execute(text(sql)).scalar()
            if not has_data:
                self.log.info(f"No data in {self.destination_schema}.{self.destination_table}. Skipping delete task.")
                return True
            return False

        except SQLAlchemyError as e:
            self.log.error(f"Error checking destination table: {e}")
            raise

    def verify_table_exists(self, conn, schema: str, table: str) -> bool:
        """Verify that the specified schema.table exists"""
        try:
            exists = conn.execute(text(self.verify_template), {"schema": schema, "table": table}).scalar()

            if not exists:
                self.log.error(f"Table {schema}.{table} does not exist")
                return False
            return True

        except SQLAlchemyError as e:
            self.log.error(f"Error verifying table {schema}.{table}: {e}")
            raise

    def execute(self, context):
        hook = BaseHook.get_hook(self.postgres_conn_id)

        # Continue with delete operation
        engine = create_engine(hook.get_uri())
        with engine.connect() as conn:
            # Check if we should skip this task
            if self.should_skip_task(conn):
                self.log.info("Skipping delete task as per skip condition")
                return

        # Render delete SQL
        delete_sql = Template(self.delete_template).render(
            destination_schema=self.destination_schema,
            destination_table=self.destination_table,
            source_schema=self.source_schema,
            source_table=self.source_table,
        )

        self.log.info(f"Preparing to execute delete SQL: {delete_sql}")

        engine = create_engine(hook.get_uri())

        try:
            with engine.connect() as conn:
                # Verify tables exist before proceeding
                if not (
                    self.verify_table_exists(conn, self.source_schema, self.source_table)
                    and self.verify_table_exists(conn, self.destination_schema, self.destination_table)
                ):
                    raise SQLAlchemyError("Required tables do not exist")

                # Begin transaction
                with conn.begin():
                    # Get count before delete
                    count_before = conn.execute(
                        text(f"SELECT COUNT(*) FROM {self.destination_schema}.{self.destination_table}")
                    ).scalar()

                    # Execute delete with logging
                    try:
                        result = conn.execute(text(delete_sql))
                        rows_deleted = result.rowcount
                    except SQLAlchemyError as e:
                        self.log.error(f"Delete operation failed: {e}")
                        raise

                    # Get count after delete
                    count_after = conn.execute(
                        text(f"SELECT COUNT(*) FROM {self.destination_schema}.{self.destination_table}")
                    ).scalar()

                    # Log results
                    self.log.info(
                        f"Delete operation completed successfully:\n"
                        f"- Records before: {count_before:,}\n"
                        f"- Records deleted: {count_before - count_after:,}\n"
                        f"- Records remaining: {count_after:,}"
                    )

                    return rows_deleted

        except Exception as e:
            self.log.error(f"Operation failed: {str(e)}")
            raise
