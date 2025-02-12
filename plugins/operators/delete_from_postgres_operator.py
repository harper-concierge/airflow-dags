# plugins/operators/delete_from_postgres_operator.py


from jinja2 import Template
from sqlalchemy import text, create_engine
from airflow.models import BaseOperator
from sqlalchemy.exc import SQLAlchemyError
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.utils.decorators import apply_defaults


class DeleteFromPostgresOperator(BaseOperator):
    """
    Operator for deleting records from a PostgreSQL table based on source data.
    Includes functionality for removing duplicates from transient data before processing.

    :param postgres_conn_id: The connection id of the PostgreSQL connection to use
    :type postgres_conn_id: str
    :param source_schema: The schema name of the source table
    :type source_schema: str
    :param source_table: The name of the source table
    :type source_table: str
    :param destination_schema: The schema name of the destination table
    :type destination_schema: str
    :param destination_table: The name of the destination table
    :type destination_table: str
    :param cascade: Whether to use CASCADE in drop operations
    :type cascade: bool
    :param skip: Whether to skip the delete operation
    :type skip: bool
    """

    template_fields = ("source_schema", "source_table", "destination_schema", "destination_table")
    ui_color = "#ffefeb"

    @apply_defaults
    def __init__(
        self,
        postgres_conn_id: str,
        source_schema: str,
        source_table: str,
        destination_schema: str,
        destination_table: str,
        cascade: bool = False,
        skip: bool = False,
        *args,
        **kwargs,
    ) -> None:
        super(DeleteFromPostgresOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.source_schema = source_schema
        self.source_table = source_table
        self.destination_schema = destination_schema
        self.destination_table = destination_table
        self.cascade = cascade
        self.skip = skip

        # SQL template for deleting records from destination based on source
        self.delete_template = """
        DELETE FROM {{ destination_schema }}.{{ destination_table }}
        WHERE id IN (
            SELECT o.id
            FROM {{ source_schema }}.{{ source_table }} o
        );
        """

        # SQL template for removing duplicates from transient data
        self.delete_duplicates_template = """
        WITH duplicate_rows AS (
            SELECT id,
                   partner_name,
                   ROW_NUMBER() OVER (
                       PARTITION BY id, partner_name
                       ORDER BY updated_at DESC NULLS LAST
                    ) as row_num
            FROM {schema}.{table}
            WHERE id IS NOT NULL
        )
        DELETE FROM {schema}.{table} a
        USING duplicate_rows b
        WHERE a.id = b.id
        AND b.row_num > 1
        AND a.partner_name = b.partner_name
        RETURNING a.id, a.partner_name;
        """

        # SQL template for validating table existence
        self.check_table_exists_template = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = :schema
            AND table_name = :table
        );
        """

    def get_sqlalchemy_engine(self, hook: BaseHook) -> object:
        """Creates SQLAlchemy engine from Airflow hook"""
        conn_uri = hook.get_uri().replace("postgres:/", "postgresql:/")
        return create_engine(conn_uri)

    def table_exists(self, conn, schema: str, table: str) -> bool:
        """
        Check if the specified table exists in the database.

        :param conn: SQLAlchemy connection
        :param schema: Schema name
        :param table: Table name
        :return: True if table exists, False otherwise
        :rtype: bool
        """
        try:
            result = conn.execute(text(self.check_table_exists_template), {"schema": schema, "table": table})
            return result.scalar()
        except SQLAlchemyError as e:
            self.log.error(f"Error checking table existence: {e}")
            raise

    def delete_transient_duplicates(self, conn) -> int:
        """
        Delete duplicate records from transient data table, keeping the most recent version
        based on updated_at, createdat, and airflow_sync_ds timestamps.

        :param conn: SQLAlchemy connection
        :return: Number of duplicate records deleted
        :rtype: int
        """
        try:
            if not self.table_exists(conn, self.source_schema, self.source_table):
                self.log.info(
                    f"Table {self.source_schema}.{self.source_table} does not exist. Skipping duplicate deletion."
                )
                return 0

            delete_dupes_sql = self.delete_duplicates_template.format(
                schema=self.source_schema, table=self.source_table
            )

            self.log.info(f"Executing duplicate deletion SQL: {delete_dupes_sql}")

            # Execute the duplicate deletion and fetch deleted records
            result = conn.execute(text(delete_dupes_sql))
            deleted_records = result.fetchall()
            deleted_count = len(deleted_records)

            if deleted_count > 0:
                self.log.info(
                    f"Deleted {deleted_count} duplicate records from {self.source_schema}.{self.source_table}"
                )
                # Log some examples of deleted duplicates
                sample_size = min(5, deleted_count)
                self.log.info(f"Sample of deleted duplicates (showing {sample_size}):")
                for i, record in enumerate(deleted_records[:sample_size]):
                    self.log.info(f"  {i + 1}. ID: {record.id}, Partner: {record.partner_name}")
            else:
                self.log.info("No duplicate records found")

            return deleted_count

        except SQLAlchemyError as e:
            self.log.error(f"Error during duplicate deletion: {e}")
            raise

    def execute(self, context: dict) -> None:
        """
        Execute the operator.

        :param context: Airflow context dictionary
        :type context: dict
        """
        if self.skip:
            self.log.info("Skip flag is set, skipping delete operation")
            return

        hook = BaseHook.get_hook(self.postgres_conn_id)
        engine = self.get_sqlalchemy_engine(hook)

        with engine.connect() as conn:
            try:
                # First, delete duplicates from transient data
                duplicates_deleted = self.delete_transient_duplicates(conn)

                # Check if destination table exists before proceeding
                if not self.table_exists(conn, self.destination_schema, self.destination_table):
                    self.log.info(
                        f"Destination table {self.destination_schema}.{self.destination_table} "
                        "does not exist. Skipping delete operation."
                    )
                    return

                # Proceed with the regular delete operation
                delete_sql = Template(self.delete_template).render(
                    destination_schema=self.destination_schema,
                    destination_table=self.destination_table,
                    source_schema=self.source_schema,
                    source_table=self.source_table,
                )

                self.log.info(f"Executing delete SQL: {delete_sql}")
                result = conn.execute(text(delete_sql))
                deleted_count = result.rowcount

                self.log.info(f"Deleted {deleted_count} rows from {self.destination_schema}.{self.destination_table}")

                # Push metrics to XCom for monitoring
                context["task_instance"].xcom_push(key="duplicates_deleted", value=duplicates_deleted)
                context["task_instance"].xcom_push(key="records_deleted", value=deleted_count)

            except SQLAlchemyError as e:
                error_msg = f"Database error during delete operation: {str(e)}"
                self.log.error(error_msg)
                raise AirflowException(error_msg)
            except Exception as e:
                error_msg = f"Unexpected error during delete operation: {str(e)}"
                self.log.error(error_msg)
                raise AirflowException(error_msg)
