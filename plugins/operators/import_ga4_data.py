# import re

import pandas as pd
from sqlalchemy import text, create_engine
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import Metric, DateRange, Dimension, RunReportRequest

# from airflow.utils.decorators import apply_defaults
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

from plugins.operators.mixins.last_successful_dagrun import LastSuccessfulDagrunMixin


class GA4ToPostgresOperator(LastSuccessfulDagrunMixin, BaseOperator):
    """
    Operator for fetching GA4 data using API
    """

    MAX_RETRIES = 3

    def __init__(
        self,
        *,
        postgres_conn_id: str = "postgres_conn_id",
        google_conn_id: str = "ga4_api_data",
        GA4_PROPERTY_ID: str,
        destination_schema: str,
        destination_table: str,
        rebuild: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.google_conn_id = google_conn_id
        self.GA4_PROPERTY_ID = GA4_PROPERTY_ID
        self.destination_schema = destination_schema
        self.destination_table = destination_table
        self.rebuild = rebuild
        self.separator = "__"
        self.last_successful_dagrun_xcom_key = "last_successful_dagrun_ts"

        self.context = {
            "destination_schema": destination_schema,
            "destination_table": destination_table,
            # "partner_ref": partner_ref,
        }

    def get_postgres_sqlalchemy_engine(self, hook):
        """Create a SQLAlchemy engine for PostgreSQL."""
        conn = hook.get_connection(self.postgres_conn_id)
        conn_uri = conn.get_uri().replace("postgres://", "postgresql://")
        return create_engine(conn_uri)

    def execute(self, context):
        google_hook = GoogleBaseHook(gcp_conn_id=self.google_conn_id)
        self.log.info(f"Successfully initialized Google hook with connection ID: {self.google_conn_id}")

        # Get the connection and log its properties
        conn = google_hook.get_connection(self.google_conn_id)
        self.log.info(f"Connection ID: {self.google_conn_id}")
        self.log.info(f"Connection Type: {conn.conn_type}")

        # Extract and log keyfile information
        keyfile_path = conn.extra_dejson.get("keyfile_path")
        keyfile_dict = conn.extra_dejson.get("keyfile_dict")

        self.log.info(f"Keyfile path from connection: {keyfile_path}")
        self.log.info(f"Keyfile dict exists: {keyfile_dict is not None}")

        # Log available connection extras for debugging
        self.log.info(f"Connection extras keys: {list(conn.extra_dejson.keys())}")

        try:
            # Set up GA4 client
            google_hook = GoogleBaseHook(gcp_conn_id=self.google_conn_id)

            # Get credentials - add error handling and logging
            try:
                credentials = google_hook.get_credentials()
                self.log.info("Successfully retrieved Google credentials")
            except Exception as e:
                self.log.error(f"Failed to get Google credentials: {str(e)}")
                raise AirflowException(f"Authentication failed: {str(e)}")

            # Initialize client with credentials
            try:
                client = BetaAnalyticsDataClient(credentials=credentials)
                self.log.info("Successfully initialized GA4 client")
            except Exception as e:
                self.log.error(f"Failed to initialize GA4 client: {str(e)}")
                raise AirflowException(f"GA4 client initialization failed: {str(e)}")

            # Set up Postgres connection
            hook = BaseHook.get_hook(self.postgres_conn_id)
            engine = self.get_postgres_sqlalchemy_engine(hook)
            run_id = context["run_id"]

            with engine.connect() as conn:
                transaction = conn.begin()
                try:
                    # Create schema if it doesn't exist
                    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.destination_schema}"))

                    # If rebuilding or no last successful run, use the default start date from the DAG
                    if self.rebuild:
                        start_param = context["dag"].default_args["start_date"].strftime("%Y-%m-%d")
                    else:
                        # Get last successful dagrun timestamp using the mixin
                        last_successful_dagrun_ts = self.get_last_successful_dagrun_ts(run_id=run_id)
                        if last_successful_dagrun_ts is None:
                            # If no previous successful run, use the default start date
                            start_param = context["dag"].default_args["start_date"].strftime("%Y-%m-%d")
                            self.log.info(f"No previous successful run found. Using default start date: {start_param}")
                        else:
                            self.log.info(f"Last successful run timestamp: {last_successful_dagrun_ts}")
                            start_param = last_successful_dagrun_ts.format("YYYY-MM-DD")

                    lte = context["data_interval_end"].format("YYYY-MM-DD")
                    self.log.info(f"Fetching GA4 data from {start_param} to {lte}")

                    # Create a list of date ranges for the API
                    date_ranges = [DateRange(start_date=start_param, end_date=lte)]

                    # Clean existing data that may overlap
                    self.log.info(f"Cleaning existing data for date range: {date_ranges}")

                    # add logging to count records before and after delete

                    delete_sql = text(
                        f"""
                        DO $$
                        BEGIN
                            IF EXISTS (
                                SELECT FROM information_schema.tables
                                WHERE table_schema = '{self.destination_schema}'
                                AND table_name = '{self.destination_table}'
                            ) THEN
                                DELETE FROM {self.destination_schema}.{self.destination_table}
                                WHERE date >= :start_date;
                            END IF;
                        END $$;
                    """
                    )
                    conn.execute(delete_sql, {"start_date": start_param})

                    total_docs_processed = 0

                    response = self.run_ga4_report(client, date_ranges)

                    if not response.rows:
                        self.log.info("No records to process")
                        total_docs_processed = 0
                    else:
                        records = [self.process_ga4_row(row, context) for row in response.rows]
                        total_docs_processed = self.write_to_database(records, conn, context)

                    # Create composite index for better query performance
                    conn.execute(
                        text(
                            f"""CREATE INDEX IF NOT EXISTS {self.destination_table}_composite_idx
                                ON {self.destination_schema}.{self.destination_table}
                                (date, city, partner_reference, service_type, event_name);"""
                        )
                    )

                    transaction.commit()

                except Exception as e:
                    self.log.error(f"Error during database operation: {str(e)}")
                    transaction.rollback()
                    raise AirflowException(f"Database operation failed. Rolling Back: {str(e)}")

            context["ti"].xcom_push(key="documents_found", value=total_docs_processed)
            return f"Successfully processed {total_docs_processed} GA4 records"
            # wipe memory

        except Exception as e:
            self.log.error(f"An error occurred: {str(e)}")
            raise AirflowException(str(e))

    def run_ga4_report(self, client, date_ranges):
        """Runs GA4 report with specified dimensions and metrics."""
        try:
            request = RunReportRequest(
                property=f"properties/{self.GA4_PROPERTY_ID}",
                dimensions=[
                    Dimension(name="date"),
                    Dimension(name="city"),
                    Dimension(name="customEvent:partner_reference"),
                    Dimension(name="customEvent:service_type"),
                    Dimension(name="eventName"),
                ],
                metrics=[
                    Metric(name="activeUsers"),
                    Metric(name="sessions"),
                    Metric(name="totalUsers"),
                ],
                date_ranges=date_ranges,
                limit=100000,
            )
            return client.run_report(request)
        except Exception as e:
            self.log.error(f"Error running GA4 report: {str(e)}")
            raise AirflowException(f"GA4 report failed: {str(e)}")

    def safe_convert_to_int(self, value, default=0):
        """Safely converts string values to integers."""
        try:
            # First convert to float to handle scientific notation
            return int(float(value))
        except (ValueError, TypeError):
            self.log.warning(f"Failed to convert value '{value}' to integer. Using default: {default}")
            return default

    def process_ga4_row(self, row, context):
        """Process a single GA4 row with safe numeric conversion."""
        try:
            return {
                "date": row.dimension_values[0].value,
                "city": row.dimension_values[1].value,
                "partner_reference": row.dimension_values[2].value,
                "service_type": row.dimension_values[3].value,
                "event_name": row.dimension_values[4].value,
                "active_users": self.safe_convert_to_int(row.metric_values[0].value),
                "sessions": self.safe_convert_to_int(row.metric_values[1].value),
                "total_users": self.safe_convert_to_int(row.metric_values[2].value),
                "sync_timestamp": pd.Timestamp.now(),
                "airflow_sync_ds": context["ds"],
            }
        except Exception as e:
            self.log.error(f"Error processing row: {str(e)}")
            self.log.error(f"Problematic row: {row}")
            raise AirflowException(f"Row processing failed: {str(e)}")

    def write_to_database(self, records, conn, context):
        """Write records to the database."""
        if not records:
            self.log.info("No records to write to database")
            return 0

        # Create table with ID if it doesn't exist
        create_table_sql = text(
            f"""
            CREATE TABLE IF NOT EXISTS {self.destination_schema}.{self.destination_table} (
                id SERIAL PRIMARY KEY,
                date DATE,
                city VARCHAR,
                partner_reference VARCHAR,
                service_type VARCHAR,
                event_name VARCHAR,
                active_users INTEGER,
                sessions INTEGER,
                total_users INTEGER,
                sync_timestamp TIMESTAMP,
                airflow_sync_ds DATE
            )
        """
        )
        conn.execute(create_table_sql)

        df = pd.DataFrame(records)
        # Sort DataFrame by date before inserting
        df = df.sort_values("date")

        # Ensure numeric columns are properly typed
        numeric_columns = ["active_users", "sessions", "total_users"]
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")

        # If rebuilding, reset the sequence
        if self.rebuild:
            conn.execute(text(f"TRUNCATE TABLE {self.destination_schema}.{self.destination_table} RESTART IDENTITY"))

        df.to_sql(
            self.destination_table,
            conn,
            schema=self.destination_schema,
            if_exists="append",
            index=False,
            chunksize=1000,
        )

        # Keep both indexes
        conn.execute(
            text(
                f"CREATE INDEX IF NOT EXISTS {self.destination_table}_date_idx "
                f"ON {self.destination_schema}.{self.destination_table} (date);"
            )
        )

        total_rows = len(records)
        self.log.info(f"Processed {total_rows} rows")
        return total_rows
