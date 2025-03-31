# import re

import time

import pandas as pd
from sqlalchemy import types, create_engine
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import Metric, DateRange, Dimension, RunReportRequest

# from airflow.utils.decorators import apply_defaults
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

from plugins.operators.mixins.last_successful_dagrun import LastSuccessfulDagrunMixin
from plugins.operators.mixins.dag_run_task_comms_mixin import DagRunTaskCommsMixin


class GA4ToPostgresOperator(LastSuccessfulDagrunMixin, DagRunTaskCommsMixin, BaseOperator):
    """
    Operator for fetching GA4 data using API
    """

    MAX_RETRIES = 3

    def __init__(
        self,
        *,
        postgres_conn_id: str = "postgres_conn_id",
        google_conn_id: str = "ga4_api_data",
        ga4_property_id: str,
        destination_schema: str,
        destination_table: str,
        rebuild: bool = False,
        debug_auth: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.google_conn_id = google_conn_id
        self.ga4_property_id = ga4_property_id
        self.destination_schema = destination_schema
        self.destination_table = destination_table
        self.rebuild = rebuild
        self.debug_auth = debug_auth
        self.separator = "__"
        self.last_successful_dagrun_xcom_key = "last_successful_dagrun_ts"
        self.offset_var_key = "offset"

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
        # Log the GA4 property ID
        self.log.info(f"GA4 Property ID: {self.ga4_property_id}")

        # Set up GA4 client once
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
            # Get credentials - add error handling and logging
            try:
                credentials = google_hook.get_credentials()
                self.log.info("Successfully retrieved Google credentials")

                # Add detailed auth logging
                if self.debug_auth:
                    if hasattr(credentials, "_service_account_email"):
                        self.log.info(f"Service account email: {credentials._service_account_email}")

                    if hasattr(credentials, "token"):
                        # Only log first few characters of token for security
                        token_preview = str(credentials.token)[:20] + "..." if credentials.token else "None"
                        self.log.info(f"Token preview: {token_preview}")

                    if hasattr(credentials, "expiry"):
                        self.log.info(f"Token expiration: {credentials.expiry}")

                    # If keyfile_dict exists, log a preview
                    if keyfile_dict:
                        dict_preview = str(keyfile_dict)[:50] + "..."
                        self.log.info(f"Keyfile dict preview: {dict_preview}")

            except Exception as e:
                self.log.error(f"Failed to get Google credentials: {str(e)}")
                if self.debug_auth:
                    self.log.error(f"Connection extras: {conn.extra_dejson}")
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
                self.ensure_task_comms_table_exists(conn)
                transaction = conn.begin()
                self.log.info(f"Destination Table: {self.destination_schema}.{self.destination_table}")
                try:
                    # Get stored offset using task comms
                    offset = self.get_last_successful_offset(conn, context)

                    if offset is not None:
                        self.log.info(f"Found previous offset: {offset}, resuming interrupted run")
                        # Use existing date range when resuming
                        last_successful_dagrun_ts = self.get_last_successful_dagrun_ts(run_id=run_id)
                        start_param = last_successful_dagrun_ts.format("YYYY-MM-DD")
                        self.log.info(f"Resuming fetch from {start_param} with offset {offset}")
                    else:
                        # No offset found, start fresh using last successful DAG run
                        last_successful_dagrun_ts = self.get_last_successful_dagrun_ts(run_id=run_id)
                        self.log.info(f"Last successful run timestamp: {last_successful_dagrun_ts}")
                        start_param = last_successful_dagrun_ts.format("YYYY-MM-DD")
                        self.log.info(f"Starting fresh fetch from {start_param}")
                        offset = 0

                    lte = context["data_interval_end"].format("YYYY-MM-DD")
                    self.log.info(f"Fetching GA4 data from {start_param} to {lte}")

                    # Create date ranges for the API
                    date_ranges = [DateRange(start_date=start_param, end_date=lte)]

                    total_docs_processed = 0
                    page_size = 50000

                    while True:
                        self.log.info(f"Fetching page with offset: {offset}")

                        response = self.run_ga4_report(client, date_ranges, offset, page_size)

                        # Check if there are rows to process
                        if response.rows:
                            # Get the total number of rows fetched from GA4
                            fetched_rows = len(response.rows)
                            self.log.info(f"Fetched {fetched_rows} rows from GA4 API.")

                            records = [self.process_ga4_row(row, context) for row in response.rows]
                            self.log.info(f"Processed {len(records)} records.")

                            batch_processed = self.write_to_database(records, conn, context)
                            self.log.info(f"Wrote {batch_processed} records to the database.")

                            # Update offset based on total fetched rows, not processed rows
                            offset += fetched_rows

                            # Store the updated offset
                            self.set_last_successful_offset(conn, context, offset)
                            self.log.info(f"Stored offset: {offset}")

                            # Track actual processed records for final count
                            total_docs_processed += batch_processed

                        # Break if the number of rows fetched is less than the page size
                        if len(response.rows) < page_size:
                            self.log.info("Less than page size fetched, breaking the loop.")
                            break

                    # Clear the offset after successful completion
                    self.clear_task_vars(conn, context)
                    self.log.info("Cleared task variables after successful completion.")
                    self.set_last_successful_dagrun_ts(context)
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

    def run_ga4_report(self, client, date_ranges, offset, page_size):
        """Runs GA4 report with specified dimensions and metrics."""
        retries = 0
        max_retries = 3
        while retries < max_retries:
            try:
                request = RunReportRequest(
                    property=f"properties/{self.ga4_property_id}",
                    dimensions=[
                        Dimension(name="date"),
                        Dimension(name="city"),
                        Dimension(name="customEvent:partner_reference"),
                        Dimension(name="customEvent:service_type"),
                        Dimension(name="eventName"),
                        Dimension(name="customEvent:partner"),
                    ],
                    metrics=[
                        Metric(name="activeUsers"),
                        Metric(name="sessions"),
                        Metric(name="totalUsers"),
                    ],
                    date_ranges=date_ranges,
                    limit=page_size,
                    offset=offset,
                )

                return client.run_report(request)

            except Exception as e:
                retries += 1
                if retries == max_retries:
                    self.log.error(f"Error after {max_retries} attempts: {str(e)}")
                    raise AirflowException(f"GA4 report failed: {str(e)}")

                # Exponential backoff
                wait_time = 2**retries
                self.log.info(f"Attempt {retries} failed. Waiting {wait_time} secs")
                time.sleep(wait_time)

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
            # Create id from combined fields, now including partner
            id = (
                f"{row.dimension_values[0].value}_{row.dimension_values[1].value}_"
                f"{row.dimension_values[2].value}_{row.dimension_values[3].value}_"
                f"{row.dimension_values[4].value}_{row.dimension_values[5].value}"
            )

            return {
                "id": id,  # composite id for Primary Key, unique constraint
                "date": row.dimension_values[0].value,
                "city": row.dimension_values[1].value,
                "partner_reference": row.dimension_values[2].value,
                "partner": row.dimension_values[5].value,
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

        df = pd.DataFrame(records)

        # Log initial count
        initial_count = len(df)

        # Filter out records where partner, partner_reference, or city is null/empty
        df = df[
            df["partner"].notna()
            & df["partner_reference"].notna()
            & df["city"].notna()
            & (df["partner"] != "")
            & (df["partner_reference"] != "")
            & (df["city"] != "")
        ]

        # Log how many records were filtered
        filtered_count = len(df)
        if filtered_count < initial_count:
            self.log.info(
                f"Filtered out {initial_count - filtered_count} records with empty partner/partner_reference/city"
            )

        if df.empty:
            self.log.info("No valid records to write after filtering")
            return 0

        # Sort DataFrame by date before inserting
        df = df.sort_values("date")

        # Ensure numeric columns are properly typed
        numeric_columns = ["active_users", "sessions", "total_users"]
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")

        # Ensure id column is treated as text/varchar
        df["id"] = df["id"].astype(str)

        df.to_sql(
            self.destination_table,
            conn,
            schema=self.destination_schema,
            if_exists="append",
            index=False,
            dtype={"id": types.String},
            chunksize=1000,
        )

        total_rows = len(df)
        self.log.info(f"Processed {total_rows} rows")
        return total_rows

    def set_last_successful_offset(self, conn, context, offset):
        """Store the last successful offset."""
        self.log.info(f"Setting offset to {offset} with key {self.offset_var_key}")
        try:
            result = self.set_task_var(conn, context, self.offset_var_key, str(offset))
            self.log.info(f"Set_task_var result: {result}")

            # Verify it was stored
            check = self.get_task_var(conn, context, self.offset_var_key)
            self.log.info(f"Verification - retrieved offset: {check}")

            return result
        except Exception as e:
            self.log.error(f"Error setting offset: {str(e)}")
            raise

    def get_last_successful_offset(self, conn, context):
        """Get the last successful offset."""
        offset = self.get_task_var(conn, context, self.offset_var_key)
        return int(offset) if offset is not None else None
