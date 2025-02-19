import re

import pandas as pd
from sqlalchemy import text, create_engine
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.utils.decorators import apply_defaults
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import Metric, DateRange, Dimension, RunReportRequest
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class GA4ToPostgresOperator(BaseOperator):
    """
    Operator that fetches data from Google Analytics 4 and loads it into a Postgres database.

    :param property_id: GA4 property ID
    :param postgres_conn_id: Airflow connection ID for Postgres
    :param google_analytics_conn_id: Airflow connection ID for GA4
    :param destination_schema: Target schema in Postgres
    :param destination_table: Target table in Postgres
    :param start_date: Start date for GA4 data pull (templated)
    :param end_date: End date for GA4 data pull (templated)
    """

    ui_color = "#f9c915"  # Consistent with other operators

    @apply_defaults
    def __init__(
        self,
        property_id: str,
        postgres_conn_id: str,
        google_analytics_conn_id: str,
        destination_schema: str,
        destination_table: str,
        start_date: str,
        end_date: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.property_id = property_id
        self.postgres_conn_id = postgres_conn_id
        self.google_analytics_conn_id = google_analytics_conn_id
        self.destination_schema = destination_schema
        self.destination_table = destination_table
        self.start_date = start_date
        self.end_date = end_date

    def get_postgres_sqlalchemy_engine(self, hook, engine_kwargs=None):
        """Creates SQLAlchemy engine from Airflow connection."""
        if engine_kwargs is None:
            engine_kwargs = {}
        conn_uri = hook.get_uri().replace("postgres:/", "postgresql:/")
        conn_uri = re.sub(r"\?.*$", "", conn_uri)
        return create_engine(conn_uri, **engine_kwargs)

    def execute(self, context):
        try:
            # Set up GA4 client
            ga4_hook = GoogleBaseHook(gcp_conn_id=self.google_analytics_conn_id)
            ga4_credentials = ga4_hook._get_credentials()
            client = BetaAnalyticsDataClient(credentials=ga4_credentials)

            # Set up Postgres connection
            hook = BaseHook.get_hook(self.postgres_conn_id)
            engine = self.get_postgres_sqlalchemy_engine(hook)

            total_docs_processed = 0
            offset = 0

            with engine.connect() as conn:
                transaction = conn.begin()
                try:
                    # Create schema if it doesn't exist
                    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.destination_schema}"))

                    # Clean existing data for the current execution date
                    self.log.info(f"Cleaning existing data for date: {self.end_date}")
                    delete_sql = text(
                        f"""
                        DELETE FROM {self.destination_schema}.{self.destination_table}
                        WHERE date = :date_to_clean
                        """
                    )
                    conn.execute(delete_sql, {"date_to_clean": self.end_date})

                    # Fetch data from GA4 with pagination
                    while True:
                        response = self.run_ga4_report(client, self.start_date, self.end_date, offset)

                        if not response.rows:
                            break

                        records = []
                        for row in response.rows:
                            records.append(
                                {
                                    "date": row.dimension_values[0].value,
                                    "city": row.dimension_values[1].value,
                                    "partner_reference": row.dimension_values[2].value,
                                    "service_type": row.dimension_values[3].value,
                                    "event_name": row.dimension_values[4].value,
                                    "event_count": int(row.metric_values[0].value),
                                    "active_users": int(row.metric_values[1].value),
                                    "sessions": int(row.metric_values[2].value),
                                    "total_users": int(row.metric_values[3].value),
                                    "checkouts": int(row.metric_values[4].value),
                                    "sync_timestamp": pd.Timestamp.now(),
                                    "airflow_sync_ds": context["ds"],
                                }
                            )

                        total_docs_processed += len(records)
                        self.log.info(f"Fetched {len(records)} rows. Total rows: {total_docs_processed}")

                        if records:
                            df = pd.DataFrame(records)
                            df.to_sql(
                                self.destination_table,
                                conn,
                                schema=self.destination_schema,
                                if_exists="append",
                                index=False,
                                chunksize=1000,
                            )

                        if len(response.rows) < 100000:  # GA4's max limit per request
                            break

                        offset += len(response.rows)

                    transaction.commit()

                except Exception as e:
                    self.log.error(f"Error during database operation: {str(e)}")
                    transaction.rollback()
                    raise AirflowException(f"Database operation failed Rolling Back: {str(e)}")

            # Push total documents processed to XCom
            context["ti"].xcom_push(key="documents_found", value=total_docs_processed)
            return f"Successfully processed {total_docs_processed} GA4 records"

        except Exception as e:
            self.log.error(f"An error occurred: {str(e)}")
            raise AirflowException(str(e))

    def run_ga4_report(self, client, start_date, end_date, offset=0):
        """Runs GA4 report with specified dimensions and metrics."""
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=[
                Dimension(name="date"),
                Dimension(name="city"),
                Dimension(name="customEvent:partner_reference"),
                Dimension(name="customEvent:service_type"),
                Dimension(name="eventName"),
            ],
            metrics=[
                Metric(name="eventCount"),
                Metric(name="activeUsers"),
                Metric(name="sessions"),
                Metric(name="totalUsers"),
                Metric(name="checkouts"),
            ],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            limit=100000,
            offset=offset,
        )
        return client.run_report(request)
