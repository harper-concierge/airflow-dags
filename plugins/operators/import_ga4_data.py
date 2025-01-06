import re

import pandas as pd
from sqlalchemy import text, create_engine
from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.utils.decorators import apply_defaults
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import Metric, DateRange, Dimension, RunReportRequest  # MetricType,
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

# from google.analytics.data_v1beta import BetaAnalyticsDataClient


class GA4ToPostgresOperator(BaseOperator):
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
        super(GA4ToPostgresOperator, self).__init__(*args, **kwargs)
        self.property_id = property_id
        self.postgres_conn_id = postgres_conn_id
        self.google_analytics_conn_id = google_analytics_conn_id
        self.destination_schema = destination_schema
        self.destination_table = destination_table
        self.start_date = start_date
        self.end_date = end_date

    def get_postgres_sqlalchemy_engine(self, hook, engine_kwargs=None):
        if engine_kwargs is None:
            engine_kwargs = {}
        conn_uri = hook.get_uri().replace("postgres:/", "postgresql:/")
        conn_uri = re.sub(r"\?.*$", "", conn_uri)
        return create_engine(conn_uri, **engine_kwargs)

    def run_ga4_report(self, client, start_date, end_date, offset=0):
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=[
                Dimension(name="date"),
                Dimension(name="city"),
                # Dimension(name="sessionSourceMedium"),
                Dimension(name="customEvent:partner_reference"),
                # Dimension(name="customEvent:Partner Name"),
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

    def clean_existing_data(self, engine, date_to_clean):
        """Clean existing data for the given date before inserting new data."""
        with engine.connect() as conn:
            # First ensure schema exists
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.destination_schema}"))

            # Then check if table exists
            table_exists = conn.execute(
                text(
                    f"""
                SELECT EXISTS (
                    SELECT FROM pg_tables
                    WHERE schemaname = '{self.destination_schema}'
                    AND tablename = '{self.destination_table}'
                );
                """
                )
            ).scalar()

            if table_exists:
                self.log.info(f"Cleaning existing data for date: {date_to_clean}")
                delete_sql = text(
                    f"""
                    DELETE FROM {self.destination_schema}.{self.destination_table}
                    WHERE month = :date_to_clean
                    """
                )
                conn.execute(delete_sql, {"date_to_clean": date_to_clean})

    def write_to_postgres(self, df, engine):
        """Write DataFrame to Postgres with proper error handling."""
        try:
            # Write data to postgres
            df.to_sql(
                self.destination_table,
                engine,
                schema=self.destination_schema,
                if_exists="append",
                index=False,
                chunksize=1000,
            )

            # Create indexes if they don't exist
            with engine.connect() as conn:
                conn.execute(
                    text(
                        f"""
                    CREATE INDEX IF NOT EXISTS idx_{self.destination_table}_month
                    ON {self.destination_schema}.{self.destination_table} (month);

                    CREATE INDEX IF NOT EXISTS idx_{self.destination_table}_event_name
                    ON {self.destination_schema}.{self.destination_table} (event_name);
                    """
                    )
                )

        except Exception as e:
            self.log.error(f"Error writing to Postgres: {str(e)}")
            raise

    def execute(self, context):
        # Set up GA4 client
        ga4_hook = GoogleBaseHook(gcp_conn_id=self.google_analytics_conn_id)
        ga4_credentials = ga4_hook._get_credentials()
        client = BetaAnalyticsDataClient(credentials=ga4_credentials)

        # Set up Postgres connection
        hook = BaseHook.get_hook(self.postgres_conn_id)
        engine = self.get_postgres_sqlalchemy_engine(hook)

        # Clean existing data for the period we're about to import
        self.clean_existing_data(engine, self.start_date)

        all_data = []
        offset = 0
        total_rows = 0

        # Fetch data from GA4
        while True:
            try:
                response = self.run_ga4_report(client, self.start_date, self.end_date, offset)

                if not response.rows:
                    break

                for row in response.rows:
                    all_data.append(
                        {
                            "date": row.dimension_values[0].value,
                            "city": row.dimension_values[1].value,
                            # "source_medium": row.dimension_values[2].value,
                            "partner_reference": row.dimension_values[3].value,
                            # "partner_name": row.dimension_values[6].value,
                            "service_type": row.dimension_values[4].value,
                            "event_name": row.dimension_values[5].value,
                            "event_count": int(row.metric_values[0].value),
                            "active_users": int(row.metric_values[1].value),
                            "sessions": int(row.metric_values[2].value),
                            "total_users": int(row.metric_values[3].value),
                            "checkouts": int(row.metric_values[4].value),
                            "sync_timestamp": pd.Timestamp.now(),
                        }
                    )

                total_rows += len(response.rows)
                self.log.info(f"Fetched {len(response.rows)} rows. Total rows: {total_rows}")

                if len(response.rows) < 100000:
                    break

                offset += len(response.rows)

            except Exception as e:
                self.log.error(f"Error fetching GA4 data: {str(e)}")
                raise

        if not all_data:
            self.log.info("No data to write to Postgres.")
            return

        # Convert to DataFrame and process
        df = pd.DataFrame(all_data)

        # Write to Postgres
        self.write_to_postgres(df, engine)

        self.log.info(f"Successfully wrote {len(df)} rows to Postgres")
