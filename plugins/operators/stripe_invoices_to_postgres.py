import re

import stripe
from pandas import DataFrame
from sqlalchemy import create_engine
from airflow.models import XCom, BaseOperator
from airflow.hooks.base import BaseHook
from airflow.utils.session import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.models.connection import Connection

from plugins.utils.render_template import render_template

from plugins.operators.mixins.flatten_json import FlattenJsonDictMixin
from plugins.operators.mixins.dag_run_task_comms_mixin import DagRunTaskCommsMixin

# YOU MUST CREATE THE DESTINATION SPREADSHEET IN ADVANCE MANUALLY IN ORDER FOR THIS TO WORK


class StripeInvoicesToPostgresOperator(DagRunTaskCommsMixin, FlattenJsonDictMixin, BaseOperator):
    @apply_defaults
    def __init__(
        self,
        postgres_conn_id,
        stripe_conn_id,
        destination_schema,
        destination_table,
        *args,
        **kwargs,
    ):
        super(StripeInvoicesToPostgresOperator, self).__init__(*args, **kwargs)
        self.destination_schema = destination_schema
        self.destination_table = destination_table
        self.postgres_conn_id = postgres_conn_id
        self.stripe_conn_id = stripe_conn_id
        self.discard_fields = ["payment_method_details", "source"]
        self.last_successful_dagrun_xcom_key = "last_successful_dagrun_ts"
        self.last_successful_item_key = "last_successful_invoice_id"
        self.separator = "__"

        self.context = {
            "destination_schema": destination_schema,
            "destination_table": destination_table,
        }
        self.delete_template = """DO $$
BEGIN
   IF EXISTS (
    SELECT FROM pg_tables WHERE schemaname = '{{destination_schema}}'
    AND tablename = '{{destination_table}}') THEN
      DROP TABLE {{ destination_schema }}.{{destination_table}};
   END IF;
END $$;
"""

    def execute(self, context):

        # Fetch data from the database
        hook = BaseHook.get_hook(self.postgres_conn_id)
        stripe_conn = Connection.get_connection_from_secrets(self.stripe_conn_id)
        stripe.api_key = stripe_conn.password
        ds = context["ds"]
        run_id = context["run_id"]
        last_successful_dagrun_ts = self.get_last_successful_dagrun_ts(run_id=run_id)

        engine = self.get_postgres_sqlalchemy_engine(hook)

        with engine.connect() as conn:
            self.ensure_task_comms_table_exists(conn)
            starting_after = self.get_last_successful_item_id(conn, context)
            extra_context = {
                **context,
                **self.context,
                f"{self.last_successful_dagrun_xcom_key}": last_successful_dagrun_ts,
            }

            self.log.info(
                f"Executing StripeInvoicesToPostgresOperator since last successful dagrun {last_successful_dagrun_ts}"  # noqa
            )

            if starting_after:
                self.log.info(
                    f"StripeInvoicesToPostgresOperator Restarting task for this Dagrun from the last successful invoice_id {starting_after} after last successful dagrun {last_successful_dagrun_ts}"  # noqa
                )
            else:
                self.log.info(
                    f"StripeInvoicesToPostgresOperator Starting Task Fresh for this dagrun from {last_successful_dagrun_ts}"  # noqa
                )
                self.log.info("StripeInvoicesToPostgresOperator Deleting previous Data from this Dagrun")  # noqa
                self.delete_sql = render_template(self.delete_template, context=extra_context)
                self.log.info(f"Ensuring Transient Data is clean - {self.delete_sql}")
                conn.execute(self.delete_sql)

            created = {
                "gt": last_successful_dagrun_ts or 1690898262,
                "lte": context["data_interval_end"].int_timestamp,
            }
            total_docs_processed = 0

            limit = 100
            has_more = True

            while has_more:
                print(created, starting_after, limit)
                result = stripe.Invoice.list(
                    limit=limit,
                    starting_after=starting_after,
                    status="paid",
                    created=created,
                    expand=[],
                )

                records = result.data
                has_more = result.has_more
                total_docs_processed += len(records)

                df = DataFrame(records)

                if df.empty:
                    self.log.info("UNEXPECTED EMPTY Balance invoices to process.")
                    break

                starting_after = records[-1].id
                self.log.info(f"Processing ResultSet {total_docs_processed} - {starting_after}.")
                df["airflow_sync_ds"] = ds
                print(records[0])

                if self.discard_fields:
                    # keep this because if we're dropping any problematic fields
                    # from the top level we might want to do this before Flattenning
                    existing_discard_fields = [col for col in self.discard_fields if col in df.columns]
                    df.drop(existing_discard_fields, axis=1, inplace=True)

                df = self.flatten_dataframe_columns_precisely(df)
                df.columns = df.columns.str.lower()

                df.to_sql(
                    self.destination_table,
                    conn,
                    if_exists="append",
                    schema=self.destination_schema,
                    index=False,
                )
                self.set_last_successful_item_id(conn, context, starting_after)

            # Check how many Docs total
            if total_docs_processed > 0:
                conn.execute(
                    f"""
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_class ic ON ic.oid = i.indexrelid
        WHERE n.nspname = '{self.destination_schema}'  -- Schema name
        AND c.relname = '{self.destination_table}'  -- Table name
        AND ic.relname = '{self.destination_table}_idx'  -- Index name
    ) THEN
        ALTER TABLE {self.destination_schema}.{self.destination_table}
            ADD CONSTRAINT {self.destination_table}_idx PRIMARY KEY (id);
    END IF;
END $$;
"""
                )

            self.clear_task_vars(conn, context)
        context["ti"].xcom_push(key="documents_found", value=total_docs_processed)
        self.set_last_successful_dagrun_ts(context, context["data_interval_end"].int_timestamp)
        self.log.info("Stripe Charges written to Datalake successfully.")

    def get_postgres_sqlalchemy_engine(self, hook, engine_kwargs=None):
        """
        Get an sqlalchemy_engine object.

        :param engine_kwargs: Kwargs used in :func:`~sqlalchemy.create_engine`.
        :return: the created engine.
        """
        if engine_kwargs is None:
            engine_kwargs = {}
        conn_uri = hook.get_uri().replace("postgres:/", "postgresql:/")
        conn_uri = re.sub(r"\?.*$", "", conn_uri)
        return create_engine(conn_uri, **engine_kwargs)

    def set_last_successful_item_id(self, conn, context, starting_after):
        return self.set_task_var(conn, context, self.last_successful_item_key, starting_after)

    def get_last_successful_item_id(self, conn, context):
        return self.get_task_var(conn, context, self.last_successful_item_key)

    def set_last_successful_dagrun_ts(self, context, value):
        context["ti"].xcom_push(key=self.last_successful_dagrun_xcom_key, value=value)

    @provide_session
    def get_last_successful_dagrun_ts(self, run_id, session=None):
        query = XCom.get_many(
            include_prior_dates=True,
            dag_ids=self.dag_id,
            run_id=run_id,
            task_ids=self.task_id,
            key=self.last_successful_dagrun_xcom_key,
            session=session,
            limit=1,
        )

        xcom = query.first()
        if xcom:
            return xcom.value

        return None
