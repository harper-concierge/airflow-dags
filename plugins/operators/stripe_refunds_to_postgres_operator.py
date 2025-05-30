import re

import stripe
from pandas import DataFrame
from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.utils.decorators import apply_defaults
from airflow.models.connection import Connection

from plugins.operators.mixins.flatten_json import FlattenJsonDictMixin
from plugins.operators.mixins.last_successful_dagrun import LastSuccessfulDagrunMixin
from plugins.operators.mixins.dag_run_task_comms_mixin import DagRunTaskCommsMixin


class StripeRefundsToPostgresOperator(
    LastSuccessfulDagrunMixin, DagRunTaskCommsMixin, FlattenJsonDictMixin, BaseOperator
):
    @apply_defaults
    def __init__(
        self,
        postgres_conn_id,
        stripe_conn_id,
        rebuild,
        destination_schema,
        destination_table,
        start_days_ago=45,
        *args,
        **kwargs,
    ):
        super(StripeRefundsToPostgresOperator, self).__init__(*args, **kwargs)
        self.destination_schema = destination_schema
        self.destination_table = destination_table
        self.postgres_conn_id = postgres_conn_id
        self.rebuild = rebuild
        self.start_days_ago = start_days_ago
        self.stripe_conn_id = stripe_conn_id
        self.discard_fields = [
            "source",
            # you only can specify the parent key, not the full parent tree
            # e.g. payment_method_details__card__three_d_secure
            "card__three_d_secure",
        ]  # discard entire nested objects before flattening
        self.discard_flattened_fields = [
            "source",
            "payment_method_details__card__wallet__apple_pay__type",
            "payment_method_details__link__country",
            "payment_method_details__card__three_d_secure__authentication_flow",
            "payment_method_details__card__three_d_secure__electronic_commerce_indicator",
            "payment_method_details__card__three_d_secure__exemption_indicator",
            "payment_method_details__card__three_d_secure__result",
            "payment_method_details__card__three_d_secure__result_reason",
            "payment_method_details__card__three_d_secure__transaction_id",
            "payment_method_details__card__three_d_secure__version",
            "payment_method_details__card__checks__address_postal_code_check",  # 62 chars
        ]  # fields to discard after flattening
        self.preserve_fields = [
            # Amount fields
            ("amount", "Int64"),
            ("amount_captured", "Int64"),
            ("amount_refunded", "Int64"),
            # Billing details
            ("billing_details__address__city", "string"),
            ("billing_details__address__country", "string"),
            ("billing_details__address__line1", "string"),
            ("billing_details__address__line2", "string"),
            ("billing_details__address__postal_code", "string"),
            ("billing_details__address__state", "string"),
            ("billing_details__email", "string"),
            ("billing_details__name", "string"),
            ("billing_details__phone", "string"),
            # Payment method details
            ("payment_method_details__card__brand", "string"),
            ("payment_method_details__card__checks__address_line1_check", "string"),
            ("payment_method_details__card__checks__address_postal_code_check", "string"),
            ("payment_method_details__card__checks__cvc_check", "string"),
            ("payment_method_details__card__country", "string"),
            ("payment_method_details__card__exp_month", "Int64"),
            ("payment_method_details__card__exp_year", "Int64"),
            ("payment_method_details__card__fingerprint", "string"),
            ("payment_method_details__card__funding", "string"),
            ("payment_method_details__card__installments", "Int64"),
            ("payment_method_details__card__last4", "string"),
            ("payment_method_details__card__mandate", "string"),
            ("payment_method_details__card__network", "string"),
            ("payment_method_details__card__wallet__dynamic_last4", "string"),
            ("payment_method_details__card__wallet__google_pay__type", "string"),
            # Top level fields
            ("currency", "string"),
            ("customer", "string"),
            ("description", "string"),
            ("disputed", "bool"),
            ("livemode", "bool"),
            ("paid", "bool"),
            ("payment_intent", "string"),
            ("payment_method", "string"),
            ("refunded", "bool"),
            ("status", "string"),
            ("transfer_data", "string"),
            ("transfer_group", "string"),
            # Existing fields
            ("fraud_details__stripe_report", "string"),
            ("fraud_details__user_report", "string"),
            ("fraud_details__user_report_type", "string"),
            ("outcome__network_status", "string"),
            ("outcome__reason", "string"),
            ("outcome__risk_level", "string"),
            ("outcome__risk_score", "string"),
            ("outcome__seller_message", "string"),
            ("outcome__type", "string"),
            ("outcome__rule", "string"),
            ("outcome__rule__action", "string"),
            ("outcome__rule__predicate", "string"),
            ("failure_code", "string"),
            ("failure_message", "string"),
            ("failure_balance_transaction", "string"),
            ("failure_reason", "string"),
            ("invoice", "string"),
            ("balance_transaction", "string"),
            ("metadata__checkout_id", "string"),
            ("metadata__customer_id", "string"),
            ("metadata__harper_invoice_subtype", "string"),
            ("metadata__harper_invoice_type", "string"),
            ("metadata__idempotency_key", "string"),
            ("metadata__internal_order_id", "string"),
            ("metadata__order", "string"),
            ("metadata__payment_country", "string"),
            ("metadata__request_id", "string"),
            ("metadata__stripe_device_name", "string"),
            ("receipt_email", "string"),
            ("receipt_number", "string"),
            ("receipt_url", "string"),
            ("review", "string"),
            ("statement_descriptor", "string"),
            ("statement_descriptor_suffix", "string"),
        ]
        self.last_successful_dagrun_xcom_key = "last_successful_dagrun_ts"
        self.last_successful_item_key = "last_successful_refund_id"
        self.separator = "__"

        self.context = {
            "destination_schema": destination_schema,
            "destination_table": destination_table,
        }

    def execute(self, context):
        # Initialize Stripe and database connections
        hook = BaseHook.get_hook(self.postgres_conn_id)
        stripe_conn = Connection.get_connection_from_secrets(self.stripe_conn_id)
        stripe.api_key = stripe_conn.password
        ds = context["ds"]
        run_id = context["run_id"]
        last_successful_dagrun = self.get_last_successful_dagrun_ts(run_id=run_id)
        if self.rebuild:
            last_successful_dagrun_ts = last_successful_dagrun
        else:
            # Go back in time and reimport latest versions as we can't get by updated timestamp so keep a
            # rolling 45 days reimport
            last_successful_dagrun_ts = last_successful_dagrun.subtract(days=self.start_days_ago)
        self.log.info(
            f"Executing StripeRefundsToPostgresOperator since last successful dagrun {last_successful_dagrun} - starting {self.start_days_ago} days back on {last_successful_dagrun_ts}"  # noqa
        )

        engine = self.get_postgres_sqlalchemy_engine(hook)

        with engine.connect() as conn:
            self.ensure_task_comms_table_exists(conn)

            starting_after = None
            if not self.rebuild:
                starting_after = self.get_last_successful_item_id(conn, context)

            if starting_after:
                self.log.info(
                    f"Restarting task for this Dagrun from the last successful refund_id {starting_after} after last successful dagrun {last_successful_dagrun_ts}"  # noqa
                )
            else:
                self.log.info(f"Starting Task Fresh for this dagrun from {last_successful_dagrun_ts}")

            created = {
                "gt": last_successful_dagrun_ts.int_timestamp,
                "lte": context["data_interval_end"].int_timestamp,
            }
            total_docs_processed = 0

            limit = 100
            has_more = True

            while has_more:
                print(created, starting_after, limit)
                result = stripe.Refund.list(
                    limit=limit,
                    starting_after=starting_after,
                    created=created,
                    expand=["data.payment_method_details", "data.balance_transaction"],
                )

                records = result.data
                has_more = result.has_more
                total_docs_processed += len(records)

                df = DataFrame(records)

                if df.empty:
                    self.log.info("UNEXPECTED EMPTY Balance refunds to process.")
                    break

                starting_after = records[-1].id
                self.log.info(f"Processing ResultSet {total_docs_processed} - {starting_after}.")
                df["airflow_sync_ds"] = ds

                if self.discard_fields:
                    # Drop any unwanted fields before flattening
                    existing_discard_fields = [col for col in self.discard_fields if col in df.columns]
                    self.log.info(f"Discarding core fields {existing_discard_fields} for {self.discard_fields}")
                    df.drop(existing_discard_fields, axis=1, inplace=True)

                # Flatten JSON structure using the mixin method
                df = self.flatten_dataframe_columns_precisely(df)
                df.columns = df.columns.str.lower()

                if self.discard_flattened_fields:
                    # Drop any unwanted fields after flattening
                    existing_flattened_discard_fields = [
                        col for col in self.discard_flattened_fields if col in df.columns
                    ]
                    self.log.info(
                        f"Discarding flattenned fields {existing_flattened_discard_fields} for {self.discard_flattened_fields}"  # noqa
                    )
                    df.drop(existing_flattened_discard_fields, axis=1, inplace=True)

                df = self.align_to_schema_df(df)

                # Write processed data to PostgreSQL
                df.to_sql(
                    self.destination_table,
                    conn,
                    if_exists="append",
                    schema=self.destination_schema,
                    index=False,
                )
                self.set_last_successful_item_id(conn, context, starting_after)

            # Ensure primary key constraint exists if records processed
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
        WHERE n.nspname = '{self.destination_schema}'
        AND c.relname = '{self.destination_table}'
        AND ic.relname = '{self.destination_table}_idx'
    ) THEN
        ALTER TABLE {self.destination_schema}.{self.destination_table}
            ADD CONSTRAINT {self.destination_table}_idx PRIMARY KEY (id);
    END IF;
END $$;
"""
                )

            # Final task communication updates
            self.clear_task_vars(conn, context)
        context["ti"].xcom_push(key="documents_found", value=total_docs_processed)
        self.set_last_successful_dagrun_ts(context)
        self.log.info("Stripe Refunds written to Datalake successfully.")

    def get_postgres_sqlalchemy_engine(self, hook, engine_kwargs=None):
        if engine_kwargs is None:
            engine_kwargs = {}
        conn_uri = hook.get_uri().replace("postgres:/", "postgresql:/")
        conn_uri = re.sub(r"\?.*$", "", conn_uri)
        return create_engine(conn_uri, **engine_kwargs)

    def set_last_successful_item_id(self, conn, context, starting_after):
        return self.set_task_var(conn, context, self.last_successful_item_key, starting_after)

    def get_last_successful_item_id(self, conn, context):
        return self.get_task_var(conn, context, self.last_successful_item_key)

    def align_to_schema_df(self, df):
        # Check if the column exists
        if "metadata__harper_invoice_subtype" not in df.columns:
            # Create the column with default value "checkout" for all rows
            df["metadata__harper_invoice_subtype"] = "checkout"
        else:
            # Fill NaN values in the existing column with "checkout"
            df["metadata__harper_invoice_subtype"].fillna("checkout", inplace=True)

        # Check if the column exists
        for field, dtype in self.preserve_fields:
            if field not in df.columns:
                df[field] = None  # because stripe is rubbish
            print(f"aligning column {field} as type {dtype}")
            df[field] = df[field].astype(dtype)

        return df
