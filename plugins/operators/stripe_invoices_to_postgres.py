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

# https://chatgpt.com/share/67801545-e8e8-800d-8842-962e1cd3ac38


class StripeInvoicesToPostgresOperator(
    LastSuccessfulDagrunMixin, DagRunTaskCommsMixin, FlattenJsonDictMixin, BaseOperator
):
    @apply_defaults
    def __init__(
        self,
        postgres_conn_id,
        rebuild: bool,
        stripe_conn_id,
        destination_schema,
        destination_table,
        start_days_ago=45,
        *args,
        **kwargs,
    ):
        super(StripeInvoicesToPostgresOperator, self).__init__(*args, **kwargs)
        self.destination_schema = destination_schema
        self.destination_table = destination_table
        self.postgres_conn_id = postgres_conn_id
        self.rebuild = rebuild
        self.stripe_conn_id = stripe_conn_id
        self.start_days_ago = start_days_ago
        self.discard_fields = ["payment_method_details", "source", "rendering"]
        self.last_successful_dagrun_xcom_key = "last_successful_dagrun_ts"
        self.last_successful_item_key = "last_successful_invoice_id"
        self.separator = "__"
        self.preserve_fields = [
            # Core fields
            ("id", "string"),
            ("object", "string"),
            ("account_country", "string"),
            ("account_name", "string"),
            ("account_tax_ids", "string"),
            ("amount_due", "Int64"),
            ("amount_paid", "Int64"),
            ("amount_remaining", "Int64"),
            ("amount_shipping", "Int64"),
            ("application", "string"),
            ("application_fee_amount", "Int64"),
            ("attempt_count", "Int64"),
            ("attempted", "boolean"),
            ("auto_advance", "boolean"),
            ("automatic_tax__enabled", "boolean"),
            ("automatic_tax__status", "string"),
            ("automatic_tax__liability", "string"),
            ("billing_reason", "string"),
            ("charge", "string"),
            ("collection_method", "string"),
            ("created", "Int64"),
            ("currency", "string"),
            ("custom_fields", "string"),
            ("customer", "string"),
            ("customer_address__city", "string"),
            ("customer_address__country", "string"),
            ("customer_address__line1", "string"),
            ("customer_address__line2", "string"),
            ("customer_address__postal_code", "string"),
            ("customer_address__state", "string"),
            ("customer_email", "string"),
            ("customer_name", "string"),
            ("customer_phone", "string"),
            ("customer_shipping", "string"),
            ("customer_tax_exempt", "string"),
            ("customer_tax_ids", "string"),
            ("default_payment_method", "string"),
            ("default_source", "string"),
            ("default_tax_rates", "string"),
            ("description", "string"),
            ("discounts", "string"),
            ("due_date", "Int64"),
            ("ending_balance", "Int64"),
            ("footer", "string"),
            ("from_invoice", "string"),
            ("hosted_invoice_url", "string"),
            ("invoice_pdf", "string"),
            ("last_finalization_error", "string"),
            ("latest_revision", "string"),
            ("livemode", "boolean"),
            ("metadata__lineitem_amount", "Int32"),
            ("metadata__lineitem_category", "string"),
            ("metadata__lineitem_type", "string"),
            ("metadata__lineitem_product_uuid", "string"),
            ("metadata__lineitem_payment_name", "string"),
            ("metadata__lineitem_name", "string"),
            ("metadata__harper_invoice_subtype", "string"),
            ("metadata__harper_invoice_type", "string"),
            ("metadata__lineitem_billed_quantity", "Int32"),
            ("next_payment_attempt", "Int64"),
            ("number", "string"),
            ("on_behalf_of", "string"),
            ("paid", "boolean"),
            ("paid_out_of_band", "boolean"),
            ("payment_intent", "string"),
            ("payment_settings__payment_method_options", "string"),
            ("payment_settings__payment_method_types", "string"),
            ("period_end", "Int64"),
            ("period_start", "Int64"),
            ("post_payment_credit_notes_amount", "Int64"),
            ("pre_payment_credit_notes_amount", "Int64"),
            ("quote", "string"),
            ("receipt_number", "string"),
            ("shipping_cost", "string"),
            ("shipping_details", "string"),
            ("starting_balance", "Int64"),
            ("statement_descriptor", "string"),
            ("status", "string"),
            ("status_transitions__finalized_at", "Int64"),
            ("status_transitions__marked_uncollectible_at", "Int64"),
            ("status_transitions__paid_at", "Int64"),
            ("status_transitions__voided_at", "Int64"),
            ("subscription", "string"),
            ("subtotal", "Int64"),
            ("subtotal_excluding_tax", "Int64"),
            ("tax", "Int64"),
            ("test_clock", "string"),
            ("total", "Int64"),
            ("total_discount_amounts", "string"),
            ("total_tax_amounts", "string"),
            ("transfer_data", "string"),
            ("webhooks_delivered_at", "Int64"),
            # Additional fields from metadata
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
        ]

        self.context = {
            "destination_schema": destination_schema,
            "destination_table": destination_table,
        }

    def execute(self, context):

        # Fetch data from the database
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
            f"Executing StripeInvoicesToPostgresOperator since last successful dagrun {last_successful_dagrun} - starting {self.start_days_ago} days back on {last_successful_dagrun_ts}"  # noqa
        )

        engine = self.get_postgres_sqlalchemy_engine(hook)

        with engine.connect() as conn:
            self.ensure_task_comms_table_exists(conn)

            starting_after = None
            if not self.rebuild:
                starting_after = self.get_last_successful_item_id(conn, context)

            if starting_after:
                self.log.info(
                    f"StripeInvoicesToPostgresOperator Restarting task for this Dagrun from the last successful invoice_id {starting_after} after last successful dagrun {last_successful_dagrun_ts}"  # noqa
                )
            else:
                self.log.info(
                    f"StripeInvoicesToPostgresOperator Starting Task Fresh for this dagrun from {last_successful_dagrun_ts}"  # noqa
                )

            created = {
                "gt": last_successful_dagrun_ts.int_timestamp,
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
                    # status="paid",
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
                df = self.align_to_schema_df(df)

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
        self.set_last_successful_dagrun_ts(context)
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

    def align_to_schema_df(self, df):
        # Check if the column exists
        if "metadata__harper_invoice_subtype" not in df.columns:
            # Create the column with default value "checkout" for all rows
            df["metadata__harper_invoice_subtype"] = "checkout"
        else:
            # Fill NaN values in the existing column with "checkout"
            df["metadata__harper_invoice_subtype"].fillna("checkout", inplace=True)
        if "metadata__harper_invoice_type" not in df.columns:
            # Create the column with default value "checkout" for all rows
            df["metadata__harper_invoice_type"] = ""
        else:
            # Fill NaN values in the existing column with "checkout"
            df["metadata__harper_invoice_type"].fillna("", inplace=True)

        for field, dtype in self.preserve_fields:
            if field not in df.columns:
                df[field] = None  # because zettle is rubbish
            print(f"aligning column {field} as type {dtype}")
            df[field] = df[field].astype(dtype)

        return df
