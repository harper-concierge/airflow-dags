import re
import json
import time

import pandas as pd
import shopify
import requests.exceptions
from pandas import DataFrame
from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

from plugins.utils.render_template import render_template

from plugins.operators.mixins.flatten_json import FlattenJsonDictMixin
from plugins.operators.mixins.last_successful_dagrun import LastSuccessfulDagrunMixin
from plugins.operators.mixins.dag_run_task_comms_mixin import DagRunTaskCommsMixin

# Filter output
required_columns = [
    "partner__name",
    "id",
    "admin_graphql_api_id",
    "app_id",
    "cancel_reason",
    "cancelled_at",
    "closed_at",
    "confirmation_number",
    "confirmed",
    "created_at",
    "currency",
    "current_subtotal_price",
    "current_subtotal_price_set__shop_money__currency_code",
    "current_subtotal_price_set__presentment_money__amount",
    "current_subtotal_price_set__presentment_money__currency_code",
    "current_total_discounts",
    "current_total_discounts_set__presentment_money__amount",
    "current_total_discounts_set__presentment_money__currency_code",
    "current_total_duties_set",
    "current_total_price",
    "current_total_price_set__presentment_money__amount",
    "current_total_price_set__presentment_money__currency_code",
    "current_total_tax",
    "current_total_tax_set__presentment_money__amount",
    "current_total_tax_set__presentment_money__currency_code",
    "discount_codes",
    "financial_status",
    "fulfillment_status",
    "harper_product",
    "name",
    "order_number",
    "order_status_url",
    "payment_gateway_names",
    "processed_at",
    "reference",
    "referring_site",
    "source_name",
    "source_url",
    "subtotal_price",
    "subtotal_price_set__presentment_money__amount",
    "subtotal_price_set__presentment_money__currency_code",
    "test",
    "tags",
    "taxes_included",
    "test",
    "total_discounts",
    "total_discounts_set__presentment_money__amount",
    "total_discounts_set__presentment_money__currency_code",
    "total_line_items_price",
    "total_line_items_price_set__presentment_money__amount",
    "total_line_items_price_set__presentment_money__currency_code",
    "total_outstanding",
    "total_price",
    "total_price_set__presentment_money__amount",
    "total_price_set__presentment_money__currency_code",
    "total_shipping_price_set__presentment_money__amount",
    "total_shipping_price_set__presentment_money__currency_code",
    "total_tax",
    "total_tax_set__presentment_money__amount",
    "total_tax_set__presentment_money__currency_code",
    "updated_at",
    "user_id",
    "customer__id",
    "customer__created_at",
    "customer__updated_at",
    "customer__state",
    "customer__tags",
    "customer__currency",
    "discount_applications",
    "line_items",
    # "payment_terms",
    "refunds",
    "shipping_address__city",
    "shipping_address__province",
    "shipping_address__country",
    "shipping_address__company",
    "shipping_address__country_code",
    "shipping_address__province_code",
    "source_name",
    "airflow_sync_ds",
    "partner__reference",
    "order_id",
    "order_name",
    "items_ordered",
    "items_returned",
    "value_ordered",
    "value_returned",
    "fulfilled_at",
    # "year_month",
]
# Reduce load for API
api_field_filter = [
    "id",
    "cancel_reason",
    "cancelled_at",
    "checkout_id",
    # 'checkout_token',
    "client_details",
    "closed_at",
    "confirmation_number",
    "confirmed",
    "contact_email",
    "created_at",
    "currency",
    "current_subtotal_price",
    "current_subtotal_price_set",
    "current_total_additional_fees_set",
    "current_total_discounts",
    "current_total_discounts_set",
    "current_total_duties_set",
    "current_total_price",
    "current_total_price_set",
    "current_total_tax",
    "current_total_tax_set",
    "discount_codes",
    "email",
    "estimated_taxes",
    "financial_status",
    "fulfillment_status",
    "landing_site",
    "location_id",
    "name",
    "number",
    "order_number",
    "order_status_url",
    "original_total_additional_fees_set",
    "original_total_duties_set",
    "payment_gateway_names",
    "phone",
    "po_number",
    "presentment_currency",
    "processed_at",
    "reference",
    "referring_site",
    "source_identifier",
    "source_name",
    "source_url",
    "subtotal_price",
    "subtotal_price_set",
    "tags",
    "tax_exempt",
    "tax_lines",
    "taxes_included",
    "test",
    # 'token',
    "total_discounts",
    "total_discounts_set",
    "total_line_items_price",
    "total_line_items_price_set",
    "total_outstanding",
    "total_price",
    "total_price_set",
    "total_shipping_price_set",
    "total_tax",
    "total_tax_set",
    "total_tip_received",
    "updated_at",
    "user_id",
    "billing_address",
    "customer",
    "discount_applications",
    "fulfillments",
    "line_items",
    "payment_terms",
    "refunds",
    "shipping_address",
    "shipping_lines",
]


class ImportShopifyPartnerDataOperator(
    LastSuccessfulDagrunMixin, DagRunTaskCommsMixin, FlattenJsonDictMixin, BaseOperator
):

    # Shopify API allows 2 requests per second for non-Plus plans
    MAX_REQUESTS_PER_SECOND = 2
    REQUEST_INTERVAL = 1 / MAX_REQUESTS_PER_SECOND  # Time interval between requests

    region_lookup = {"england": "ENG", "wales": "WLS", "scotland": "SCT", "northern ireland": "NIR"}

    """
    :param postgres_conn_id: postgres connection id
    :type postgres_conn_id: str
    :param schema: Schema name
    :type schema: str
    :param destination_schema: Schema name
    :type destination_schema: str
    :param destination_table: Table name
    :type destination_table: st
    :param partner_ref: partner reference
    :type partner_ref: str
    """

    ui_color = "#f9c915"

    def __init__(
        self,
        *,
        postgres_conn_id: str = "postgres_conn_id",
        schema: str,
        destination_schema: str,
        destination_table: str,
        partner_ref: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.log.info("Initialising ImportShopifyPartnerDataOperator")
        self.postgres_conn_id = postgres_conn_id
        self.schema = schema
        self.destination_schema = destination_schema
        self.destination_table = destination_table
        self.partner_ref = partner_ref
        self.separator = "__"
        self.last_successful_dagrun_xcom_key = "last_successful_dagrun_ts"
        self.next_page_url_key = f"{partner_ref}_next_page_url"
        self.discard_fields = []
        self.preserve_fields = [
            ("company", "string"),
            ("user_id", "Int64"),
            ("taxes_included", "bool"),
            ("confirmed", "bool"),
            ("test", "bool"),
            ("order_number", "Int64"),
        ]

        self.context = {
            "schema": schema,
            "destination_schema": destination_schema,
            "destination_table": destination_table,
            "partner_ref": partner_ref,
        }
        self.get_partner_config_template = f"""
        SELECT
            reference,
            name,
            partner_platform_api_access_token,
            partner_platform_base_url,
            partner_platform_api_version,
            allowed_region_for_harper,
            partner_shopify_app_type,
            partner_platform_api_key,
            partner_platform_api_secret

        FROM {self.schema}.partner
        WHERE partner_platform = 'shopify'
        AND reference = '{self.partner_ref}'
        """
        self.delete_template = """DO $$
        BEGIN
        IF EXISTS (
            SELECT FROM pg_tables WHERE schemaname = '{{destination_schema}}'
            AND tablename = '{{destination_table}}') THEN
            DELETE FROM {{ destination_schema }}.{{destination_table}}
            WHERE partner__name = '{{partner_ref}}'
            AND airflow_sync_ds = '{{ds}}'
            ;
        END IF;
        END $$;
        """

        self.log.info("Initialised ImportShopifyPartnerDataOperator")

    """def custom_wait(self, retry_state):
        exp_wait = wait_exponential(multiplier=1, min=4, max=60).sleep(retry_state)
        rand_wait = random.uniform(0, 5)  # Add up to 5 seconds of random wait
        return exp_wait + rand_wait"""

    def execute(self, context):
        hook = BaseHook.get_hook(self.postgres_conn_id)
        engine = self.get_postgres_sqlalchemy_engine(hook)
        ds = context["ds"]
        run_id = context["run_id"]
        total_docs_processed = 0
        filtered_out_orders = []

        try:
            with engine.connect() as conn:
                # Initial setup
                last_successful_dagrun_ts = self.get_last_successful_dagrun_ts(run_id=run_id)
                self.ensure_task_comms_table_exists(conn)
                next_page_url = self.get_next_page_url(conn, context)

                self.log.info(
                    f"Starting import for partner {self.partner_ref} from {last_successful_dagrun_ts}"
                    f" with next_page_url: {next_page_url}"
                )

                # Handle data cleanup if not continuing from previous run
                if not next_page_url:
                    self._clean_existing_partner_data(conn, ds)

                # Get partner configuration and setup Shopify session
                partner_config = self._get_partner_config(conn, context)
                self._setup_shopify_session(partner_config)
                allowed_regions = partner_config["allowed_region_for_harper"]

                # Set up date range for data fetch
                lte = context["data_interval_end"].to_iso8601_string()
                start_param = last_successful_dagrun_ts.to_iso8601_string()

                # Main processing loop
                while True:
                    # Fetch orders from Shopify
                    orders = self._fetch_shopify_orders(next_page_url, start_param, lte)

                    # Get next page URL before processing current batch
                    new_next_page_url = orders.next_page_url
                    self.log.info(f"Retrieved new next_page_url: {new_next_page_url}")
                    self.set_next_page_url(conn, context, new_next_page_url)

                    # Process current batch
                    valid_records = self._process_orders_batch(orders, allowed_regions, filtered_out_orders)

                    if valid_records:
                        self._write_records_to_database(valid_records, conn, ds)
                        total_docs_processed += len(valid_records)

                    # Check if we're done
                    if not new_next_page_url:
                        self.log.info("No more pages to process")
                        break

                    next_page_url = new_next_page_url

                # Final cleanup and index creation
                self._handle_completion(conn, total_docs_processed, filtered_out_orders, context)

            # Update task state
            self.set_last_successful_dagrun_ts(context)
            context["ti"].xcom_push(key="documents_found", value=total_docs_processed)
            self.log.info(f"Successfully completed import for partner {self.partner_ref}")

        except Exception as e:
            self.log.error(f"Error processing partner {self.partner_ref}: {str(e)}")
            raise

    def _clean_existing_partner_data(self, conn, ds):
        """Clean existing data for the partner before fresh import."""
        try:
            # Check if table exists first
            table_exists = conn.execute(
                f"""
                SELECT EXISTS (
                    SELECT FROM pg_tables
                    WHERE schemaname = '{self.destination_schema}'
                    AND tablename = '{self.destination_table}'
                );
                """
            ).scalar()

            if table_exists:
                before_count = conn.execute(
                    f"SELECT COUNT(*) FROM {self.destination_schema}.{self.destination_table} "
                    f"WHERE partner__name = '{self.partner_ref}'"
                ).scalar()
                self.log.info(f"Records for {self.partner_ref} before delete: {before_count}")

                self.delete_sql = render_template(self.delete_template, context={"ds": ds, **self.context})
                self.log.info(f"Executing delete SQL: {self.delete_sql}")
                conn.execute(self.delete_sql)

                after_count = conn.execute(
                    f"SELECT COUNT(*) FROM {self.destination_schema}.{self.destination_table} "
                    f"WHERE partner__name = '{self.partner_ref}'"
                ).scalar()
                self.log.info(f"Records for {self.partner_ref} after delete: {after_count}")
            else:
                self.log.info(
                    f"Table {self.destination_schema}.{self.destination_table} does not exist yet. No cleanup needed."
                )

        except Exception as e:
            self.log.warning(f"Error during cleanup for partner {self.partner_ref}: {str(e)}")
            # Continue execution since the table will be created during data write if it doesn't exist

    def _fetch_shopify_orders(self, next_page_url, start_param, lte):
        """Fetch orders from Shopify with retry logic."""
        while True:
            try:
                time.sleep(self.REQUEST_INTERVAL)

                if next_page_url:
                    return shopify.Order.find(from_=next_page_url)
                else:
                    query = {
                        "created_at_min": start_param,
                        "created_at_max": lte,
                        "limit": 100,
                        "status": "any",
                        "fields": ",".join(api_field_filter),
                    }
                    return shopify.Order.find(**query)

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    retry_after = int(e.response.headers.get("Retry-After", 5))
                    self.log.warning(f"Rate limit exceeded. Retrying after {retry_after} seconds.")
                    time.sleep(retry_after)
                    continue
                raise

    def _process_orders_batch(self, orders, allowed_regions, filtered_out_orders):
        """Process a batch of orders and return valid records."""
        self.log.info(f"Processing batch of {len(orders)} orders")
        valid_records = []

        for order in orders:
            order_dict = order.to_dict()
            if self._check_province_code(order_dict, allowed_regions):
                valid_records.append(order_dict)
            else:
                filtered_out_orders.append(order_dict)

        self.log.info(f"Found {len(valid_records)} valid orders in batch")
        return valid_records

    def _write_records_to_database(self, valid_records, conn, ds):
        """Transform and write records to database."""
        df = DataFrame(valid_records)
        df = self._preprocess_dataframe(df, ds)
        df = self._process_additional_fields(df)
        df = self.align_to_schema(df)

        self.log.info(f"Writing {len(df)} records to database")
        self.log.info(f"Sample of records: {df[['partner__name', 'id', 'created_at']].head()}")

        df.to_sql(
            self.destination_table,
            conn,
            if_exists="append",
            schema=self.destination_schema,
            index=False,
            chunksize=500,
        )

    def _handle_completion(self, conn, total_docs_processed, filtered_out_orders, context):
        """Handle completion tasks like creating indexes and logging summaries."""
        if filtered_out_orders:
            self.log.info(f"Total filtered-out orders: {len(filtered_out_orders)}")
        self.log.info(f"Total valid orders processed: {total_docs_processed}")

        if total_docs_processed > 0:
            conn.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {self.destination_table}_idx "
                f"ON {self.destination_schema}.{self.destination_table} (id);"
            )
        self.clear_task_vars(conn, context)

    def get_next_page_url(self, conn, context):
        return self.get_task_var(conn, context, self.next_page_url_key)

    def set_next_page_url(self, conn, context, next_page_url):
        return self.set_task_var(conn, context, self.next_page_url_key, next_page_url)

    def parse_json_field(self, field):
        """Parse JSON-like string fields into lists of dictionaries."""
        if isinstance(field, str):
            try:
                field = json.loads(field)
            except (ValueError, TypeError):
                self.log.error("Failed to parse JSON")
                field = []
        return field if isinstance(field, list) else []

    def convert_dict_columns_to_strings(self, df):
        """
        Converts dictionary and list columns in the DataFrame to JSON strings.

        Parameters:
            df (pd.DataFrame): The DataFrame with potential dictionary or list columns.

        Returns:
            pd.DataFrame: The DataFrame with dictionary and list columns converted to JSON strings.
        """
        # Identify columns with object data types that might contain dictionaries or lists
        object_columns = df.select_dtypes(include=["object"]).columns

        for col in object_columns:
            # Try to convert the column entries to JSON strings
            try:
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
            except Exception as e:
                print(f"Error converting column {col}: {e}")

        return df

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

    def _get_partner_config(self, conn, context):
        self.get_partner_config_sql = render_template(
            self.get_partner_config_template,
            context=context,
            extra_context=self.context,
        )
        partner_row = conn.execute(self.get_partner_config_sql).fetchone()
        if partner_row:
            self.api_access_token = partner_row["partner_platform_api_access_token"]
            self.base_url = partner_row["partner_platform_base_url"]
            self.api_version = partner_row["partner_platform_api_version"]
            self.shopify_app_type = partner_row["partner_shopify_app_type"]
            self.api_key = partner_row["partner_platform_api_key"]
            self.api_secret = partner_row["partner_platform_api_secret"]
            self.partner_reference = partner_row["reference"]
            self.partner_name = partner_row["name"]
            provinces_json = partner_row["allowed_region_for_harper"]
            provinces = json.loads(provinces_json)

            return {
                "partner_platform_api_access_token": self.api_access_token,
                "partner_platform_base_url": self.base_url,
                "partner_platform_api_version": self.api_version,
                "partner_shopify_app_type": self.shopify_app_type,
                "partner_platform_api_key": self.api_key,
                "partner_platform_api_secret": self.api_secret,
                "reference": self.partner_reference,
                "name": self.partner_name,
                "allowed_region_for_harper": provinces,
            }
        else:
            self.log.error("No partner details found.")
            raise AirflowException("No partner details found.")

    def _setup_shopify_session(self, partner_config):
        self.log.info(f"Setting up Shopify session for partner: {partner_config['name']}")
        self.log.info(f"Using base URL: {partner_config['partner_platform_base_url']}")
        self.log.info(f"API Version: {partner_config['partner_platform_api_version']}")
        self.log.info(f"App Type: {partner_config['partner_shopify_app_type']}")

        if partner_config is None:
            raise AirflowException("Partner configuration is missing or invalid")

        if partner_config["partner_shopify_app_type"] == "private":
            site_url = (
                f"https://{partner_config['partner_platform_api_key']}:"
                f"{partner_config['partner_platform_api_secret']}@"
                f"{partner_config['partner_platform_base_url']}/admin/api/"
                f"{partner_config['partner_platform_api_version']}"
            )
            shopify.ShopifyResource.set_site(site_url)
        else:
            site_url = (
                f"https://{partner_config['partner_platform_base_url']}/"
                f"admin/api/{partner_config['partner_platform_api_version']}"
            )
            shopify.ShopifyResource.set_site(site_url)
            shopify.ShopifyResource.set_headers(
                {"X-Shopify-Access-Token": partner_config["partner_platform_api_access_token"]}
            )

    def _check_province_code(self, order, allowed_regions):
        shipping_address = order.get("shipping_address", None)
        if shipping_address:
            province_code = shipping_address.get("province_code")
            allowed_province_codes = [
                self.region_lookup[region.lower()]
                for region in allowed_regions
                if region.lower() in self.region_lookup
            ]
            if province_code in allowed_province_codes:
                return True
        return False

    # @provide_session
    # def get_last_successful_dagrun_ts(self, run_id, session=None):
    #     run_id = run_id if isinstance(run_id, (str, int)) else str(run_id)

    #     query = XCom.get_many(
    #         include_prior_dates=True,
    #         dag_ids=self.dag_id,
    #         run_id=run_id,
    #         task_ids=self.task_id,
    #         key=self.last_successful_dagrun_xcom_key,
    #         session=session,
    #         limit=1,
    #     )

    #     xcom = query.first()
    #     self.log.info(f"xcom return: {xcom}, data type: {type(xcom)}")

    #     if xcom:
    #         value = xcom.value
    #         print(f"Retrieved XCom value: {value}")
    #         if isinstance(value, dict) and "timestamp" in value:
    #             timestamp = value["timestamp"]
    #             print(f"Timestamp retrieved from XCom: {timestamp} (Type: {type(timestamp)})")
    #             # Convert integer timestamp to datetime object
    #             if isinstance(timestamp, int):
    #                 return self.convert_from_int(timestamp)
    #             elif isinstance(timestamp, str):
    #                 # Handle string timestamp if needed
    #                 print(f"Handling string timestamp: {timestamp}")
    #                 return self.convert_from_str(timestamp)
    #         elif isinstance(value, int):
    #             return self.convert_from_int(value)
    #         elif isinstance(value, str):
    #             return self.convert_from_str(value)
    #     return None

    # def set_last_successful_dagrun_ts(self, context, timestamp):
    #     if isinstance(timestamp, datetime):
    #         timestamp_str = timestamp.isoformat()
    #     elif isinstance(timestamp, int):
    #         timestamp_str = self.convert_from_int(timestamp).isoformat()
    #     elif isinstance(timestamp, str):
    #         # Validate the string if needed
    #         try:
    #             self.convert_from_str(timestamp)
    #             timestamp_str = timestamp
    #         except ValueError:
    #             print(f"Invalid timestamp string: {timestamp}")
    #             raise ValueError("Timestamp must be a valid ISO 8601 string, integer, or datetime object.")
    #     else:
    #         print(f"Invalid timestamp type: {type(timestamp)}. Expected a string, integer, or datetime.")
    #         raise ValueError("Timestamp must be a string, integer, or datetime object.")

    #     print(f"Setting last successful DAG run timestamp: {timestamp_str}")

    #     # Push the timestamp string to XCom
    #     context["ti"].xcom_push(key=self.last_successful_dagrun_xcom_key, value={"timestamp": timestamp_str})

    def _preprocess_dataframe(self, df: pd.DataFrame, ds: str) -> pd.DataFrame:
        df.insert(0, "partner__name", self.partner_name)
        df.insert(1, "partner__reference", self.partner_reference)
        df["airflow_sync_ds"] = ds
        if self.discard_fields:
            df = df.drop(columns=[col for col in self.discard_fields if col in df.columns])
        return self.flatten_dataframe_columns_precisely(df)

    def _process_additional_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        # Ensure df is not None and is a DataFrame
        df["order_name"] = df["name"]
        for field in ["line_items", "fulfillments", "refunds"]:
            df[field] = df[field].apply(self.parse_json_field)

        df["items_ordered"] = df["line_items"].apply(lambda x: sum(item["quantity"] for item in x))
        df["items_fulfilled"] = df["fulfillments"].apply(
            lambda x: sum(
                sum(item.get("quantity", 0) for item in fulfillment.get("line_items", [])) for fulfillment in x
            )
        )
        df["items_returned"] = df["refunds"].apply(
            lambda x: sum(sum(item["quantity"] for item in refund["refund_line_items"]) for refund in x)
        )
        # Value ordered including discount and tax but not shipping
        df["total_price"] = pd.to_numeric(df["total_price"], errors="coerce")
        df["total_shipping_price_set__presentment_money__amount"] = pd.to_numeric(
            df["total_shipping_price_set__presentment_money__amount"], errors="coerce"
        )

        df["value_ordered"] = (
            df["total_price"] - df["total_shipping_price_set__presentment_money__amount"]
        )  # includes discount
        df["value_returned"] = df["refunds"].apply(
            lambda x: sum(sum(float(item["subtotal"]) for item in refund["refund_line_items"]) for refund in x)
        )
        df["fulfilled_at"] = df["fulfillments"].apply(lambda x: x[-1]["created_at"] if x else None)

        def get_harper_product(
            tags,
        ):  # This was incorrect before and therefore past data will be incorrect, needs backfill
            if not isinstance(tags, str):
                return None

            tags_lower = tags.lower()
            if "harper_try" in tags_lower or "harper:try" in tags_lower:
                return "harper_try"
            elif "harper_concierge" in tags_lower or "harper:concierge" in tags_lower:
                return "harper_concierge"
            return None

        # Add the new harper_product field
        df["harper_product"] = df["tags"].apply(get_harper_product)

        return df

    def align_to_schema(self, df):
        # Ensure date fields are stored as datetime
        for col in df.columns:
            if col.endswith("_at"):
                df[col] = pd.to_datetime(df[col], errors="coerce")
                # Convert dict columns to JSON strings

        df = self.convert_dict_columns_to_strings(df)

        # Clean and align columns
        df.columns = df.columns.str.lower()
        # print(df.head)

        for field, dtype in self.preserve_fields:
            if field not in df.columns:
                df[field] = None
            print(f"aligning column {field} as type {dtype}")
            df[field] = df[field].astype(dtype)

        for col in required_columns:
            if col not in df.columns:
                df[col] = None
        df = df[required_columns]
        return df

    # def convert_from_int(self, timestamp_int):
    #     # Convert Unix timestamp integer to datetime object in UTC
    #     print(f"Converting integer timestamp to datetime: {timestamp_int} (Type: {type(timestamp_int)})")
    #     return datetime.fromtimestamp(timestamp_int, tz=timezone.utc)

    # def convert_from_str(self, timestamp_str):
    #     # Convert ISO 8601 string to datetime object
    #     print(f"Converting ISO 8601 timestamp string to datetime: {timestamp_str} (Type: {type(timestamp_str)})")
    #     try:
    #         return datetime.fromisoformat(timestamp_str).replace(tzinfo=timezone.utc)
    #     except ValueError:
    #         print(f"Invalid ISO 8601 timestamp string: {timestamp_str}")
    #         raise ValueError(f"Invalid ISO 8601 timestamp string: {timestamp_str}")
