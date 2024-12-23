import time
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import requests
from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

from plugins.utils.render_template import render_template

from plugins.operators.mixins.flatten_json import FlattenJsonDictMixin
from plugins.operators.mixins.dag_run_task_comms_mixin import DagRunTaskCommsMixin


class ImportBigCommerceDataOperator(DagRunTaskCommsMixin, FlattenJsonDictMixin, BaseOperator):
    def __init__(
        self,
        *,
        postgres_conn_id: str = "postgres_conn_id",
        schema: str,
        destination_schema: str,
        destination_table: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.schema = schema
        self.destination_schema = destination_schema
        self.destination_table = destination_table
        self.separator = "__"
        self.last_successful_dagrun_xcom_key = "last_successful_dagrun_ts"

        self.context = {
            "schema": schema,
            "destination_schema": destination_schema,
            "destination_table": destination_table,
        }

        self.get_partner_config_template = f"""
        SELECT
            _api_store_id,
            partner_platform_api_access_token
        FROM {self.schema}.partner
        WHERE partner_platform = 'bigcommerce'
        """

        self.delete_template = """
        DELETE FROM {{ destination_schema }}.{{destination_table}}
        WHERE airflow_sync_ds = '{{ds}}'
        """

    def execute(self, context):
        hook = BaseHook.get_hook(self.postgres_conn_id)
        engine = create_engine(hook.get_uri())
        ds = context["ds"]

        try:
            with engine.connect() as conn:
                # Get partner configuration
                partner_config = self._get_partner_config(conn)
                base_url = f"https://api.bigcommerce.com/stores/{partner_config['_api_store_id']}/v2"
                headers = {
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "X-Auth-Token": partner_config["partner_platform_api_access_token"],
                }

                # Clean existing data
                self._clean_existing_data(conn, ds)

                # Fetch and process orders
                all_orders = []
                page = 1

                while True:
                    orders = self._fetch_orders(base_url, headers, page)
                    if not orders:
                        break

                    all_orders.extend(orders)
                    page += 1
                    time.sleep(0.5)  # Rate limiting

                if all_orders:
                    # Process and write data
                    df = self._process_orders(all_orders, ds)
                    self._write_to_database(df, conn)

                    # Update task state
                    context["ti"].xcom_push(key="documents_found", value=len(all_orders))

                self.log.info(f"Successfully processed {len(all_orders)} orders")

        except Exception as e:
            self.log.error(f"Error processing BigCommerce data: {str(e)}")
            raise

    def _get_partner_config(self, conn):
        result = conn.execute(self.get_partner_config_template).fetchone()
        if not result:
            raise AirflowException("No BigCommerce partner configuration found")
        return dict(result)

    def _clean_existing_data(self, conn, ds):
        delete_sql = render_template(self.delete_template, context={"ds": ds, **self.context})
        conn.execute(delete_sql)

    def _fetch_orders(self, base_url: str, headers: Dict[str, str], page: int) -> List[Dict[str, Any]]:
        """Fetch orders from BigCommerce API with pagination"""
        try:
            response = requests.get(f"{base_url}/orders", headers=headers, params={"page": page, "limit": 250})
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.log.error(f"Error fetching orders: {str(e)}")
            raise

    def _filter_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter DataFrame to include only desired columns."""
        desired_columns = [
            "id",
            "customer_id",
            "date_created",
            "status_id",
            "status",
            "subtotal_ex_tax",
            "subtotal_inc_tax",
            "subtotal_tax",
            "shipping_cost_inc_tax",
            "shipping_cost_tax",
            "total_inc_tax",
            "total_tax",
            "items_total",
            "items_shipped",
            "payment_status",
            "refunded_amount",
            "geoip_country",
            "geoip_country_iso2",
            "currency_code",
            "discount_amount",
            "order_source",
            "channel_id",
            "external_source",
            "external_id",
            "staff_notes",
            "external_order_id",
            "payment_method",
        ]
        filtered_df = df[desired_columns]
        filtered_df["total_inc_tax_noshipping"] = filtered_df["total_inc_tax"] - filtered_df["shipping_cost_inc_tax"]
        return filtered_df

    def _process_harper_flags(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process harper flags and filter to GB orders."""
        df["harper"] = df["staff_notes"].str.contains("harper", case=False, na=False).astype(int)
        df.loc[df["harper"] == 1, "geoip_country_iso2"] = "GB"
        return df[df["geoip_country_iso2"] == "GB"]

    def _process_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert dates and add month column."""
        df["date_created"] = pd.to_datetime(df["date_created"])
        df["month"] = df["date_created"].dt.to_period("M")
        return df

    def _merge_harper_orders(self, df: pd.DataFrame) -> pd.DataFrame:
        """Merge with harper orders from thefold.csv."""
        try:
            harper_orders = pd.read_csv("thefold.csv")
            df["id"] = df["id"].astype(str)
            return df.merge(harper_orders, how="left", left_on="id", right_on="original_order_name")
        except FileNotFoundError:
            self.log.warning("thefold.csv not found, continuing without harper order data")
            return df

    def _add_channel_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add channel and harper product information."""
        df["channel"] = np.where(df["harper"] == 1, "harper", "Online")
        df["harper_product"] = np.where(
            (df["harper"] == 1) & (df["order_type"] == "harper_try"),
            "harper_try",
            np.where((df["harper"] == 1) & (df["order_type"] != "harper_try"), "harper_concierge", " "),
        )
        return df

    def _calculate_aggregate_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate metrics for all channels and by channel."""

        def aggregate_data(group_df, groupby_cols=None):
            if groupby_cols is None:
                groupby_cols = ["month"]
            return (
                group_df.groupby(groupby_cols)
                .agg(
                    number_of_orders=("id", "count"),
                    total_value_ordered=("total_inc_tax_noshipping", "sum"),
                    refunded_value=("refunded_amount", "sum"),
                    total_items_ordered=("items_total", "sum"),
                )
                .reset_index()
            )

        # Calculate for all channels
        all_channels_df = aggregate_data(df)
        all_channels_df["channel"] = "all channels"
        all_channels_df["harper_product"] = " "

        # Calculate by channel
        grouped_df = aggregate_data(df, ["month", "channel", "harper_product"])

        return pd.concat([grouped_df, all_channels_df])

    def _calculate_final_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate final metrics and round values."""
        df["total_value_purchased"] = df["total_value_ordered"] - df["refunded_value"]
        df["gross_atv"] = df["total_value_ordered"] / df["number_of_orders"]
        df["gross_upt"] = df["total_items_ordered"] / df["number_of_orders"]
        df["gross_asp"] = df["total_value_ordered"] / df["total_items_ordered"]
        df["net_atv"] = df["total_value_purchased"] / df["number_of_orders"]

        numeric_columns = df.select_dtypes(include=["float", "int"]).columns
        df[numeric_columns] = df[numeric_columns].applymap(lambda x: round(x, 2))
        return df

    def _process_orders(self, orders: List[Dict[str, Any]], ds: str) -> pd.DataFrame:
        """Process orders with the same logic as thefold2.py"""
        # Initial DataFrame creation
        df = pd.DataFrame(orders)
        df["airflow_sync_ds"] = ds
        df = self.flatten_dataframe_columns_precisely(df)

        # Process the data through each step
        df = self._filter_columns(df)
        df = self._process_harper_flags(df)
        df = self._process_dates(df)
        df = self._merge_harper_orders(df)
        df = self._add_channel_info(df)
        df = self._calculate_aggregate_metrics(df)
        df = self._calculate_final_metrics(df)

        return df

    def _write_to_database(self, df: pd.DataFrame, conn) -> None:
        """Write processed data to database"""
        df.to_sql(
            self.destination_table,
            conn,
            schema=self.destination_schema,
            if_exists="append",
            index=False,
            chunksize=500,
        )
