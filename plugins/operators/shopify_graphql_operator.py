import json
import time
from typing import Dict, List

import pandas as pd
import requests
from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

from plugins.utils.render_template import render_template

from plugins.operators.mixins.last_successful_dagrun import LastSuccessfulDagrunMixin
from plugins.operators.mixins.dag_run_task_comms_mixin import DagRunTaskCommsMixin


class ShopifyGraphQLPartnerDataOperator(LastSuccessfulDagrunMixin, DagRunTaskCommsMixin, BaseOperator):
    """
    Operator for fetching Shopify data using GraphQL API
    """

    MAX_RETRIES = 3
    RATE_LIMIT_DELAY = 1.0  # 1 second between requests

    # Test query for checking customer access
    TEST_CUSTOMER_ACCESS_QUERY = """
    query($query: String!) {
        orders(first: 1, query: $query) {
            edges {
                node {
                    customer {
                        id
                        createdAt
                        updatedAt
                        tags
                    }
                }
            }
        }
    }
    """

    def __init__(
        self,
        *,
        postgres_conn_id: str = "postgres_conn_id",
        schema: str,
        destination_schema: str,
        destination_table: str,
        partner_ref: str,
        rebuild: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.schema = schema
        self.destination_schema = destination_schema
        self.destination_table = destination_table
        self.partner_ref = partner_ref
        self.rebuild = rebuild
        self.separator = "__"
        self.last_successful_dagrun_xcom_key = "last_successful_dagrun_ts"
        self.last_successful_cursor_key = f"last_successful_cursor_{partner_ref}"
        self.has_customer_access = False  # Initialize customer access flag

        self.context = {
            "schema": schema,
            "destination_schema": destination_schema,
            "destination_table": destination_table,
            "partner_ref": partner_ref,
        }

        # Template for fetching partner configuration
        self.get_partner_config_template = f"""
        SELECT
            reference,
            name,
            partner_platform_api_access_token,
            partner_platform_base_url,
            partner_platform_api_version,
            allowed_region_for_harper,
            partner_platform_api_key,
            partner_platform_api_secret,
            partner_shopify_app_type

        FROM {self.schema}.partner
        WHERE partner_platform = 'shopify'
        AND reference = '{self.partner_ref}'
        """

        self.delete_template = """DO $$
        BEGIN
        IF EXISTS (
            SELECT FROM pg_tables WHERE schemaname = '{{destination_schema}}'
            AND tablename = '{{destination_table}}'
            ) THEN
            DELETE FROM {{destination_schema}}.{{destination_table}}
            WHERE partner_reference = '{{partner_ref}}'
            ;
        END IF;
        END $$;
        """

        self.customer_section = (
            """
            customer {
                id
                createdAt
                updatedAt
                tags
                numberOfOrders
                amountSpent {
                    amount
                    currencyCode
                }
            }
            """
            if self.has_customer_access
            else ""
        )

        self.main_query = f"""
        query($query: String!, $after: String) {{
            orders(first: 250, after: $after, query: $query) {{
                pageInfo {{
                    hasNextPage
                    endCursor
                }}
                edges {{
                    node {{
                        # Basic Order Info
                        publication {{
                            name
                            app {{
                                title
                            }}
                        }}
                        id
                        name
                        createdAt
                        updatedAt
                        currencyCode
                        cancelledAt
                        cancelReason
                        displayFulfillmentStatus
                        displayFinancialStatus

                        # Totals and Pricing
                        currentSubtotalLineItemsQuantity
                        taxesIncluded

                        # Money Sets
                        # Money Sets
                        currentSubtotalPriceSet {{
                            presentmentMoney {{
                                amount
                                currencyCode
                            }}
                            shopMoney {{
                                amount
                                currencyCode
                            }}
                        }}
                        currentTotalPriceSet {{
                            presentmentMoney {{
                                amount
                                currencyCode
                            }}
                            shopMoney {{
                                amount
                                currencyCode
                            }}
                        }}
                        currentTotalTaxSet {{
                            presentmentMoney {{
                                amount
                                currencyCode
                            }}
                            shopMoney {{
                                amount
                                currencyCode
                            }}
                        }}
                        currentTotalDiscountsSet {{
                            presentmentMoney {{
                                amount
                                currencyCode
                            }}
                            shopMoney {{
                                amount
                                currencyCode
                            }}
                        }}
                        totalDiscountsSet {{
                            presentmentMoney {{
                                amount
                                currencyCode
                            }}
                            shopMoney {{
                                amount
                                currencyCode
                            }}
                        }}
                        totalPriceSet {{
                            presentmentMoney {{
                                amount
                                currencyCode
                            }}
                            shopMoney {{
                                amount
                                currencyCode
                            }}
                        }}
                        totalRefundedSet {{
                            presentmentMoney {{
                                amount
                                currencyCode
                            }}
                            shopMoney {{
                                amount
                                currencyCode
                            }}
                        }}
                        totalTaxSet {{
                            presentmentMoney {{
                                amount
                                currencyCode
                            }}
                            shopMoney {{
                                amount
                                currencyCode
                            }}
                        }}


                        # Line Items
                        lineItems(first: 100) {{
                            edges {{
                                node {{
                                    id
                                    quantity
                                    sku
                                    title
                                }}
                            }}
                        }}

                        refunds {{
                            refundLineItems(first: 50) {{
                                edges {{
                                    node {{
                                        quantity
                                        priceSet {{
                                            shopMoney {{
                                                amount
                                                currencyCode
                                            }}
                                        }}
                                    }}
                                }}
                            }}
                        }}

                        # Shipping Address
                        shippingAddress {{
                            city
                            country
                            countryCode
                            province
                            provinceCode
                        }}

                        # Shipping Line
                        shippingLines(first: 10) {{
                            edges {{
                                node {{
                                    title
                                    discountedPriceSet {{
                                        shopMoney {{
                                            amount
                                            currencyCode
                                        }}
                                    }}
                                }}
                            }}
                        }}

                        # Payment Info
                        paymentGatewayNames

                        # Additional Fields
                        test
                        tags

                        {self.customer_section}
                        # Dynamically insert customer section here if has_customer_access is True

                    }}
                }}
            }}
        }}
        """

    def execute(self, context):
        hook = BaseHook.get_hook(self.postgres_conn_id)
        engine = create_engine(hook.get_uri())
        self.log.info(f"Destination Table: {self.destination_schema}.{self.destination_table}")

        try:
            with engine.connect() as conn:
                run_id = context["run_id"]
                self.partner_config = self._get_partner_config(conn)

                # Ensure task communications table exists
                self.ensure_task_comms_table_exists(conn)

                # If rebuilding,
                if self.rebuild:
                    after = None
                    start_param = context["dag"].default_args["start_date"].strftime("%Y-%m-%d")

                else:
                    # Get cursor from previous run if exists
                    after = self.get_last_successful_cursor(conn, context)
                    # self.log.info(f"Continuing from cursor: {after}")

                    # Get last successful dagrun timestamp using the mixin
                    last_successful_dagrun_ts = self.get_last_successful_dagrun_ts(run_id=run_id)

                    self.log.info(f"last successful run timestamp: {last_successful_dagrun_ts}")
                    start_param = last_successful_dagrun_ts.format("YYYY-MM-DD")

                lte = context["data_interval_end"].format("YYYY-MM-DD")

                if after:
                    self.log.info(f"Continuing from cursor: {after}")
                    self.log.info(f"Fetching orders from {start_param} to {lte}")
                else:
                    self.log.info(f"Starting fresh fetch from {start_param} to {lte}")
                    self._clean_existing_partner_data(conn)

                # Setup shop URL and access token
                self.shop_url = f"https://{self.partner_config['partner_platform_base_url']}"
                self.access_token = self.partner_config["partner_platform_api_access_token"]

                # orders, has_customer_access = self._fetch_all_orders(conn, context, start_param, lte, after)
                orders = self._fetch_and_process_orders(conn, context, start_param, lte, after)

                # Update last successful run timestamp
                self.set_last_successful_dagrun_ts(context)

                return (orders) if orders else 0

        except Exception as e:
            self.log.error(f"Error processing partner {self.partner_ref}: {str(e)}")
            raise

    def _get_partner_config(self, conn) -> Dict:
        result = conn.execute(self.get_partner_config_template).fetchone()
        if not result:
            raise AirflowException(f"No configuration found for partner {self.partner_ref}")
        return dict(result)

    def _make_graphql_request(self, query: str, variables: Dict = None) -> Dict:
        # Log the details before making the request
        # self.log.info(f"Shop URL: {self.shop_url}")
        # self.log.info(f"Query: {query}")
        # elf.log.info(f"Variables: {json.dumps(variables, indent=2) if variables else 'None'}")

        headers = {
            "X-Shopify-Access-Token": self.access_token,
            "Content-Type": "application/json",
        }
        if self.partner_config["partner_shopify_app_type"] == "private":
            # Construct URL with auth credentials in it
            url = (
                f"https://{self.partner_config['partner_platform_api_key']}"
                f":{self.partner_config['partner_platform_api_secret']}"
                f"@{self.partner_config['partner_platform_base_url']}/admin/api/"
                f"{self.partner_config['partner_platform_api_version']}/graphql.json"
            )
            auth = None  # Don't use separate auth since it's in URL
        else:
            url = (
                f"https://{self.partner_config['partner_platform_base_url']}"
                f"/admin/api/{self.partner_config['partner_platform_api_version']}/graphql.json"
            )
            headers["X-Shopify-Access-Token"] = self.partner_config["partner_platform_api_access_token"]
            auth = None

        for attempt in range(self.MAX_RETRIES):
            try:
                payload = {"query": query}
                if variables:
                    payload["variables"] = variables

                response = requests.post(url, headers=headers, json=payload, auth=auth)
                response.raise_for_status()

                time.sleep(self.RATE_LIMIT_DELAY)
                return response.json()

            except requests.exceptions.RequestException as e:
                self.log.error(f"Request attempt {attempt + 1} failed: {str(e)}")
                if attempt == self.MAX_RETRIES - 1:
                    raise AirflowException(f"Failed to fetch data from Shopify: {str(e)}")
                # time.sleep(2**attempt)

        # return url

    def _fetch_and_process_orders(self, conn, context, start_param: str, lte: str, after: str = None) -> int:
        """
        Fetch all orders within the given date range using Shopify's GraphQL API.
        Process and write orders in batches as they are retrieved.

        Args:
            conn: Database connection
            context: Airflow context
            start_param: Start date for fetching orders
            lte: End date for fetching orders
            after: Cursor for pagination

        Returns:
            int: Total number of orders processed
        """
        total_processed = 0
        has_next_page = True
        page_count = 0

        # Initialize empty DataFrame
        df = pd.DataFrame()
        page_orders = []

        # Check if we have customer access first
        self.log.info("Testing for customer data access...")
        test_variables = {"query": f"updated_at:>={start_param} AND updated_at:<={lte}"}
        test_result = self._make_graphql_request(self.TEST_CUSTOMER_ACCESS_QUERY, test_variables)
        self.has_customer_access = "errors" not in test_result
        self.log.info(f"Customer data access: {'Available' if self.has_customer_access else 'Not available'}")

        # Calculate the date for 550 days ago
        five_hundred_fifty_days_ago = (pd.Timestamp.now() - pd.DateOffset(days=550)).strftime("%Y-%m-%d")

        while has_next_page:
            page_count += 1
            self.log.info(f"Fetching page {page_count}")

            # Add date optimization and test order filter
            order_query = (
                f"updated_at:>={start_param} AND updated_at:<={lte} "
                f"AND created_at:>={five_hundred_fifty_days_ago} "
                f'AND NOT source_name:"Point of Sale" '
                f"AND shipping_address_country_code:GB "
                f'AND (app_title:"Harper Concierge" OR app_title:"Harper" OR app_title:"Online Store") '
                f"AND test:false "
                f"AND updated_at:<now "
            )

            variables = {"query": order_query, "after": after}
            self.log.info(f"Fetching orders with variables: {variables}")

            try:
                result = self._make_graphql_request(self.main_query, variables)

                if "errors" in result:
                    error_message = result.get("errors", [{}])[0].get("message", "Unknown GraphQL error")
                    raise AirflowException(f"GraphQL query failed: {error_message}")

                data = result["data"]["orders"]
                page_orders = [edge["node"] for edge in data["edges"]]

                if page_orders:
                    df = self._transform_to_dataframe(page_orders, self.has_customer_access)
                    df = self._process_dataframe(df, self.partner_config)
                    self.log.info(f"Writing {len(page_orders)} orders from page {page_count}")

                    df.to_sql(
                        self.destination_table,
                        conn,
                        schema=self.destination_schema,
                        if_exists="append",
                        index=False,
                        chunksize=500,
                    )

                    total_processed += len(page_orders)
                    self.log.info(f"Processed {len(page_orders)} orders on page {page_count}")

                    self.clear_task_vars(conn, context)

                    # Update pagination info
                    has_next_page = data["pageInfo"]["hasNextPage"]
                    after = data["pageInfo"]["endCursor"]

                    if has_next_page:
                        # Store cursor for recovery if there's more data to fetch
                        self.set_last_successful_cursor(conn, context, after)
                        self.log.info(f"Stored cursor for recovery: {after}")
                    else:
                        # Clear cursor when we've successfully processed all pages
                        self.clear_task_vars(conn, context)
                        self.log.info("Successfully completed all pages, cleared cursor")

                else:
                    self.log.info("No orders found in the specified date range")
                    has_next_page = False  # Exit loop if no orders found

            except Exception as e:
                self.log.error(f"Error fetching/processing orders on page {page_count}: {str(e)}")
                raise

            time.sleep(self.RATE_LIMIT_DELAY)  # Respect rate limits

        self.log.info(f"Completed processing {total_processed} total orders")
        return total_processed

    def _transform_to_dataframe(self, page_orders: List[Dict], has_customer_access: bool) -> pd.DataFrame:
        """
        Transform a page of orders into a pandas DataFrame.

        Args:
            page_orders: List of order dictionaries from the current page
            has_customer_access: Boolean indicating if we have access to customer data

        Returns:
            pd.DataFrame: Transformed orders data
        """
        flattened_orders = []

        for order in page_orders:
            # Safely get publication info with fallbacks for None values
            publication = order.get("publication", {}) or {}
            publication_app = publication.get("app", {}) or {}
            shipping_address = order.get("shippingAddress") or {}

            # Safely get shipping line info
            shipping_lines = order.get("shippingLines", {}).get("edges", [])
            shipping_cost = 0.0
            shipping_cost_currency = None
            if shipping_lines:
                shipping_line = shipping_lines[0].get("node", {})
                shipping_cost = self._safe_float(
                    shipping_line.get("discountedPriceSet", {}).get("shopMoney", {}).get("amount")
                )
                shipping_cost_currency = (
                    shipping_line.get("discountedPriceSet", {}).get("shopMoney", {}).get("currencyCode")
                )

            flat_order = {
                # Basic Order Info
                "publication_name": publication.get("name"),
                "app_title": publication_app.get("title"),
                "id": order.get("id"),
                "name": order.get("name"),
                "created_at": order.get("createdAt"),
                "updated_at": order.get("updatedAt"),
                "cancelled_at": order.get("cancelledAt"),
                "is_cancelled": 1 if order.get("cancelledAt") and order.get("cancelledAt").strip() else 0,
                "cancel_reason": order.get("cancelReason"),
                "display_fulfillment_status": order.get("displayFulfillmentStatus"),
                "display_financial_status": order.get("displayFinancialStatus"),
                # Shipping Address - with null safety
                "shipping_city": shipping_address.get("city"),
                "shipping_country": shipping_address.get("country"),
                "shipping_country_code": shipping_address.get("countryCode"),
                "shipping_province": shipping_address.get("province"),
                "shipping_province_code": shipping_address.get("provinceCode"),
                "shipping_cost": shipping_cost,
                "shipping_cost_currency": shipping_cost_currency,
                # Additional fields
                "customer_id": None,
                "customer_created_at": None,
                "customer_updated_at": None,
                "customer_state": None,
                "customer_tags": None,
                "customer_total_spent": 0.0,
                "customer_total_spent_currency": None,
                "customer_numberOfOrders": 0.0,
                # "total_refund_amount": 0.0,
                "total_refund_quantity": 0,
                "tags": order.get("tags"),
                "test": order.get("test", False),
                # Payment Info
                "payment_gateway_names": order["paymentGatewayNames"],
                # Safe access for monetary fields
                # Update the monetary values in flat_order
                "current_subtotal_price": self._safe_float(
                    order.get("currentSubtotalPriceSet", {}).get("presentmentMoney", {}).get("amount")
                ),
                "current_subtotal_price_currency": order.get("currentSubtotalPriceSet", {})
                .get("presentmentMoney", {})
                .get("currencyCode"),
                "current_total_price": self._safe_float(
                    order.get("currentTotalPriceSet", {}).get("shopMoney", {}).get("amount")
                ),
                "current_total_price_currency": order.get("currentTotalPriceSet", {})
                .get("shopMoney", {})
                .get("currencyCode"),
                "current_total_tax": self._safe_float(
                    order.get("currentTotalTaxSet", {}).get("shopMoney", {}).get("amount")
                ),
                "current_total_tax_currency": order.get("currentTotalTaxSet", {})
                .get("shopMoney", {})
                .get("currencyCode"),
                "current_total_discounts": self._safe_float(
                    order.get("currentTotalDiscountsSet", {}).get("shopMoney", {}).get("amount")
                ),
                "current_total_discounts_currency": order.get("currentTotalDiscountsSet", {})
                .get("shopMoney", {})
                .get("currencyCode"),
                "total_refunded": self._safe_float(
                    order.get("totalRefundedSet", {}).get("shopMoney", {}).get("amount")
                ),
                "total_refunded_currency": order.get("totalRefundedSet", {}).get("shopMoney", {}).get("currencyCode"),
                "total_price": self._safe_float(order.get("totalPriceSet", {}).get("shopMoney", {}).get("amount")),
                "total_price_currency": order.get("totalPriceSet", {}).get("shopMoney", {}).get("currencyCode"),
                "total_tax": self._safe_float(order.get("totalTaxSet", {}).get("shopMoney", {}).get("amount")),
                "total_tax_currency": order.get("totalTaxSet", {}).get("shopMoney", {}).get("currencyCode"),
                "total_discounts": self._safe_float(
                    order.get("totalDiscountsSet", {}).get("shopMoney", {}).get("amount")
                ),
                "total_discounts_currency": order.get("totalDiscountsSet", {})
                .get("shopMoney", {})
                .get("currencyCode"),
                # Calculate total quantity from line items
                "items_quantity": (
                    sum(edge["node"]["quantity"] for edge in order["lineItems"]["edges"])
                    if order.get("lineItems", {}).get("edges")
                    else 0
                ),
            }

            # Add customer fields if we have access
            if has_customer_access:
                customer = order.get("customer", {})
                if customer is not None:  # Only process if customer exists
                    amount_spent = customer.get("amountSpent", {}) or {}
                    flat_order.update(
                        {
                            "customer_id": customer.get("id"),
                            "customer_created_at": customer.get("createdAt"),
                            "customer_updated_at": customer.get("updatedAt"),
                            "customer_numberOfOrders": customer.get("numberOfOrders", 0),
                            "customer_tags": customer.get("tags"),
                            "customer_total_spent": self._safe_float(amount_spent.get("amount")),
                            "customer_total_spent_currency": amount_spent.get("currencyCode"),
                        }
                    )

            # Process refunds
            try:
                # total_refund_amount = 0.0
                total_refund_quantity = 0
                refunds = order.get("refunds", []) or []

                for refund in refunds:
                    for refund_edge in refund.get("refundLineItems", {}).get("edges", []):
                        refund_node = refund_edge.get("node", {})
                        total_refund_quantity += refund_node.get("quantity", 0)
                        # price_set = refund_node.get("priceSet", {}).get("shopMoney", {})
                    # total_refund_amount += self._safe_float(price_set.get("amount"))

                flat_order.update(
                    {
                        # "total_refund_amount": total_refund_amount,
                        "total_refund_quantity": total_refund_quantity,
                    }
                )
            except Exception as e:
                self.log.warning(f"Error processing refunds for order {id}: {str(e)}")
                flat_order.update(
                    {
                        # "total_refund_amount": 0.0,
                        "total_refund_quantity": 0,
                    }
                )

            flattened_orders.append(flat_order)

        # Convert the flattened data to a DataFrame
        df = pd.DataFrame(flattened_orders)
        return df

    def _process_dataframe(self, df: pd.DataFrame, partner_config: Dict) -> pd.DataFrame:
        # Add partner information
        df["partner_name"] = partner_config["name"]
        df["partner_reference"] = partner_config["reference"]
        # df["airflow_sync_ds"] = ds

        df["total_price_ex_shipping"] = df["total_price"] - df["shipping_cost"]

        # Calculate net values
        n_cols = len(df.columns)
        df.insert(n_cols - 4, "net_price", (df["total_price"] - df["shipping_cost"] - df["total_refunded"]))
        df.insert(n_cols - 5, "net_items_quantity", (df["items_quantity"] - df["total_refund_quantity"]))

        # Convert date columns
        date_columns = [
            "created_at",
            "updated_at",
            "customer_created_at",
            "customer_updated_at",
        ]
        # Safely convert dates with error handling
        for col in date_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                except Exception as e:
                    self.log.warning(f"Error converting {col} to datetime: {str(e)}")
                    df[col] = pd.NaT

        def determine_harper_product(tags):
            """
            Determine harper product from complex tag strings.

            Args:
                tags: String of tags which can include quoted strings, colons, and special characters

            Returns:
                str: 'harper_try', 'harper_concierge', or None
            """
            # Handle empty/null cases
            if not tags or not isinstance(tags, str):
                return None

            try:
                # Remove curly braces if present
                tags_str = tags.strip().lstrip("{").rstrip("}")
                if not tags_str:
                    return None

                # Split tags handling quoted strings
                parts = []
                current_part = []
                in_quotes = False

                for char in tags_str:
                    if char == '"' and not current_part:
                        in_quotes = True
                    elif char == '"' and in_quotes:
                        in_quotes = False
                    elif char == "," and not in_quotes:
                        if current_part:
                            parts.append("".join(current_part).strip())
                        current_part = []
                    else:
                        current_part.append(char)

                # Add the last part
                if current_part:
                    parts.append("".join(current_part).strip())

                # Clean and lowercase all parts
                tags_list = [part.strip().lower() for part in parts if part.strip()]

                # Debug log
                self.log.debug(f"Parsed tags: {tags_list}")

                # First check for try product
                if any(any(marker in tag for marker in ["harper:try", "harper_try"]) for tag in tags_list):
                    return "harper_try"

                # Then check for concierge indicators
                concierge_markers = [
                    "harper:concierge",
                    "harper_concierge",
                    "harper complete",
                    "harper:complete",
                    "harper_concierge:complete",
                    "Harper",
                ]

                if any(any(marker in tag for marker in concierge_markers) for tag in tags_list):
                    return "harper_concierge"

                # Finally check for generic harper tag
                if any("harper" in tag for tag in tags_list):
                    return "harper_concierge"

                return None

            except Exception as e:
                self.log.error(f"Error processing tags {tags}: {str(e)}")
                return None

        self.log.info("Processing tags to determine Harper product...")
        self.log.info(f"Sample of first few tags: {df['tags'].head()}")

        df["harper_product"] = df["tags"].apply(determine_harper_product)

        # Log the distribution of harper products
        self.log.info("Harper product distribution:")
        self.log.info(df["harper_product"].value_counts(dropna=True))

        # Filter by allowed regions
        allowed_regions = json.loads(partner_config["allowed_region_for_harper"])
        region_lookup = {"england": "ENG", "wales": "WLS", "scotland": "SCT", "northern ireland": "NIR"}
        allowed_codes = [
            region_lookup[region.lower()] for region in allowed_regions if region.lower() in region_lookup
        ]
        df = df[df["shipping_province_code"].isin(allowed_codes)]

        return df

    def _safe_float(self, value) -> float:
        """Safely convert value to float, returning 0.0 if conversion fails."""
        if value is None:
            return 0.0
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0

    def _write_to_database(self, df: pd.DataFrame, conn) -> None:
        # Check if table exists and delete existing records for this partner/day
        # ds = df["airflow_sync_ds"].iloc[0] if not df.empty else None
        # self._clean_existing_partner_data(self, conn)

        self.log.info(f"Destination Table: {self.destination_schema}.{self.destination_table}")

        # Write new records
        # Write new records
        df.to_sql(
            self.destination_table,
            conn,
            schema=self.destination_schema,
            if_exists="append",
            index=False,
            chunksize=500,
        )

        # Create index if it doesn't exist
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS {self.destination_table}_idx "
            f"ON {self.destination_schema}.{self.destination_table} (id);"
        )

    def _clean_existing_partner_data(self, conn):
        """Clean existing data for the partner before a fresh import."""
        self.delete_sql = render_template(self.delete_template, context=self.context)
        self.log.info(f"Cleaning existing data for partner {self.partner_ref} with SQL: {self.delete_sql}")
        # Execute the delete SQL query
        conn.execute(self.delete_sql)

        # Log the completion of the cleanup
        self.log.info(f"Successfully cleared existing data for partner {self.partner_ref}")

    def set_last_successful_cursor(self, conn, context, cursor):
        return self.set_task_var(conn, context, self.last_successful_cursor_key, cursor)

    def get_last_successful_cursor(self, conn, context):
        return self.get_task_var(conn, context, self.last_successful_cursor_key)
