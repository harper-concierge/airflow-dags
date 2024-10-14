import pandas as pd
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import Metric, DateRange, Dimension, RunReportRequest
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class GA4ToGoogleSheetOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        property_id,
        spreadsheet_id,
        worksheet,
        google_sheets_conn_id,
        google_analytics_conn_id,
        start_date,
        end_date,
        *args,
        **kwargs,
    ):
        super(GA4ToGoogleSheetOperator, self).__init__(*args, **kwargs)
        self.property_id = property_id
        self.spreadsheet_id = spreadsheet_id
        self.worksheet = worksheet
        self.google_sheets_conn_id = google_sheets_conn_id
        self.google_analytics_conn_id = google_analytics_conn_id
        self.start_date = start_date
        self.end_date = end_date

    def run_ga4_report(self, client, start_date, end_date, offset=0):
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=[
                Dimension(name="date"),
                Dimension(name="city"),
                Dimension(name="sessionSourceMedium"),
                Dimension(name="customEvent:partner_reference"),
                Dimension(name="customEvent:service_type"),
            ],
            metrics=[
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

    def write_to_sheets(self, sheets_hook, data):
        spreadsheet = sheets_hook.get_spreadsheet(self.spreadsheet_id)
        sheet_exists = any(sheet["properties"]["title"] == self.worksheet for sheet in spreadsheet.get("sheets", []))

        if not sheet_exists:
            create_request_body = {
                "requests": [
                    {
                        "addSheet": {
                            "properties": {
                                "title": self.worksheet,
                            }
                        }
                    }
                ]
            }
            sheets_hook.get_conn().spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id, body=create_request_body
            ).execute()
        else:
            sheets_hook.clear(spreadsheet_id=self.spreadsheet_id, range_=f"{self.worksheet}")

        sheets_hook.update_values(
            spreadsheet_id=self.spreadsheet_id,
            range_=f"{self.worksheet}!A1",
            values=data,
        )

    def execute(self, context):
        ga4_hook = GoogleBaseHook(gcp_conn_id=self.google_analytics_conn_id)
        ga4_credentials = ga4_hook._get_credentials()
        client = BetaAnalyticsDataClient(credentials=ga4_credentials)

        all_data = []
        offset = 0
        total_rows = 0

        while True:
            response = self.run_ga4_report(client, self.start_date, self.end_date, offset)

            if not response.rows:
                break

            for row in response.rows:
                all_data.append(
                    {
                        "Date": row.dimension_values[0].value,
                        "City": row.dimension_values[1].value,
                        "Source/Medium": row.dimension_values[2].value,
                        "Partner Reference": row.dimension_values[3].value,
                        "Service Type": row.dimension_values[4].value,
                        "Active Users": row.metric_values[0].value,
                        "Sessions": row.metric_values[1].value,
                        "Total Users": row.metric_values[2].value,
                        "Checkouts": row.metric_values[3].value,
                    }
                )

            total_rows += len(response.rows)
            self.log.info(f"Fetched {len(response.rows)} rows. Total rows: {total_rows}")

            if len(response.rows) < 100000:
                break

            offset += len(response.rows)

        df = pd.DataFrame(all_data)

        if df.empty:
            self.log.info("No data to write to Google Sheet.")
        else:
            df["Date"] = pd.to_datetime(df["Date"], format="%Y%m%d")
            df = df.sort_values(by="Date", ascending=False)
            sheet_data = [df.columns.tolist()] + df.values.tolist()

            sheets_hook = GSheetsHook(gcp_conn_id=self.google_sheets_conn_id)
            self.write_to_sheets(sheets_hook, sheet_data, self.worksheet)

            self.log.info("GA4 data written to Google Sheet successfully.")

            # Create monthly summary
            monthly_summary = df.groupby([df["Date"].dt.to_period("M"), "Partner Reference"]).agg(
                {"Active Users": "sum", "Total Users": "sum", "Sessions": "sum", "Checkouts": "sum"}
            )
            monthly_summary.reset_index(inplace=True)
            monthly_summary["Date"] = monthly_summary["Date"].dt.to_timestamp()
            monthly_sheet_data = [monthly_summary.columns.tolist()] + monthly_summary.values.tolist()
            self.write_to_sheets(sheets_hook, monthly_sheet_data, f"{self.worksheet}_Monthly")

            # Create weekly summary
            weekly_summary = df.groupby([df["Date"].dt.to_period("W-MON"), "Partner Reference"]).agg(
                {"Active Users": "sum", "Total Users": "sum", "Sessions": "sum", "Checkouts": "sum"}
            )
            weekly_summary.reset_index(inplace=True)
            weekly_summary["Date"] = weekly_summary["Date"].dt.to_timestamp()
            weekly_summary["Week Beginning"] = weekly_summary["Date"].dt.strftime("%Y-%m-%d")
            weekly_summary = weekly_summary[
                ["Week Beginning", "Partner Reference", "Active Users", "Total Users", "Sessions", "Checkouts"]
            ]
            weekly_sheet_data = [weekly_summary.columns.tolist()] + weekly_summary.values.tolist()
            self.write_to_sheets(sheets_hook, weekly_sheet_data, f"{self.worksheet}_Weekly")

            self.log.info("Monthly and weekly summaries written to Google Sheet successfully.")
