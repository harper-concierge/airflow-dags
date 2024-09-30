import os

import pandas as pd
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# from google.oauth2.credentials import Credentials
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import Metric, DateRange, Dimension, RunReportRequest
from airflow.providers.google.suite.hooks.sheets import GSheetsHook


class GA4ToGoogleSheetOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        property_id,
        spreadsheet_id,
        worksheet,
        google_conn_id,
        start_date,
        end_date="today",
        *args,
        **kwargs,
    ):
        super(GA4ToGoogleSheetOperator, self).__init__(*args, **kwargs)
        self.property_id = property_id
        self.spreadsheet_id = spreadsheet_id
        self.worksheet = worksheet
        self.google_conn_id = google_conn_id
        self.start_date = start_date
        self.end_date = end_date

    def conn_ga4(self):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service_account.json"
        return BetaAnalyticsDataClient()

    def run_ga4_report(self, client, start_date, end_date):
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=[
                Dimension(name="date"),
                Dimension(name="city"),
            ],
            metrics=[
                Metric(name="activeUsers"),
                Metric(name="sessions"),
            ],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
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
        client = self.conn_ga4()
        response = self.run_ga4_report(client, self.start_date, self.end_date)

        data = []
        for row in response.rows:
            data.append(
                {
                    "Date": row.dimension_values[0].value,
                    "City": row.dimension_values[1].value,
                    "Active Users": row.metric_values[0].value,
                    "Sessions": row.metric_values[1].value,
                }
            )

        df = pd.DataFrame(data)

        if df.empty:
            self.log.info("No data to write to Google Sheet.")
        else:
            df["Date"] = pd.to_datetime(df["Date"], format="%Y%m%d")
            df = df.sort_values(by="Date", ascending=False)
            sheet_data = [df.columns.tolist()] + df.values.tolist()

            sheets_hook = GSheetsHook(gcp_conn_id=self.google_conn_id)
            self.write_to_sheets(sheets_hook, sheet_data)

            self.log.info("GA4 data written to Google Sheet successfully.")
