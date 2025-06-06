import re
import json
import datetime

import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from googleapiclient.errors import HttpError
from airflow.utils.decorators import apply_defaults
from airflow.providers.google.suite.hooks.sheets import GSheetsHook

# YOU MUST CREATE THE DESTINATION SPREADSHEET IN ADVANCE MANUALLY IN ORDER FOR THIS TO WORK


class PostgresToGoogleSheetOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        schema,
        table,
        spreadsheet_id,
        worksheet,
        google_conn_id,
        postgres_conn_id,
        *args,
        **kwargs,
    ):
        super(PostgresToGoogleSheetOperator, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.spreadsheet_id = spreadsheet_id
        self.worksheet = worksheet
        self.google_conn_id = google_conn_id
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        # Construct SQL query
        sql_query = f"SELECT * FROM {self.schema}.{self.table} LIMIT 1000"

        # Fetch data from the database
        hook = BaseHook.get_hook(self.postgres_conn_id)
        engine = self.get_postgres_sqlalchemy_engine(hook)

        with engine.connect() as conn:

            result = conn.execute(sql_query)
            records = result.fetchall()
            df = DataFrame(records)

            if df.empty:
                self.log.info("No data to write to Google Sheet.")
                return

            self.log.info(f"Number of rows in DataFrame Before processing: {len(df)}")

            # Convert all datetime.date and datetime.datetime objects to ISO strings
            def convert_datetime(val):
                if isinstance(val, (datetime.datetime, datetime.date)):
                    return val.isoformat()
                return val

            df = df.applymap(convert_datetime)

            # Ensure datetime columns are converted to string in ISO format
            for col, dtype in df.dtypes.items():
                if dtype.kind in ("M", "m"):  # 'M' for datetime-like, 'm' for timedelta
                    df[col] = df[col].apply(lambda x: x.isoformat() if not pd.isnull(x) else None)
                elif isinstance(df[col].iloc[0], list):  # Handle list data
                    df[col] = df[col].apply(lambda x: ", ".join(map(str, x)) if x else None)
                elif dtype.kind == "O":  # Check for 'object' dtype which might include dates
                    try:
                        # Attempt to convert any standard date or datetime objects to string
                        if isinstance(df[col].iloc[0], (datetime.date, datetime.datetime)):
                            df[col] = df[col].apply(lambda x: x.isoformat() if not pd.isnull(x) else None)
                    except (TypeError, AttributeError):
                        # If conversion fails, it's not a date/datetime, ignore or log if needed
                        self.log.info(f"Column {col} contains non-datetime data that was not converted.")
                elif dtype.kind in (
                    "i",
                    "u",
                    "f",
                ):  # 'i' for integer, 'u' for unsigned integer, 'f' for float
                    df[col] = df[col].apply(lambda x: x if pd.notnull(x) else None)

            self.log.info(f"Number of rows in DataFrame after processing: {len(df)}")

            # Convert DataFrame to a list of lists (Google Sheets format)
            # Stack overflow answer suggests to_numpy is useful. But stashing this as so far all works....
            # df.fillna("").to_numpy().tolist()
            data = [df.columns.tolist()] + df.fillna("").values.tolist()

            def is_json_serializable(val):
                try:
                    json.dumps(val)
                    return True
                except TypeError:
                    return False

            # Check for non-serializable values in data
            for row_idx, row in enumerate(data[1:], start=2):  # skip header, start at row 2
                for col_idx, val in enumerate(row):
                    if not is_json_serializable(val):
                        col_name = data[0][col_idx]
                        raise TypeError(
                            f"Value '{val}' in column '{col_name}' (row {row_idx}) is not JSON serializable. Type: {type(val)}"  # noqa
                        )

            # Initialize Google Sheets hook
            sheets_hook = GSheetsHook(gcp_conn_id=self.google_conn_id)

            spreadsheet = sheets_hook.get_spreadsheet(self.spreadsheet_id)
            # Iterate through the sheets in the spreadsheet
            sheet_exists = False
            for sheet in spreadsheet.get("sheets", []):
                if sheet["properties"]["title"] == self.worksheet:
                    sheet_exists = True

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
            if not sheet_exists:
                self.log.info(f"Creating New Sheet '{self.worksheet}'.")
                try:
                    sheets_hook.get_conn().spreadsheets().batchUpdate(
                        spreadsheetId=self.spreadsheet_id, body=create_request_body
                    ).execute()
                except HttpError as e:
                    self.log.error(f"Error creating sheet: {e}")
                    raise
            else:
                try:
                    sheets_hook.clear(spreadsheet_id=self.spreadsheet_id, range_=f"{self.worksheet}")
                except HttpError as e:
                    self.log.error(f"Error clearing sheet: {e}")
                    raise

            print(data)
            # Write data to the sheet
            sheets_hook.update_values(
                spreadsheet_id=self.spreadsheet_id,
                range_=f"{self.worksheet}!A1",
                values=data,
            )

        self.log.info("Data written to Google Sheet successfully.")

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
