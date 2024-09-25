from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import Metric, DateRange, Dimension, RunReportRequest

# Path to your credentials file
CREDENTIALS_PATH = "path/to/your/credentials.json"

# Your GA4 property ID
PROPERTY_ID = "347125794"


def fetch_sessions_data():
    # Initialize the client using the service account file
    client = BetaAnalyticsDataClient.from_service_account_file(CREDENTIALS_PATH)

    # Create a request to fetch sessions and date
    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        dimensions=[Dimension(name="date")],
        metrics=[Metric(name="sessions")],
        date_ranges=[DateRange(start_date="2023-01-01", end_date="2023-01-31")],  # Example date range
    )

    # Run the report
    response = client.run_report(request)

    # Process and print the response
    for row in response.rows:
        date = row.dimension_values[0].value
        sessions = row.metric_values[0].value
        print(f"Date: {date}, Sessions: {sessions}")


if __name__ == "__main__":
    fetch_sessions_data()
