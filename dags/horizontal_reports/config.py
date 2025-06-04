"""
Horizontal Reporting System Configuration

This module configures the transformation of vertical time-series data into horizontal format reports.
The system uses a SQL-based approach to efficiently pivot and aggregate data across time periods.

How it works:
1. Source Data:
   - Each report starts with a vertical format table/view (e.g., rep__partnership_metric_summary)
   - Data is organized by date, with metrics as rows and time periods as columns

2. Transformation Process:
   - The HorizontalReportOperator creates a vertical metrics view
   - A dynamic pivot function generates the horizontal format
   - Results are stored in destination views with proper indexing

3. Report Configuration:
   - Each report in HORIZONTAL_REPORT_CONFIGS defines:
     * Source table and destination view names
     * Date column for time-based grouping
     * Metric columns to pivot
     * Group columns for aggregation (e.g., brand_name, product_type)

Example Output Format:
    brand_name | product_type | metric_name | month_2023_01 | month_2023_02 | ...
    -----------|--------------|-------------|---------------|---------------|----
    Brand A    | Service X    | orders      | 100          | 120          | ...
    Brand A    | Service X    | revenue     | 1000         | 1200         | ...
    Brand B    | Service Y    | orders      | 200          | 220          | ...

Usage:
    Add new reports by extending HORIZONTAL_REPORT_CONFIGS with appropriate configuration.
    The system will automatically create the necessary views and indexes.
"""

HORIZONTAL_REPORT_CONFIGS = [
    {
        "task_id": "create_partnership_horizontal_report",
        "source_table": "rep__partnership_metric_summary",
        "destination_table": "partnership_dashboard_horizontal",
        "date_column": "appointment__date__dim_date",
        "metric_columns": [
            "num_order_name",
            "num_success_orders",
            "num_orders_over_250",
            "num_no_sale_order",
            "new_harper_customers",
            "num_items_ordered",
            "num_items_fulfilled",
            "num_purchased",
            "num_returned",
            "num_purchased_net",
            "total_value_ordered",
            "total_value_purchased",
            "total_value_returned",
            "total_value_purchased_net",
        ],
        "group_columns": ["brand_name", "harper_product_type"],
    },
    # Other reports can be added here
]
