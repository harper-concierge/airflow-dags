from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook


class HorizontalReportOperator(BaseOperator):
    """
    Operator to transform any vertical time-series data into a horizontal format.
    Uses SQL for efficient data transformation and pivoting.

    The source table/view should have:
    - A date/time column that can be used for monthly grouping
    - A metric value column
    - Any grouping columns (e.g., brand_name, service_type)
    """

    @apply_defaults
    def __init__(
        self,
        postgres_conn_id: str,
        source_schema: str,
        source_table: str,
        destination_schema: str,
        destination_table: str,
        date_column: str,
        metric_columns: list,
        group_columns: list,
        *args,
        **kwargs,
    ) -> None:
        """
        Initialize the operator.

        Args:
            postgres_conn_id: Airflow connection ID for PostgreSQL
            source_schema: Schema containing the source view/table
            source_table: Source view/table name
            destination_schema: Schema for the destination view
            destination_table: Destination view name
            date_column: Column name containing the date to group by (will be converted to year-month)
            metric_columns: List of column names containing metrics to pivot
            group_columns: List of column names to group by (e.g., ['brand_name', 'service_type'])
        """
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.source_schema = source_schema
        self.source_table = source_table
        self.destination_schema = destination_schema
        self.destination_table = destination_table
        self.date_column = date_column
        self.metric_columns = metric_columns
        self.group_columns = group_columns

    def execute(self, context):
        """
        Execute the data transformation and loading process using SQL.
        """
        self.log.info("Starting horizontal report data transformation")

        # Create database connection
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        engine = hook.get_sqlalchemy_engine()
        self.log.info("Connected to PostgreSQL database")

        try:
            # Create vertical metrics view
            group_columns_str = ", ".join(self.group_columns)
            metric_columns_str = ", ".join(
                [f"'{col}' as metric_name, {col} as metric_value" for col in self.metric_columns]
            )

            vertical_view_sql = f"""
            CREATE OR REPLACE VIEW {self.destination_schema}.{self.destination_table}_vertical AS
            WITH base_metrics AS (
                SELECT
                    {group_columns_str},
                    EXTRACT(YEAR FROM {self.date_column}) as year,
                    EXTRACT(MONTH FROM {self.date_column}) as month,
                    CONCAT(
                        EXTRACT(YEAR FROM {self.date_column})::text, '-',
                        LPAD(EXTRACT(MONTH FROM {self.date_column})::text, 2, '0')
                    ) as year_month,
                    {", ".join(self.metric_columns)}
                FROM {self.source_schema}.{self.source_table}
            )
            SELECT
                {group_columns_str},
                year_month,
                {metric_columns_str}
            FROM base_metrics;
            """

            self.log.info("Creating vertical metrics view")
            engine.execute(vertical_view_sql)

            # Create dynamic horizontal pivot function
            pivot_function_sql = f"""
            CREATE OR REPLACE FUNCTION {self.destination_schema}.create_horizontal_report()
            RETURNS TEXT AS $$
            DECLARE
                month_columns TEXT;
                dynamic_sql TEXT;
                view_name TEXT := '{self.destination_schema}.{self.destination_table}';
            BEGIN
                -- Get months as properly formatted column names
                SELECT string_agg(
                    FORMAT(
                        'MAX(CASE WHEN year_month = %L THEN metric_value::DECIMAL(15,2) END) AS %I',
                        year_month,
                        'month_' || replace(year_month, '-', '_')
                    ),
                    ', ' ORDER BY year_month
                ) INTO month_columns
                FROM (
                    SELECT DISTINCT year_month
                    FROM {self.destination_schema}.{self.destination_table}_vertical
                    ORDER BY year_month
                ) months;

                -- Drop and recreate view
                EXECUTE FORMAT(
                    'DROP VIEW IF EXISTS %I CASCADE',
                    view_name
                );

                dynamic_sql := FORMAT('
                    CREATE VIEW %I AS
                    SELECT
                        {group_columns_str},
                        metric_name,
                        %s,
                        COUNT(*) as total_months_with_data,
                        NOW() as last_updated
                    FROM {self.destination_schema}.{self.destination_table}_vertical
                    GROUP BY {group_columns_str}, metric_name
                    ORDER BY {group_columns_str}, metric_name',
                    view_name, month_columns
                );

                EXECUTE dynamic_sql;

                RETURN FORMAT('Created view %I with %s columns', view_name,
                            array_length(string_to_array(month_columns, ','), 1));
            END;
            $$ LANGUAGE plpgsql;
            """

            self.log.info("Creating dynamic pivot function")
            engine.execute(pivot_function_sql)

            # Execute the pivot function to create the horizontal view
            self.log.info("Creating horizontal report view")
            result = engine.execute(f"SELECT {self.destination_schema}.create_horizontal_report();").scalar()
            self.log.info(f"Horizontal report creation result: {result}")

            # Create indexes for performance
            index_sql = f"""
            CREATE INDEX IF NOT EXISTS idx_{self.destination_table}_vertical_group
            ON {self.destination_schema}.{self.destination_table}_vertical ({group_columns_str});

            CREATE INDEX IF NOT EXISTS idx_{self.destination_table}_vertical_month
            ON {self.destination_schema}.{self.destination_table}_vertical (year_month);

            CREATE INDEX IF NOT EXISTS idx_{self.destination_table}_vertical_metric
            ON {self.destination_schema}.{self.destination_table}_vertical (metric_name);
            """

            self.log.info("Creating performance indexes")
            engine.execute(index_sql)

            self.log.info("Data transformation and loading completed successfully")

        except Exception as e:
            self.log.error(f"Error during data transformation: {str(e)}")
            raise
