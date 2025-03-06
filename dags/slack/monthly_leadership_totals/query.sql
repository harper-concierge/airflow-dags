-- Aggregating revenue and orders for the last 18 months from daily data
WITH monthly_data AS (
    SELECT
        DATE_TRUNC('month', dim_date) AS yearmonth,
        SUM(total_concierge_revenue) AS current_concierge_revenue,
        SUM(total_try_revenue) AS current_try_revenue,
        SUM(total_concierge_orders_created) AS current_concierge_orders,
        SUM(total_try_orders_created) AS current_try_orders
    FROM rep__daily_total_metrics
    WHERE dim_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '24 months'
    GROUP BY 1
),

-- Aggregating revenue and orders for the current month up to today
current_month_to_date AS (
    SELECT
        DATE_TRUNC('month', dim_date) AS yearmonth,
        SUM(total_concierge_revenue) AS current_concierge_revenue_to_date,
        SUM(total_try_revenue) AS current_try_revenue_to_date,
        SUM(total_concierge_orders_created) AS current_concierge_orders_to_date,
        SUM(total_try_orders_created) AS current_try_orders_to_date
    FROM rep__daily_total_metrics
    WHERE dim_date >= DATE_TRUNC('month', CURRENT_DATE)
          AND dim_date <= CURRENT_DATE
    GROUP BY 1
),

-- Aggregating revenue and orders for each previous month up to the same day
previous_months_to_date AS (
    SELECT
        DATE_TRUNC('month', dim_date) AS yearmonth,
        SUM(total_concierge_revenue) AS previous_concierge_revenue_to_date,
        SUM(total_try_revenue) AS previous_try_revenue_to_date,
        SUM(total_concierge_orders_created) AS previous_concierge_orders_to_date,
        SUM(total_try_orders_created) AS previous_try_orders_to_date
    FROM rep__daily_total_metrics
    WHERE dim_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '23 months')
          AND dim_date <= (DATE_TRUNC('month', dim_date) + (CURRENT_DATE - DATE_TRUNC('month', CURRENT_DATE)))
    GROUP BY 1
)

SELECT
    m.yearmonth,
    m.current_concierge_revenue,
    m.current_try_revenue,
    m.current_concierge_orders,
    m.current_try_orders,
    cmtd.current_concierge_revenue_to_date,
    cmtd.current_try_revenue_to_date,
    cmtd.current_concierge_orders_to_date,
    cmtd.current_try_orders_to_date,
    pmtd.previous_concierge_revenue_to_date,
    pmtd.previous_try_revenue_to_date,
    pmtd.previous_concierge_orders_to_date,
    pmtd.previous_try_orders_to_date
FROM monthly_data m
LEFT JOIN current_month_to_date cmtd ON m.yearmonth = cmtd.yearmonth
LEFT JOIN previous_months_to_date pmtd ON m.yearmonth = pmtd.yearmonth
ORDER BY m.yearmonth;
