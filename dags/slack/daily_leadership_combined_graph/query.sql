-- Aggregating revenue and orders for the last 18 months from daily data
WITH monthly_data AS (
    SELECT
        DATE_TRUNC('month', dim_date) AS yearmonth,
        SUM(total_concierge_revenue + total_try_revenue) AS total_revenue,
        SUM(total_concierge_orders_created + total_try_orders_created) AS total_orders
    FROM rep__daily_total_metrics
    WHERE dim_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '18 months'
    GROUP BY 1
),

-- Aggregating revenue and orders for the current month up to today
current_month_to_date AS (
    SELECT
        DATE_TRUNC('month', dim_date) AS yearmonth,
        SUM(total_concierge_revenue + total_try_revenue) AS total_revenue_to_date,
        SUM(total_concierge_orders_created + total_try_orders_created) AS total_orders_to_date
    FROM rep__daily_total_metrics
    WHERE dim_date >= DATE_TRUNC('month', CURRENT_DATE)
          AND dim_date <= CURRENT_DATE
    GROUP BY 1
),

-- Aggregating revenue and orders for the same period last month
last_month_to_date AS (
    SELECT
        DATE_TRUNC('month', dim_date) AS yearmonth,
        SUM(total_concierge_revenue + total_try_revenue) AS last_month_revenue_to_date,
        SUM(total_concierge_orders_created + total_try_orders_created) AS last_month_orders_to_date
    FROM rep__daily_total_metrics
    WHERE dim_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
          AND dim_date <= (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') + (CURRENT_DATE - DATE_TRUNC('month', CURRENT_DATE)))
    GROUP BY 1
),

-- Previous month data for MoM calculations
previous_month AS (
    SELECT
        yearmonth + INTERVAL '1 month' AS yearmonth,
        total_revenue AS last_month_revenue,
        total_orders AS last_month_orders
    FROM monthly_data
),

-- Last year data for YoY calculations
previous_year AS (
    SELECT
        yearmonth + INTERVAL '1 year' AS yearmonth,
        total_revenue AS last_year_revenue,
        total_orders AS last_year_orders
    FROM monthly_data
)

-- Combining results with growth calculations
SELECT
    m.yearmonth,
    cmtd.total_revenue_to_date AS current_revenue_to_date,
    lmtd.last_month_revenue_to_date AS last_month_revenue_to_date,
    m.total_revenue AS current_revenue,
    pm.last_month_revenue,
    py.last_year_revenue,
    (m.total_revenue - pm.last_month_revenue) / NULLIF(pm.last_month_revenue, 0) * 100 AS mom_growth,
    (m.total_revenue - py.last_year_revenue) / NULLIF(py.last_year_revenue, 0) * 100 AS yoy_growth,
    cmtd.total_orders_to_date AS current_orders_to_date,
    lmtd.last_month_orders_to_date AS last_month_orders_to_date,
    m.total_orders AS current_orders,
    pm.last_month_orders,
    py.last_year_orders,
    (m.total_orders - pm.last_month_orders) / NULLIF(pm.last_month_orders, 0) * 100 AS mom_orders_growth,
    (m.total_orders - py.last_year_orders) / NULLIF(py.last_year_orders, 0) * 100 AS yoy_orders_growth
FROM monthly_data m
LEFT JOIN previous_month pm ON m.yearmonth = pm.yearmonth
LEFT JOIN previous_year py ON m.yearmonth = py.yearmonth
LEFT JOIN current_month_to_date cmtd ON m.yearmonth = cmtd.yearmonth
LEFT JOIN last_month_to_date lmtd ON m.yearmonth = lmtd.yearmonth
ORDER BY m.yearmonth;
