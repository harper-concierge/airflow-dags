-- Aggregating current month's revenue so far
WITH current_month AS (
    SELECT
        DATE_TRUNC('month', date) AS yearmonth,
        SUM(total_concierge_revenue + total_try_revenue) AS total_revenue
    FROM rep__daily_total_metrics
    WHERE date >= DATE_TRUNC('month', CURRENT_DATE)
    GROUP BY 1
),

-- Fetching total revenue for the last full month
last_month AS (
    SELECT
        yearmonth,
        total_concierge_revenue + total_try_revenue AS total_revenue
    FROM rep__monthly_total_metrics
    WHERE yearmonth = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
),

-- Fetching total revenue for the same month last year
last_year AS (
    SELECT
        yearmonth,
        total_concierge_revenue + total_try_revenue AS total_revenue
    FROM rep__monthly_total_metrics
    WHERE yearmonth = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 year'
)

-- Combining results with growth calculations
SELECT
    c.yearmonth,
    c.total_revenue AS current_revenue,
    lm.total_revenue AS last_month_revenue,
    ly.total_revenue AS last_year_revenue,
    (c.total_revenue - lm.total_revenue) / NULLIF(lm.total_revenue, 0) * 100 AS mom_growth,
    (c.total_revenue - ly.total_revenue) / NULLIF(ly.total_revenue, 0) * 100 AS yoy_growth
FROM current_month c
LEFT JOIN last_month lm ON c.yearmonth = lm.yearmonth
LEFT JOIN last_year ly ON c.yearmonth = ly.yearmonth;
