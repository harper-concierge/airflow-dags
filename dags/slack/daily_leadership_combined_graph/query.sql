-- Aggregating revenue for the current month up to today
WITH current_month AS (
    SELECT
        DATE_TRUNC('month', dim_date) AS yearmonth,
        SUM(total_concierge_revenue + total_try_revenue) AS total_revenue
    FROM {{ schema }}.rep__daily_total_metrics
    WHERE dim_date >= DATE_TRUNC('month', CURRENT_DATE) AND dim_date <= CURRENT_DATE
    GROUP BY 1
),

-- Aggregating revenue for the same period in the last month (like-for-like days)
last_month AS (
    SELECT
        DATE_TRUNC('month', dim_date) AS yearmonth,
        SUM(total_concierge_revenue + total_try_revenue) AS total_revenue
    FROM {{ schema }}.rep__daily_total_metrics
    WHERE dim_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
          AND dim_date <= (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') + (CURRENT_DATE - DATE_TRUNC('month', CURRENT_DATE)))
    GROUP BY 1
),

-- Aggregating revenue for the same period in the last year
last_year AS (
    SELECT
        DATE_TRUNC('month', dim_date) AS yearmonth,
        SUM(total_concierge_revenue + total_try_revenue) AS total_revenue
    FROM {{ schema }}.rep__daily_total_metrics
    WHERE dim_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 year')
          AND dim_date <= (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 year') + (CURRENT_DATE - DATE_TRUNC('month', CURRENT_DATE)))
    GROUP BY 1
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
