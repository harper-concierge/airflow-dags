WITH latest_data AS (
    SELECT
        dim_date,
        TO_CHAR(dim_date, 'Day') AS latest_day_of_week,
        ROUND(total_concierge_revenue / 100.0, 2) AS concierge_revenue,
        ROUND(total_try_revenue / 100.0, 2) AS try_revenue,
        total_concierge_orders_created AS concierge_orders,
        total_try_orders_created AS try_orders
    FROM rep__daily_total_metrics
    WHERE
        dim_date = '{{ dag_run.data_interval_start.strftime("%Y-%m-%d") }}'::date
),
last_week_data AS (
    SELECT
        dim_date AS last_week_date,
        TO_CHAR(dim_date, 'Day') AS last_week_day_of_week,
        ROUND(total_concierge_revenue / 100.0, 2) AS concierge_revenue_last_week,
        ROUND(total_try_revenue / 100.0, 2) AS try_revenue_last_week,
        total_concierge_orders_created AS concierge_orders_last_week,
        total_try_orders_created AS try_orders_last_week
    FROM rep__daily_total_metrics
    WHERE
        dim_date = ('{{ dag_run.data_interval_start.strftime("%Y-%m-%d") }}'::date - INTERVAL '7 days')
)
SELECT
    l.dim_date AS latest_date,
    l.latest_day_of_week,
    lw.last_week_date,
    lw.last_week_day_of_week,

    -- Concierge Revenue
    l.concierge_revenue,
    lw.concierge_revenue_last_week,
    ROUND(l.concierge_revenue - lw.concierge_revenue_last_week, 2) AS concierge_revenue_diff,
    ROUND(
        (l.concierge_revenue - lw.concierge_revenue_last_week) * 100.0 / NULLIF(lw.concierge_revenue_last_week, 0),
        2
    ) AS concierge_revenue_change_percent,

    -- Try Revenue
    l.try_revenue,
    lw.try_revenue_last_week,
    ROUND(l.try_revenue - lw.try_revenue_last_week, 2) AS try_revenue_diff,
    ROUND(
        (l.try_revenue - lw.try_revenue_last_week) * 100.0 / NULLIF(lw.try_revenue_last_week, 0),
        2
    ) AS try_revenue_change_percent,

    -- Concierge Revenue per Order
    ROUND(l.concierge_revenue / NULLIF(l.concierge_orders, 0), 2) AS concierge_revenue_per_order,
    ROUND(lw.concierge_revenue_last_week / NULLIF(lw.concierge_orders_last_week, 0), 2) AS concierge_revenue_per_order_last_week,
    ROUND(
        (l.concierge_revenue / NULLIF(l.concierge_orders, 0)) -
        (lw.concierge_revenue_last_week / NULLIF(lw.concierge_orders_last_week, 0)),
        2
    ) AS concierge_revenue_per_order_diff,
    ROUND(
        ((l.concierge_revenue / NULLIF(l.concierge_orders, 0)) -
        (lw.concierge_revenue_last_week / NULLIF(lw.concierge_orders_last_week, 0))) * 100.0 /
        NULLIF((lw.concierge_revenue_last_week / NULLIF(lw.concierge_orders_last_week, 0)), 0),
        2
    ) AS concierge_revenue_per_order_change_percent,

    -- Try Revenue per Order
    ROUND(l.try_revenue / NULLIF(l.try_orders, 0), 2) AS try_revenue_per_order,
    ROUND(lw.try_revenue_last_week / NULLIF(lw.try_orders_last_week, 0), 2) AS try_revenue_per_order_last_week,
    ROUND(
        (l.try_revenue / NULLIF(l.try_orders, 0)) -
        (lw.try_revenue_last_week / NULLIF(lw.try_orders_last_week, 0)),
        2
    ) AS try_revenue_per_order_diff,
    ROUND(
        ((l.try_revenue / NULLIF(l.try_orders, 0)) -
        (lw.try_revenue_last_week / NULLIF(lw.try_orders_last_week, 0))) * 100.0 /
        NULLIF((lw.try_revenue_last_week / NULLIF(lw.try_orders_last_week, 0)), 0),
        2
    ) AS try_revenue_per_order_change_percent,

    -- Total Revenue
    ROUND(l.concierge_revenue + l.try_revenue, 2) AS total_revenue,
    ROUND(lw.concierge_revenue_last_week + lw.try_revenue_last_week, 2) AS total_revenue_last_week,
    ROUND((l.concierge_revenue + l.try_revenue) - (lw.concierge_revenue_last_week + lw.try_revenue_last_week), 2) AS total_revenue_diff,
    ROUND(
        ((l.concierge_revenue + l.try_revenue) - (lw.concierge_revenue_last_week + lw.try_revenue_last_week)) * 100.0
        / NULLIF((lw.concierge_revenue_last_week + lw.try_revenue_last_week), 0),
        2
    ) AS total_revenue_change_percent,

    -- Concierge Orders
    l.concierge_orders,
    lw.concierge_orders_last_week,
    l.concierge_orders - lw.concierge_orders_last_week AS concierge_orders_diff,
    ROUND(
        (l.concierge_orders - lw.concierge_orders_last_week) * 100.0 / NULLIF(lw.concierge_orders_last_week, 0),
        2
    ) AS concierge_orders_change_percent,

    -- Try Orders
    l.try_orders,
    lw.try_orders_last_week,
    l.try_orders - lw.try_orders_last_week AS try_orders_diff,
    ROUND(
        (l.try_orders - lw.try_orders_last_week) * 100.0 / NULLIF(lw.try_orders_last_week, 0),
        2
    ) AS try_orders_change_percent,

    -- Total Orders
    (l.concierge_orders + l.try_orders) AS total_orders,
    (lw.concierge_orders_last_week + lw.try_orders_last_week) AS total_orders_last_week,
    (l.concierge_orders + l.try_orders) - (lw.concierge_orders_last_week + lw.try_orders_last_week) AS total_orders_diff,
    ROUND(
        ((l.concierge_orders + l.try_orders) - (lw.concierge_orders_last_week + lw.try_orders_last_week)) * 100.0
        / NULLIF((lw.concierge_orders_last_week + lw.try_orders_last_week), 0),
        2
    ) AS total_orders_change_percent
FROM
    latest_data l
LEFT JOIN
    last_week_data lw
ON
    l.dim_date = lw.last_week_date + INTERVAL '7 days';
