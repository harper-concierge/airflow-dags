WITH latest_data AS (
    SELECT
        date,
        TO_CHAR(date, 'Day') AS latest_day_of_week,
        num_appointments_booked,
        num_try_orders_created,
        num_orders_paid,
        total_items_ordered,
        total_items_purchased,
        total_value_ordered,
        total_amount_purchased
    FROM
        public.rep__daily_total_metrics
    WHERE
        date = (SELECT MAX(date) FROM public.rep__daily_total_metrics)
),
last_week_data AS (
    SELECT
        date AS last_week_date,
        TO_CHAR(date, 'Day') AS last_week_day_of_week,
        num_appointments_booked AS num_appointments_booked_last_week,
        num_try_orders_created AS num_try_orders_created_last_week,
        num_orders_paid AS num_orders_paid_last_week,
        total_items_ordered AS total_items_ordered_last_week,
        total_items_purchased AS total_items_purchased_last_week,
        total_value_ordered AS total_value_ordered_last_week,
        total_amount_purchased AS total_amount_purchased_last_week
    FROM
        public.rep__daily_total_metrics
    WHERE
        date = (SELECT MAX(date) FROM public.rep__daily_total_metrics) - INTERVAL '7 days'
)
SELECT
    l.date AS latest_date,
    l.latest_day_of_week,
    lw.last_week_date,
    lw.last_week_day_of_week,
    l.num_appointments_booked,
    lw.num_appointments_booked_last_week,
    l.num_appointments_booked - lw.num_appointments_booked_last_week AS appointments_diff,
    ROUND(
        (l.num_appointments_booked - lw.num_appointments_booked_last_week) * 100.0 / NULLIF(lw.num_appointments_booked_last_week, 0),
        2
    ) AS appointments_percent_change,
    l.num_try_orders_created,
    lw.num_try_orders_created_last_week,
    l.num_try_orders_created - lw.num_try_orders_created_last_week AS try_orders_diff,
    ROUND(
        (l.num_try_orders_created - lw.num_try_orders_created_last_week) * 100.0 / NULLIF(lw.num_try_orders_created_last_week, 0),
        2
    ) AS try_orders_percent_change,
    l.num_orders_paid,
    lw.num_orders_paid_last_week,
    l.num_orders_paid - lw.num_orders_paid_last_week AS orders_paid_diff,
    ROUND(
        (l.num_orders_paid - lw.num_orders_paid_last_week) * 100.0 / NULLIF(lw.num_orders_paid_last_week, 0),
        2
    ) AS orders_paid_percent_change,
    l.total_items_ordered,
    lw.total_items_ordered_last_week,
    l.total_items_ordered - lw.total_items_ordered_last_week AS items_ordered_diff,
    ROUND(
        (l.total_items_ordered - lw.total_items_ordered_last_week) * 100.0 / NULLIF(lw.total_items_ordered_last_week, 0),
        2
    ) AS items_ordered_percent_change,
    l.total_items_purchased,
    lw.total_items_purchased_last_week,
    l.total_items_purchased - lw.total_items_purchased_last_week AS items_purchased_diff,
    ROUND(
        (l.total_items_purchased - lw.total_items_purchased_last_week) * 100.0 / NULLIF(lw.total_items_purchased_last_week, 0),
        2
    ) AS items_purchased_percent_change,
    ROUND(l.total_value_ordered / 100, 2) AS total_value_ordered_gbp,
    ROUND(lw.total_value_ordered_last_week / 100, 2) AS total_value_ordered_last_week_gbp,
    ROUND((l.total_value_ordered - lw.total_value_ordered_last_week) / 100, 2) AS value_ordered_diff_gbp,
    ROUND(
        (l.total_value_ordered - lw.total_value_ordered_last_week) * 100.0 / NULLIF(lw.total_value_ordered_last_week, 0),
        2
    ) AS value_ordered_percent_change,
    ROUND(l.total_amount_purchased / 100, 2) AS total_amount_purchased_gbp,
    ROUND(lw.total_amount_purchased_last_week / 100, 2) AS total_amount_purchased_last_week_gbp,
    ROUND((l.total_amount_purchased - lw.total_amount_purchased_last_week) / 100, 2) AS amount_purchased_diff_gbp,
    ROUND(
        (l.total_amount_purchased - lw.total_amount_purchased_last_week) * 100.0 / NULLIF(lw.total_amount_purchased_last_week, 0),
        2
    ) AS amount_purchased_percent_change
FROM
    latest_data l
LEFT JOIN
    last_week_data lw
ON
    l.date = lw.last_week_date + INTERVAL '7 days';
