WITH concierge_data AS (
    SELECT
        date,
        TO_CHAR(date, 'Day') AS day_of_week,
        num_appointments_booked,
        ROUND(total_value_ordered / 100, 2) AS total_value_ordered_gbp
    FROM
        public.rep__daily_total_metrics
    WHERE
        date >= '{{ (dag_run.data_interval_start - macros.timedelta(days=548)).strftime("%Y-%m-%d") }}'::date
        AND date <= '{{ dag_run.data_interval_start.strftime("%Y-%m-%d") }}'::date
        AND EXTRACT(DOW FROM date) = EXTRACT(DOW FROM '{{ dag_run.data_interval_start.strftime("%Y-%m-%d") }}'::date)
)
SELECT
    date,
    day_of_week,
    num_appointments_booked,
    total_value_ordered_gbp
FROM
    concierge_data
ORDER BY
    date;
