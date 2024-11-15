{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__daily_total_metrics CASCADE;
{% endif %}
CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__daily_total_metrics AS
WITH
purchases AS (
	SELECT
		transaction_info__payment_at__dim_date AS metric_date,
		COUNT(DISTINCT harper_order_name)::INTEGER AS num_orders_paid, -- Cast to integer
		SUM(transaction_info__item_count) AS total_items_purchased,
		SUM(transaction_info__payment_invoiced_amount) AS total_amount_purchased
	FROM
		{{ schema }}.rep__transactionlog__view
	WHERE
		harper_order__order_status = 'completed'
		AND lineitem_type = 'purchase'
		AND lineitem_category = 'product'
	GROUP BY
		transaction_info__payment_at__dim_date
),
orders AS (
    SELECT
        createdat__dim_date AS metric_date,
        COUNT(DISTINCT CASE WHEN order_type LIKE '%%appointment%%' THEN order_name END) AS num_appointments_booked,
        COUNT(DISTINCT CASE WHEN order_type = 'harper_try' THEN order_name END) AS num_try_orders_created,
	    SUM(itemsummary__num_items_ordered) AS total_items_ordered,
        SUM(itemsummary__total_value_ordered) AS total_value_ordered
    FROM
        {{ schema }}.clean__order__summary
    WHERE
        link_order__is_child = 0
    GROUP BY
        createdat__dim_date
),
combined_data AS (
    SELECT
        COALESCE(o.metric_date, p.metric_date) AS date,
        (o.num_appointments_booked)::INTEGER,
        (o.num_try_orders_created)::INTEGER,
	    (p.num_orders_paid)::INTEGER,
	    (o.total_items_ordered)::INTEGER,
	    o.total_value_ordered,
	    (p.total_items_purchased)::INTEGER,
	    p.total_amount_purchased
    FROM
        orders o
    FULL OUTER JOIN purchases p ON o.metric_date = p.metric_date
)
SELECT *
FROM combined_data
-- WHERE date = (SELECT MAX(date) FROM combined_data)

WITH NO DATA;
{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__daily_total_metrics_idx ON {{ schema }}.rep__daily_total_metrics (date);
{% endif %}
REFRESH MATERIALIZED VIEW {{ schema }}.rep__daily_total_metrics;
