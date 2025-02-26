{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__daily_total_metrics CASCADE;
{% endif %}
CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__daily_total_metrics AS
WITH
concierge_purchases AS (
	SELECT
		transaction_info__payment_at__dim_date AS metric_date,
		COUNT(DISTINCT harper_order_name)::INTEGER AS total_concierge_orders_paid, -- Cast to integer
		SUM(transaction_info__item_count)::INTEGER AS total_concierge_items_purchased,
		SUM(transaction_info__payment_invoiced_amount) AS total_concierge_amount_purchased
	FROM
		{{ schema }}.rep__transactionlog__view
	WHERE
		harper_order__order_status = 'completed'
		AND lineitem_type = 'purchase'
		AND lineitem_category = 'product'
        AND harper_product_type = 'harper_concierge'
	GROUP BY
		transaction_info__payment_at__dim_date
),
initiated_concierge_purchases AS (
	SELECT
		transaction_info__payment_at__dim_date AS metric_date,
		COUNT(DISTINCT harper_order_name)::INTEGER AS total_initiated_concierge_orders_paid, -- Cast to integer
		SUM(transaction_info__item_count)::INTEGER AS total_initiated_concierge_items_purchased,
		SUM(transaction_info__payment_invoiced_amount) AS total_initiated_concierge_amount_purchased
	FROM
		{{ schema }}.rep__transactionlog__view
	WHERE
		harper_order__order_status = 'completed'
		AND lineitem_type = 'purchase'
		AND lineitem_category = 'product'
        AND harper_product_type = 'harper_concierge'
        AND item_info__is_initiated_sale = 1
	GROUP BY
		transaction_info__payment_at__dim_date
),
try_purchases AS (
	SELECT
		transaction_info__payment_at__dim_date AS metric_date,
		COUNT(DISTINCT harper_order_name)::INTEGER AS total_try_orders_paid, -- Cast to integer
		SUM(transaction_info__item_count)::INTEGER AS total_try_items_purchased,
		SUM(transaction_info__payment_invoiced_amount) AS total_try_amount_purchased
	FROM
		{{ schema }}.rep__transactionlog__view
	WHERE
		harper_order__order_status = 'completed'
		AND lineitem_type = 'purchase'
		AND lineitem_category = 'product'
        AND harper_product_type = 'harper_try'
	GROUP BY
		transaction_info__payment_at__dim_date
),
try_orders AS (
    SELECT
        createdat__dim_date AS metric_date,
        COUNT(DISTINCT  order_name )::INTEGER AS total_try_orders_created,
	    SUM(itemsummary__num_items_ordered)::INTEGER AS total_try_items_ordered,
        SUM(itemsummary__total_value_ordered) AS total_try_value_ordered
    FROM
        {{ schema }}.clean__order__summary
    WHERE
        link_order__is_child = 0
        AND harper_product_type = 'harper_try'
    GROUP BY
        createdat__dim_date
),
concierge_orders AS (
    SELECT
        createdat__dim_date AS metric_date,
        COUNT(DISTINCT  order_name )::INTEGER AS total_concierge_orders,
	    SUM(itemsummary__num_items_ordered)::INTEGER AS total_concierge_items_ordered,
        SUM(itemsummary__total_value_ordered) AS total_concierge_value_ordered
    FROM
        {{ schema }}.clean__order__summary
    WHERE
        link_order__is_child = 0
        AND harper_product_type = 'harper_concierge'
    GROUP BY
        createdat__dim_date
),
initiated_concierge_orders AS (
    SELECT
        createdat__dim_date AS metric_date,
        COUNT(DISTINCT  order_name )::INTEGER AS total_initiated_concierge_orders,
	    SUM(itemsummary__num_items_ordered)::INTEGER AS total_initiated_concierge_items_ordered,
        SUM(itemsummary__total_value_ordered) AS total_initiated_concierge_value_ordered
    FROM
        {{ schema }}.clean__order__summary
    WHERE
        link_order__is_child = 0
        AND harper_product_type = 'harper_concierge'
        AND is_initiated_sale = 1
    GROUP BY
        createdat__dim_date
),
all_orders AS (
    SELECT
        createdat__dim_date AS metric_date
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
	    cp.total_concierge_orders_paid,
	    cp.total_concierge_items_purchased,
	    cp.total_concierge_amount_purchased,
	    icp.total_initaiated_concierge_orders_paid,
	    icp.total_initaiated_concierge_items_purchased,
	    icp.total_initaiated_concierge_amount_purchased,
	    tp.total_try_orders_paid,
	    tp.total_try_items_purchased,
	    tp.total_try_amount_purchased,
	    ico.total_initiated_concierge_orders,
	    ico.total_initiated_concierge_items_ordered,
	    ico.total_initiated_concierge_value_ordered,
	    co.total_concierge_orders,
	    co.total_concierge_items_ordered,
	    co.total_concierge_value_ordered,
	    tt.total_try_orders,
	    tt.total_try_items_ordered,
	    tt.total_try_value_ordered,
    FROM
        all_orders o
    FULL OUTER JOIN concierge_purchases cp ON o.metric_date = cp.metric_date
    FULL OUTER JOIN initiated_concierge_purchases icp ON o.metric_date = icp.metric_date
    FULL OUTER JOIN try_purchases tp ON o.metric_date = tp.metric_date
    FULL OUTER JOIN initiated_concierge_orders ico ON o.metric_date = ico.metric_date
    FULL OUTER JOIN concierge_orders co ON o.metric_date = co.metric_date
    FULL OUTER JOIN try_orders tt ON o.metric_date = tt.metric_date
)
SELECT *
FROM combined_data
-- WHERE date = (SELECT MAX(date) FROM combined_data)

WITH NO DATA;
{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__daily_total_metrics_idx ON {{ schema }}.rep__daily_total_metrics (date);
{% endif %}
REFRESH MATERIALIZED VIEW {{ schema }}.rep__daily_total_metrics;
