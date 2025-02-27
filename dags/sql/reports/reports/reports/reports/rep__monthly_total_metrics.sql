{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__monthly_total_metrics CASCADE;
{% endif %}
CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__monthly_total_metrics AS
WITH
concierge_revenue AS (
	SELECT
		trial_period_ended_at__dim_yearcalendarweek_sc AS metric_date,
		SUM(commission_calculated_amount) AS total_concierge_revenue
	FROM
		{{ schema }}.rep__transactionlog__view
	WHERE
		-- harper_order__order_status = 'completed'
		AND lineitem_type = 'purchase'
		AND lineitem_category = 'product'
        AND harper_product_type = 'harper_concierge'
	GROUP BY
		trial_period_ended_at__dim_yearcalendarweek_sc
),
try_revenue AS (
	SELECT
		trial_period_ended_at__dim_yearcalendarweek_sc AS metric_date,
		SUM(commission_calculated_amount) AS total_try_revenue
	FROM
		{{ schema }}.rep__transactionlog__view
	WHERE
		-- harper_order__order_status = 'completed'
		AND lineitem_type = 'try_on'
		AND lineitem_category = 'product'
        AND harper_product_type = 'harper_try'
	GROUP BY
		trial_period_ended_at__dim_yearcalendarweek_sc
),
try_orders AS (
    SELECT
        createdat__dim_yearcalendarweek_sc AS metric_date,
        COUNT(DISTINCT  order_name )::INTEGER AS total_try_orders_created,
    FROM
        {{ schema }}.clean__order__summary
    WHERE
        link_order__is_child = 0
        AND harper_product_type = 'harper_try'
    GROUP BY
        createdat__dim_yearcalendarweek_sc
),
concierge_orders AS (
    SELECT
        createdat__dim_yearcalendarweek_sc AS metric_date,
        COUNT(DISTINCT  order_name )::INTEGER AS total_concierge_orders_created,
    FROM
        {{ schema }}.clean__order__summary
    WHERE
        link_order__is_child = 0
        AND harper_product_type = 'harper_concierge'
    GROUP BY
        createdat__dim_yearcalendarweek_sc
),
all_orders AS (
    SELECT
        createdat__dim_yearcalendarweek_sc AS metric_date
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

	    cp.total_concierge_revenue,
	    tp.total_try_revenue,

	    co.total_concierge_orders_created,
	    tt.total_try_orders_created,

    FROM
        all_orders o
    FULL OUTER JOIN concierge_purchases cp ON o.metric_date = cp.metric_date
    FULL OUTER JOIN try_purchases tp ON o.metric_date = tp.metric_date
    FULL OUTER JOIN concierge_orders co ON o.metric_date = co.metric_date
    FULL OUTER JOIN try_orders tt ON o.metric_date = tt.metric_date
)
SELECT *
FROM combined_data
-- WHERE date = (SELECT MAX(date) FROM combined_data)

WITH NO DATA;
{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__monthly_total_metrics_idx ON {{ schema }}.rep__monthly_total_metrics (date);
{% endif %}
REFRESH MATERIALIZED VIEW {{ schema }}.rep__monthly_total_metrics;
