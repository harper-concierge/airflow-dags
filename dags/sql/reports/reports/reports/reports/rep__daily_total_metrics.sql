{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__daily_total_metrics CASCADE;
{% endif %}
CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__daily_total_metrics AS
WITH
concierge_revenue AS (
    SELECT
        createdat AS metric_date,

        -- Adjusted revenue calculation with per-row VAT correction
        SUM(
          CASE
            -- Handle fees:
            WHEN lineitem_category = 'fee' THEN
              CASE
                WHEN revenue_is_service_fee_inclusive=1 THEN lineitem_amount
                ELSE 0
              END
            -- Explicitly handle return_fee and refundable_fee as 0
            WHEN lineitem_category IN ('return_fee', 'refundable_fee') THEN 0
          END
        )::INTEGER
        +
        SUM(
          CASE
            WHEN lineitem_category != 'fee' AND lineitem_category NOT IN ('return_fee', 'refundable_fee') THEN
              CASE
                -- Handle discounts:
                WHEN lineitem_category IN ('item_discount', 'order_discount', 'harper_item_discount') THEN
                  CASE
                    WHEN lineitem_type = 'purchase' THEN -lineitem_amount
                    WHEN lineitem_type = 'refund'   THEN lineitem_amount
                    ELSE lineitem_amount
                  END

                -- Handle non-discount items:
                ELSE
                  CASE
                    WHEN lineitem_type = 'purchase' THEN lineitem_amount
                    WHEN lineitem_type = 'refund'   THEN -lineitem_amount
                    ELSE lineitem_amount
                  END
              END
          END
          -- Apply VAT correction per row if needed
          / CASE
              WHEN commission_is_vat_inclusive=1 THEN 1  -- No need to adjust
              ELSE 1.2  -- Remove VAT per row
            END
          * (commission_percentage / 100)
        )::INTEGER
         AS total_concierge_revenue

    FROM
        public.rep__transactionlog__view
    WHERE
        lineitem_type <> 'try_on'
        AND harper_product_type = 'harper_concierge'
    GROUP BY
        createdat
),

try_revenue AS (
	SELECT
		trial_period_ended_at AS metric_date,
        SUM(
            CASE
                WHEN lineitem_category IN ('item_discount', 'order_discount', 'harper_item_discount') THEN -lineitem_amount
                ELSE lineitem_amount
            END
            * (COALESCE(NULLIF(commission_percentage, 0), 2.4) / 100)
        )::integer AS total_try_revenue

	FROM
		{{ schema }}.rep__transactionlog__view
	WHERE
		-- harper_order__order_status = 'completed'
		lineitem_type = 'try_on'
        AND harper_product_type = 'harper_try'
	GROUP BY
		trial_period_ended_at
),
try_orders AS (
    SELECT
        createdat__dim_date AS metric_date,
        COUNT(DISTINCT order_name )::INTEGER AS total_try_orders_created
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
        COUNT(DISTINCT  order_name )::INTEGER AS total_concierge_orders_created
    FROM
        {{ schema }}.clean__order__summary
    WHERE
        link_order__is_child = 0
        AND harper_product_type = 'harper_concierge'
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
    ORDER BY createdat__dim_date
),
combined_data AS (
    SELECT
        o.metric_date AS dim_date,

	    cp.total_concierge_revenue,
	    tp.total_try_revenue,

	    co.total_concierge_orders_created,
	    tt.total_try_orders_created

    FROM
        all_orders o
    FULL OUTER JOIN concierge_revenue cp ON o.metric_date = cp.metric_date
    FULL OUTER JOIN try_revenue tp ON o.metric_date = tp.metric_date
    FULL OUTER JOIN concierge_orders co ON o.metric_date = co.metric_date
    FULL OUTER JOIN try_orders tt ON o.metric_date = tt.metric_date
)
SELECT *
FROM combined_data
-- WHERE date = (SELECT MAX(date) FROM combined_data)

WITH NO DATA;
{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__daily_total_metrics_idx ON {{ schema }}.rep__daily_total_metrics (dim_date);
{% endif %}
REFRESH MATERIALIZED VIEW {{ schema }}.rep__daily_total_metrics;
