{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__weekly_partner_metrics CASCADE;
{% endif %}
CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__weekly_partner_metrics AS
WITH
concierge_revenue AS (
    SELECT
        dt.dim_yearcalendarweek_sc AS metric_date,
        t.partner_name,

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
        {{ schema }}.rep__transactionlog__view t
    LEFT JOIN {{ schema }}.dim__time dt
        ON t.createdat::date = dt.dim_date_id -- should this be payment date?
    WHERE
        lineitem_type <> 'try_on'
        AND harper_product_type = 'harper_concierge'
    GROUP BY
        dt.dim_yearcalendarweek_sc,
        t.partner_name
),

try_revenue AS (
	SELECT
		dt.dim_yearcalendarweek_sc AS metric_date,
        t.partner_name,
        SUM(
            CASE
                WHEN lineitem_category IN ('item_discount', 'order_discount', 'harper_item_discount') THEN -lineitem_amount
                ELSE lineitem_amount
            END
            * (COALESCE(NULLIF(commission_percentage, 0), 2.4) / 100)
        )::integer AS total_try_revenue

	FROM
		{{ schema }}.rep__transactionlog__view t
	LEFT JOIN {{ schema }}.dim__time dt
        ON t.trial_period_ended_at::date = dt.dim_date_id
	WHERE
		-- harper_order__order_status = 'completed'
		lineitem_type = 'try_on'
        AND harper_product_type = 'harper_try'
	GROUP BY
		dt.dim_yearcalendarweek_sc,
        t.partner_name
),
try_orders AS (
    SELECT
        dt.dim_yearcalendarweek_sc AS metric_date,
        o.brand_name AS partner_name,
        COUNT(DISTINCT order_name)::INTEGER AS total_try_orders_created
    FROM
        {{ schema }}.clean__order__summary o
    LEFT JOIN {{ schema }}.dim__time dt
        ON o.createdat::date = dt.dim_date_id
    WHERE
        link_order__is_child = 0
        AND harper_product_type = 'harper_try'
    GROUP BY
        dt.dim_yearcalendarweek_sc,
        o.brand_name
),
concierge_orders AS (
    SELECT
        dt.dim_yearcalendarweek_sc AS metric_date,
        o.brand_name AS partner_name,
        COUNT(DISTINCT order_name)::INTEGER AS total_concierge_orders_created
    FROM
        {{ schema }}.clean__order__summary o
    LEFT JOIN {{ schema }}.dim__time dt
        ON o.createdat::date = dt.dim_date_id
    WHERE
        link_order__is_child = 0
        AND harper_product_type = 'harper_concierge'
    GROUP BY
        dt.dim_yearcalendarweek_sc,
        o.brand_name
),
all_orders AS (
    SELECT
        dt.dim_yearcalendarweek_sc AS metric_date,
        o.brand_name AS partner_name
    FROM
        {{ schema }}.clean__order__summary o
    LEFT JOIN {{ schema }}.dim__time dt
        ON o.createdat::date = dt.dim_date_id
    WHERE
        link_order__is_child = 0
    GROUP BY
        dt.dim_yearcalendarweek_sc,
        o.brand_name
    ORDER BY dt.dim_yearcalendarweek_sc
),
combined_data AS (
    SELECT
        o.metric_date AS dim_yearcalendarweek_sc,
        o.partner_name,

	    cp.total_concierge_revenue,
	    tp.total_try_revenue,

	    co.total_concierge_orders_created,
	    tt.total_try_orders_created

    FROM
        all_orders o
    FULL OUTER JOIN concierge_revenue cp ON o.metric_date = cp.metric_date AND o.partner_name = cp.partner_name
    FULL OUTER JOIN try_revenue tp ON o.metric_date = tp.metric_date AND o.partner_name = tp.partner_name
    FULL OUTER JOIN concierge_orders co ON o.metric_date = co.metric_date AND o.partner_name = co.partner_name
    FULL OUTER JOIN try_orders tt ON o.metric_date = tt.metric_date AND o.partner_name = tt.partner_name
)
SELECT *
FROM combined_data
-- WHERE date = (SELECT MAX(date) FROM combined_data)

WITH NO DATA;
{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__weekly_partner_metrics_idx ON {{ schema }}.rep__weekly_partner_metrics (dim_yearcalendarweek_sc, partner_name);
{% endif %}
REFRESH MATERIALIZED VIEW {{ schema }}.rep__weekly_partner_metrics;
