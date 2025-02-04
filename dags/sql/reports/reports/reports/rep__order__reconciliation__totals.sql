{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__order__reconciliation__totals CASCADE;
{% endif %}
CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__order__reconciliation__totals AS
     WITH stripe_totals AS (
        SELECT
          order_id,
          SUM(amount) as stripe_total
        FROM rep__stripe__order__transactions
        GROUP BY order_id
    ),
    partial_refunds AS (
        SELECT
            o.id AS order_id,
            SUM(
                (CAST((match_array[1]) AS NUMERIC) * 100)::INT
            ) AS total_partial_refund_amount
        FROM orderevents e
        JOIN orders o ON o.id = e.order_id
        CROSS JOIN LATERAL (
            SELECT
                regexp_matches(e.message, '[0-9]+(?:\.[0-9]{2})?$', 'g') AS match_array
        ) AS matches
        WHERE e.message LIKE '%%partial refund -%%'
        GROUP BY o.id
    ),
    shipping_fee_refunds AS (
        SELECT
          o.id AS order_id,
          1 AS shipping_fee_refunded
        FROM clean__order__summary o
        WHERE o.orderstatusevent__shippingfeerefundedbywarehouse_at IS NOT NULL
    )
    -- WITH transactionlog_totals AS (
        -- SELECT
        -- COUNT(DISTINCT harper_order_name)::INTEGER AS num_orders_paid, -- Cast to integer
        -- SUM(transaction_info__item_count) AS total_items_purchased,
        -- SUM(transaction_info__payment_invoiced_amount) AS total_amount_purchased
    -- FROM
        -- {{ schema }}.rep__transactionlog__view
    -- WHERE
        -- harper_order__order_status = 'completed'
        -- AND lineitem_type = 'purchase'
        -- AND lineitem_category = 'product'
    -- GROUP BY
--
    -- ),
    SELECT
        o.order_id,
        cos.order_type,
        cos.order_status,
        cos.trial_period_ended,
        cos.trial_period_start_at,
        cos.trial_period_actually_ended_at,
        cos.updatedat,
        cos.tp_actually_reconciled__dim_calendarweek,
        CASE
            WHEN sfr.shipping_fee_refunded = 1 THEN o.total_value_purchased_net
            ELSE (o.total_value_purchased_net + o.shipping_method__price)
        END AS order_total,
        CASE
            WHEN sfr.shipping_fee_refunded = 1 THEN 1
            ELSE 0
        END as shipping_fee_refunded,
        s.stripe_total,
        COALESCE(pr.total_partial_refund_amount, 0) AS total_partial_refund_amount

    FROM {{ schema }}.clean__order__item__summary o
    LEFT JOIN stripe_totals s ON s.order_id = o.order_id
    LEFT JOIN shipping_fee_refunds sfr ON sfr.order_id = o.order_id
    LEFT JOIN partial_refunds pr ON pr.order_id = o.order_id
    LEFT JOIN clean__order__summary cos ON cos.id = o.order_id
WITH NO DATA;
{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__order__reconciliation__totals_idx ON {{ schema }}.rep__order__reconciliation__totals (order_id);
{% endif %}
REFRESH MATERIALIZED VIEW {{ schema }}.rep__order__reconciliation__totals;
