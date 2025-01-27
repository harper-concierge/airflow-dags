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
        cos.updatedat,
        cos.tp_actually_reconciled__dim_calendarweek,
        (o.total_value_purchased_net + o.shipping_method__price) as order_total,
        s.stripe_total

    FROM {{ schema }}.clean__order__item__summary o
    LEFT JOIN stripe_totals s ON s.order_id = o.order_id
    LEFT JOIN clean__order__summary cos ON cos.id = o.order_id

WITH NO DATA;
{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__order__reconciliation__totals_idx ON {{ schema }}.rep__order__reconciliation__totals (order_id);
{% endif %}
REFRESH MATERIALIZED VIEW {{ schema }}.rep__order__reconciliation__totals;
