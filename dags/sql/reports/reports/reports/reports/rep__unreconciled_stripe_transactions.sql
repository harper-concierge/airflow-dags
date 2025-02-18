{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__unreconciled_stripe_transactions CASCADE;
{% endif %}
CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__unreconciled_stripe_transactions AS
    SELECT  r.order_id,
        r.order_total,
        r.stripe_total,
        r.transactionlog_total,
        r.total_partial_refund_amount,
        r.shipping_fee_refunded,
        CASE
            WHEN r.total_partial_refund_amount > 0 AND r.order_total - r.stripe_total = COALESCE(r.total_partial_refund_amount, 0) THEN 1
            ELSE 0
        END AS partial_refund_matched,
        get_halo_url(r.order_id, 'harper_try') AS halo_link,
        r.trial_period_start_at,
        r.trial_period_actually_ended_at
    FROM {{ schema }}.rep__order__reconciliation__totals r
    WHERE (stripe_total <> order_total OR stripe_total <> transactionlog_total OR order_total <> transactionlog_total)
        AND trial_period_ended = 1
    ORDER BY r.trial_period_actually_ended_at DESC;
WITH NO DATA;
{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__unreconciled_stripe_transactions_idx ON {{ schema }}.rep__unreconciled_stripe_transactions (order_id);
{% endif %}
REFRESH MATERIALIZED VIEW {{ concurrently }} {{ schema }}.rep__unreconciled_stripe_transactions;
