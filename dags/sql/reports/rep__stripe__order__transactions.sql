{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__stripe__order__transactions CASCADE;
{% endif %}

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__stripe__order__transactions AS

    SELECT
        t.id,
        t.amount,
        t.net as net_amount,
        t.currency,
        c.order_id,
        c.customer_id,
        c.internal_order_id,
        c.invoice_type,
        c.invoice_subtype,
        c.transaction_created_at as charge_transaction_created_at,
        t.charge_id,
        t.refund_id,
        c.invoice_id as charge_invoice_id,
        t.transaction_source_invoice_id as transaction_invoice_id,
        c.card_brand,
        c.card_network,
        c.card_fingerprint,
        c.card_funding_type,
        c.transaction_status as charge_transaction_status,
        t.status as balance_status,
        dt.*
    FROM public.clean__stripe__transaction t
    LEFT JOIN public.clean__stripe__charge__summary c ON t.charge_id = c.charge_id
    LEFT JOIN public.dim__time dt ON t.created_at::date = dt.dim_date_id
    ORDER BY t.created_at desc

WITH NO DATA;

{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__stripe__order__transactions_idx ON {{ schema }}.rep__stripe__order__transactions (id);
{% endif %}

REFRESH MATERIALIZED VIEW {{ schema }}.rep__stripe__order__transactions;
