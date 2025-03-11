{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__stripe__order__transactionlog CASCADE;
{% endif %}

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__stripe__order__transactionlog AS

    SELECT
      i.*,
      o.order_id,
      t.harper_invoice_type,
      t.payment_reference,
      t.payment_reference_id,
      t.payment_timestamp,
      t.payment_at,
      t.createdat
    FROM clean__transaction__item__summary i
    LEFT JOIN public.transaction t on t.id=i.transaction_id
    LEFT JOIN transaction__orders o on o.transaction_id=i.transaction_id
    --LEFT JOIN public.dim__time dt ON t.payment_at::date = dt.dim_date_id
    WHERE t.payment_provider = 'stripe'

    ORDER BY t.createdat desc

WITH NO DATA;

{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__stripe__order__transactionlog_idx ON {{ schema }}.rep__stripe__order__transactionlog (transaction_id);
{% endif %}

REFRESH MATERIALIZED VIEW {{ schema }}.rep__stripe__order__transactionlog;
