{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__stripe_transactions CASCADE;
{% endif %}

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__stripe_transactions AS
    SELECT
  i.metadata__order AS order_id,
  i.metadata__internal_order_id AS internal_order_id,
  c.invoice AS invoice_id,
  'CHARGE' as transaction_type,
  c.id AS charge_id,
  NULL AS refund_id,
  to_timestamp(c.created) AS transaction_created_at,
  c.status AS transaction_status,
  c.payment_method_details__card__brand AS card_brand,
  c.payment_method_details__card__network AS card_network,
  c.payment_method_details__card__fingerprint AS card_fingerprint,
  c.payment_method_details__card__funding AS card_funding_type,
  c.receipt_url,
  c.outcome__type,
  c.outcome__network_status AS reason,
  c.amount_captured AS amount,
  c.balance_transaction__id AS balance_transaction_id,
  c.balance_transaction__amount,
  c.disputed,
  i.customer_email,
  i.customer_name,
  i.hosted_invoice_url,
  i.metadata__harper_invoice_type AS invoice_type,
  i.metadata__harper_invoice_subtype AS invoice_subtype
FROM stripe__charges c
LEFT JOIN stripe__invoices i ON c.invoice = i.id

UNION ALL

SELECT
  i.metadata__order AS order_id,
  i.metadata__internal_order_id AS internal_order_id,
  c.invoice AS invoice_id,
  'REFUND' AS transaction_type,
  c.id AS charge_id,
  r.id AS refund_id,
  to_timestamp(r.created) AS transaction_created_at,
  r.status AS transaction_status,
  c.payment_method_details__card__brand AS card_brand,
  c.payment_method_details__card__network AS card_network,
  c.payment_method_details__card__fingerprint AS card_fingerprint,
  c.payment_method_details__card__funding AS card_funding_type,
  NULL AS receipt_url,
  r.destination_details__card__type AS outcome__type,
  r.reason AS outcome__type,
  r.amount AS amount,
  r.balance_transaction__id AS balance_transaction_id,
  r.balance_transaction__amount,
  c.disputed,
  i.customer_email,
  i.customer_name,
  i.hosted_invoice_url,
  i.metadata__harper_invoice_type AS invoice_type,
  i.metadata__harper_invoice_subtype AS invoice_subtype
FROM stripe__refunds r
JOIN stripe__charges c ON c.id = r.charge
JOIN stripe__invoices i ON c.invoice = i.id

ORDER BY internal_order_id, invoice_id, charge_id, refund_id

WITH NO DATA;

{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__stripe_transactions_idx ON {{ schema }}.rep__stripe_transactions (invoice_id, charge_id, refund_id);
{% endif %}

REFRESH MATERIALIZED VIEW {{ schema }}.rep__stripe_transactions;
