DROP VIEW IF EXISTS {{ schema }}.clean__stripe__charge__summary CASCADE;
CREATE VIEW {{ schema }}.clean__stripe__charge__summary AS
    SELECT
          -- Combined fields
          COALESCE(c.metadata__order, i.metadata__order) AS order_id,
          COALESCE(c.metadata__internal_order_id, i.metadata__internal_order_id) AS internal_order_id,
          COALESCE(c.metadata__customer_id, i.metadata__customer_id) AS customer_id,
          COALESCE(i.metadata__harper_invoice_type, c.metadata__harper_invoice_type) AS invoice_type,
          COALESCE(i.metadata__harper_invoice_subtype, c.metadata__harper_invoice_subtype) AS invoice_subtype,

          -- Source-specific fields
          -- i.metadata__order AS invoice_order_id,
          -- c.metadata__order AS charge_order_id,
          -- i.metadata__internal_order_id AS invoice_internal_order_id,
          -- c.metadata__internal_order_id AS charge_internal_order_id,
          -- i.metadata__customer_id AS invoice_customer_id,
          -- c.metadata__customer_id AS charge_customer_id,

          -- Other fields
          c.amount_captured AS initial_amount,
          c.id AS charge_id,
          NULL AS refund_id,
          'CHARGE' AS transaction_type,
          to_timestamp(c.created) AS transaction_created_at,
          c.airflow_sync_ds AS stripe_charge_airflow_sync_ds,
          c.status AS transaction_status,
          c.invoice AS invoice_id,
          c.payment_method_details__card__brand AS card_brand,
          c.payment_method_details__card__network AS card_network,
          c.payment_method_details__card__fingerprint AS card_fingerprint,
          c.payment_method_details__card__funding AS card_funding_type,
          c.outcome__network_status AS reason,
          -- c.balance_transaction__id AS balance_transaction_id,
          -- c.balance_transaction__amount,
          c.disputed,
          c.customer as stripe_customer_id,
          dt.*
    FROM stripe__charges c
    LEFT JOIN stripe__invoices i ON c.invoice = i.id
    LEFT JOIN dim__time dt ON to_timestamp(c.created)::date = dt.dim_date_id;
