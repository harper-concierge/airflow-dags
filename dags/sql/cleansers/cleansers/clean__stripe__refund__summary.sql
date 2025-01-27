DROP VIEW IF EXISTS {{ schema }}.clean__stripe__refund__summary CASCADE;
CREATE VIEW {{ schema }}.clean__stripe__refund__summary AS
    SELECT
        -- Combined fields
        COALESCE(i.metadata__order, c.metadata__order) AS order_id,
        COALESCE(i.metadata__internal_order_id, c.metadata__internal_order_id) AS internal_order_id,
        COALESCE(i.metadata__customer_id, c.metadata__customer_id) AS customer_id,
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
        r.amount AS initial_amount,
        c.id AS charge_id,
        r.id AS refund_id,
        'REFUND' AS transaction_type,
        to_timestamp(r.created) AS created_at,
        r.airflow_sync_ds AS stripe_refund_airflow_sync_ds,
        r.status AS transaction_status,
        c.payment_method_details__card__brand AS card_brand,
        c.payment_method_details__card__network AS card_network,
        c.payment_method_details__card__fingerprint AS card_fingerprint,
        c.payment_method_details__card__funding AS card_funding_type,
        r.destination_details__card__type AS card_refund_type,
        r.reason,
        -- r.balance_transaction__id AS balance_transaction_id,
        -- r.balance_transaction__amount,
        c.disputed,
        c.customer as stripe_customer_id,
        dt.*
    FROM stripe__refunds r
    JOIN stripe__charges c ON c.id = r.charge
    LEFT JOIN stripe__invoices i ON c.invoice = i.id
    LEFT JOIN dim__time dt ON to_timestamp(c.created)::date = dt.dim_date_id;
