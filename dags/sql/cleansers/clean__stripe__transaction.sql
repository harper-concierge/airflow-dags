DROP VIEW IF EXISTS {{ schema }}.clean__stripe__transaction CASCADE;
CREATE VIEW {{ schema }}.clean__stripe__transaction  AS
    SELECT
        id,
        CASE
            WHEN type = 'charge' THEN source_id
            WHEN type = 'refund' THEN source_charge_id
            ELSE NULL
        END AS charge_id,
        CASE
            WHEN type = 'refund' THEN source_id
            ELSE NULL
        END AS refund_id,
        amount,
        net,
        source_invoice_id as transaction_source_invoice_id,
        TO_TIMESTAMP(available_on) as available_on_at,
        TO_TIMESTAMP(created) as created_at,
        currency,
        description,
        reporting_category as transaction_type,
        status,
        airflow_sync_ds
    FROM
        raw__stripe__transactions
    WHERE
        type IN ('charge', 'refund');
