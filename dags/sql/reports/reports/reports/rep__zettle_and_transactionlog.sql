{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__zettle_and_transactionlog CASCADE;
{% endif %}

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__zettle_and_transactionlog AS
    SELECT
        -- Zettle transaction details
        z.zettle_amount,
        z.zettle_originatingtransactionuuid,
        z.zettle_originatortransactiontype,
        z.zettle_timestamp,
        z.zettle_transaction_amount,
        z.zettle_transaction_airflow_sync_ds,
        z.zettle_purchase_id,
        z.zettle_created,
        z.zettle_purchase_airflow_sync_ds,
        z.zettle_purchase_uuid,
        z.zettle_purchase_amount,
        z.zettle_purchase_type,
        z.zettle_purchase_gratuityamount,

        -- Transaction log details
        t.id as transaction_id,
        t.payment_reference,
        t.payment_reference_id,
        t.payment_provider,
        t.payment_timestamp,
        t.payment_at,
        t.total_purchased_amount,
        t.total_try_on_amount,
        t.payment_invoiced_amount,
        t.item_count,

        -- Date dimensions for Zettle purchase
        dt_zettle.dim_date_id as zettle_created__dim_date,
        dt_zettle.dim_month as zettle_created__dim_month,
        dt_zettle.dim_year as zettle_created__dim_year,
        dt_zettle.dim_yearmonth_sc as zettle_created__dim_yearmonth,
        dt_zettle.dim_yearcalendarweek_sc as zettle_created__dim_yearcalendarweek,

        -- Date dimensions for transaction payment
        dt_payment.dim_date_id as payment_at__dim_date,
        dt_payment.dim_month as payment_at__dim_month,
        dt_payment.dim_year as payment_at__dim_year,
        dt_payment.dim_yearmonth_sc as payment_at__dim_yearmonth,
        dt_payment.dim_yearcalendarweek_sc as payment_at__dim_yearcalendarweek

    FROM {{ schema }}.clean__zettle__transaction__summary z
    FULL OUTER JOIN {{ schema }}.clean__transaction__summary t
        ON t.payment_reference_id = z.zettle_purchase_id
        AND z.zettle_originatortransactiontype = 'PAYMENT'
    LEFT JOIN {{ schema }}.dim__time dt_zettle
        ON z.zettle_created::date = dt_zettle.dim_date_id
    LEFT JOIN {{ schema }}.dim__time dt_payment
        ON t.payment_at::date = dt_payment.dim_date_id
    WHERE (z.zettle_created >= '2023-08-01' OR t.payment_at >= '2023-08-01')
        AND z.zettle_originatortransactiontype = 'PAYMENT'
WITH NO DATA;

{% if is_modified %}
CREATE INDEX IF NOT EXISTS rep__zettle_and_transactionlog_idx ON {{ schema }}.rep__zettle_and_transactionlog (zettle_purchase_id);
{% endif %}

REFRESH MATERIALIZED VIEW {{ schema }}.rep__zettle_and_transactionlog;
