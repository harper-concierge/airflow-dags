DROP VIEW IF EXISTS {{ schema }}.clean__zettle__transaction__summary CASCADE;
CREATE VIEW {{ schema }}.clean__zettle__transaction__summary AS
    select
        zt.amount as zettle_amount,
        zt.originatingtransactionuuid as zettle_originatingtransactionuuid,
        zt.originatortransactiontype as zettle_originatortransactiontype,
        zt.timestamp as zettle_timestamp,
        zt.amount as zettle_transaction_amount,
        zt.airflow_sync_ds as zettle_transaction_airflow_sync_ds,
        zpp.purchase_id as zettle_purchase_id,
        zpp.created as zettle_created,
        zpp.airflow_sync_ds as zettle_purchase_airflow_sync_ds,
        zpp.uuid as zettle_purchase_uuid,
        zpp.amount as zettle_purchase_amount,
        zpp.type as zettle_purchase_type,
        zpp.gratuityamount as zettle_purchase_gratuityamount,
        zpp.created::date as payment_at__dim_date
    FROM {{ schema }}.zettle__transactions zt
    LEFT JOIN {{ schema }}.clean__zettle__purchase_payments zpp ON zt.originatingtransactionuuid=zpp.uuid
;
