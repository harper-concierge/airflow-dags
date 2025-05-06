DROP VIEW IF EXISTS {{ schema }}.rep__order__in_resolution_events CASCADE;

CREATE VIEW {{ schema }}.rep__order__in_resolution_events AS

SELECT
    o.order_name,
    o.order_status,
    ose.order_id,
    CASE
        WHEN ose.writtenoff_at IS NOT NULL
        THEN 1
        ELSE 0
    END AS order_was_written_off,
    CASE
        WHEN ose.orderrejected_at IS NOT NULL
        THEN 1
        ELSE 0
    END AS order_was_rejected,
	    CASE
        WHEN ose.paymentfailed_at IS NOT NULL
        THEN 1
        ELSE 0
    END AS payment_failed,
    ose.orderfulfilled_at,
    ose.orderreceived_at,
    ose.invoiceissued_at,
    ose.invoicevoided_at,
    ose.trialperiodstarted_at,
    ose.trialperiodended_at,
	ose.trialperiodfinalreconciliation_at,
    ose.returncreatedbycustomer_at,
    ose.returnsentbycustomer_at,
    ose.returndeliveredbycustomer_at,
    ose.itemsrefundedbywarehouse_at,
    ose.paymentpreauthed_at,
    ose.paymentfailed_at,
    ose.ordercompleted_at,
    ose.orderproblem_at,
    ose.customerservicealert_at,
    ose.writtenoff_at,
    ose.ordercreated_at,
    ose.orderfinalised_at,
    ose.orderfraudulent_at,
	first_failure_timestamp,
    first_failure_stage,
    first_trial_end_invoice,
    first_reconciliation_invoice
FROM
    {{ schema }}.clean__order__status_events ose
LEFT JOIN
    {{ schema }}.orders o ON ose.order_id = o.id
LEFT JOIN {{ schema }}.rep__order__first_payment_failure pf
    ON ose.order_id = pf.order_id
WHERE
    ordercreated_at >= '2024-01-01'
	AND (ose.paymentfailed_at IS NOT NULL
	OR ose.orderproblem_at IS NOT NULL
    OR ose.writtenoff_at IS NOT NULL
    OR ose.orderrejected_at IS NOT NULL)
	AND order_status != 'new'
