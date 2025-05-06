DROP VIEW IF EXISTS {{ schema }}.rep__order__first_payment_failure CASCADE;

CREATE VIEW {{ schema }}.rep__order__first_payment_failure AS

WITH
payment_failures AS (
    SELECT
        order_id,
        order_name,
        failure_stage,
        event__createdat,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY event__createdat
        ) AS rn
    FROM
        {{ schema }}.rep__payment_failures
    WHERE
        failure_stage != 'checkout'
),

-- Get the first failure for each order
failures AS (SELECT
    order_id,
    order_name,
	failure_stage,
    event__createdat AS first_failure_timestamp
FROM
    payment_failures
WHERE
    rn = 1) ,

invoices AS
(SELECT
  order_id,
  MIN(CASE WHEN message LIKE '%%trial_end %%' THEN createdat END) AS first_trial_end_invoice,
  MIN(CASE WHEN message LIKE '%%trial_end_replacement%%' THEN createdat END) AS first_reconciliation_invoice
FROM {{ schema }}.orderevents
WHERE event_name_id = 'invoiceIssued'
GROUP BY order_id)

SELECT
    f.order_id,
    order_name,
    first_failure_timestamp,
CASE WHEN first_failure_timestamp < first_reconciliation_invoice THEN 'trial_end'
	WHEN first_reconciliation_invoice IS NULL AND first_failure_timestamp > first_trial_end_invoice THEN 'trial_end'
	WHEN first_failure_timestamp > first_reconciliation_invoice THEN 'reconciliation'
	ELSE f.failure_stage
END AS first_failure_stage,
first_trial_end_invoice,
first_reconciliation_invoice
from failures f
LEFT JOIN invoices i
	ON i.order_id = f.order_id
