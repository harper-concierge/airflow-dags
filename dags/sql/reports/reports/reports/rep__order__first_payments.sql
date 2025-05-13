DROP VIEW IF EXISTS {{ schema }}.rep__order__first_payments CASCADE;

CREATE VIEW {{ schema }}.rep__order__first_payments AS

WITH ranked_charges AS (
    SELECT
        payment_intent,
        created,
        TO_CHAR(TO_TIMESTAMP(created), 'YYYY-MM-DD HH24:MI:SS') as formatted_created,
        paid,
		invoice,
		amount AS sc_amount,
        ROW_NUMBER() OVER (PARTITION BY payment_intent ORDER BY created) as rn
    FROM {{ schema }}.stripe__charges
),

--first payment for payment intent id
first_payments AS (
SELECT
    payment_intent,
    formatted_created as first_charge__createdat,
    paid,
	invoice,
	sc_amount
FROM ranked_charges
WHERE rn = 1
),

invoices AS (
select
TO_CHAR(TO_TIMESTAMP(created), 'YYYY-MM-DD HH24:MI:SS') AS created,
metadata__internal_order_id,
payment_intent,
metadata__harper_invoice_type,
ROW_NUMBER() OVER (PARTITION BY metadata__internal_order_id, metadata__harper_invoice_type ORDER BY created) as rn
from {{ schema }}.stripe__invoices si
--WHERE metadata__harper_invoice_type =  'trial_end'
--AND status != 'void'
)

SELECT
	o.createdat AS order__createdat,
	i.created AS invoice__createdat,
	brand_name,
	metadata__internal_order_id,
	o.order_name,
	metadata__harper_invoice_type,
	fp.*
FROM invoices i
LEFT JOIN first_payments fp
	ON fp.payment_intent = i.payment_intent
LEFT JOIN {{ schema }}.orders o
	ON o.internal_order_id = i.metadata__internal_order_id
WHERE rn = 1 --ensure only one invoice
AND order_type = 'harper_try'
ORDER BY metadata__internal_order_id
