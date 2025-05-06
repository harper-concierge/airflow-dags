-- Report to show failed orders in order to see fraud patterns (Harper Try orders)
{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__payment_failures CASCADE;
{% endif %}

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__payment_failures AS
SELECT
    oe.id,
    o.order_name,
	o.createdat AS order__createdat,
	oe.order_id,
    oe.createdat AS event__createdat,
    oe.message,
    CASE
        WHEN oe.message LIKE '%%checkout%%' THEN 'checkout' -- TODO: check if theres a more efficient way to identify this
        WHEN oe.message LIKE '%%trial_end %%' THEN 'trial_end'
        WHEN oe.message LIKE '%%trial_end_replacement%%' THEN 'reconciliation'
        ELSE 'other'
    END AS failure_stage,
	trial_period_actually_started_at,
	trial_period_actually_ended_at,
	trial_period_reconciled,
	trial_period_actually_reconciled_at
FROM
    {{ schema }}.orderevents oe
LEFT JOIN {{ schema }}.orders o
	ON o.id = oe.order_id
WHERE
    oe.event_name_id = 'paymentFailed'
AND o.createdat >= '2024-01-01'
AND o.order_type
    NOT IN ('ship_direct', 'old_initiated_sale', 'inspire_me', 'add_to_order', 'new_appointment', 'second_appointment')
     -- Only interested in Try orders (This might still include conciergeorders failed at checkout if order type is null)
WITH NO DATA;

{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__payment_failures_idx ON {{ schema }}.rep__payment_failures (id);
CREATE INDEX IF NOT EXISTS rep__payment_failures_orderid_createdat_idx ON {{ schema }}.rep__payment_failures (order_id, order__createdat);
{% endif %}

REFRESH MATERIALIZED VIEW {{ schema }}.rep__payment_failures;
