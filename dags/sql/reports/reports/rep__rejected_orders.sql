DROP VIEW IF EXISTS {{ schema }}.rep__rejected_orders CASCADE;

CREATE VIEW {{ schema }}.rep__rejected_orders AS

WITH
stripe_blocked_orders as (
    SELECT i.metadata__internal_order_id AS id
    FROM {{ schema }}.stripe__charges c
    LEFT JOIN {{ schema }}.stripe__invoices i
        ON c.invoice = i.id
    WHERE outcome__type ='blocked'
    )

SELECT
	DATE(oe.createdat) AS event_date,
    t.dim_year,
	t.dim_month,
    t.dim_quartal,
    t.dim_yearmonth,
	brand_name,
	o.order_type,-- to make joining tables easier
	COUNT(DISTINCT s.id) AS stripe_blocked_orders,
	SUM(CASE WHEN oe.event_name_id = 'orderRejected' THEN 1 ELSE 0 END) AS rejected_orders,
	COUNT(DISTINCT oe.order_id) AS total_blocked_orders
FROM {{ schema }}.orderevents oe
LEFT JOIN {{ schema }}.orders o
	ON o.id = oe.order_id
LEFT JOIN {{ schema }}.dim__time t
	ON t.dim_date_id = DATE(oe.createdat)
LEFT JOIN stripe_blocked_orders s
	on s.id = o.internal_order_id
WHERE (oe.event_name_id = 'orderRejected'
	OR s.id IS NOT NULL)
group by
	DATE(oe.createdat) ,
    t.dim_year,
	t.dim_month,
    t.dim_quartal,
    t.dim_yearmonth,
	brand_name,
	o.order_type
