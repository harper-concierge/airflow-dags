DROP VIEW IF EXISTS {{ schema }}.rep__rejected_orders CASCADE;

CREATE VIEW {{ schema }}.rep__rejected_orders AS

SELECT
	DATE(oe.createdat) AS event_date,
    t.dim_year,
    t.dim_quartal,
    t.dim_yearmonth,
	brand_name,
	o.order_type,-- to make joining tables easier
	oe.message,
	COUNT(o.id) AS rejections,
	SUM(COUNT(o.id)) OVER (
		PARTITION BY brand_name
		ORDER BY DATE(oe.createdat)
		ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
	) AS running_total_rejections_by_brand
	FROM {{ schema }}.orderevents oe
LEFT JOIN {{ schema }}.orders o
	ON o.id = oe.order_id
LEFT JOIN {{ schema }}.dim__time t
	ON t.dim_date_id = DATE(oe.createdat)
WHERE oe.event_name_id = 'orderRejected'
GROUP BY
	brand_name,
	DATE(oe.createdat),
	    t.dim_year,
    t.dim_quartal,
    t.dim_yearmonth,
	o.order_type,
	oe.message
ORDER BY DATE(oe.createdat),
	brand_name,
	order_type,
	oe.message
