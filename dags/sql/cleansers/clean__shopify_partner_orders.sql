DROP VIEW IF EXISTS {{ schema }}.clean__shopify_partner_orders CASCADE;
CREATE VIEW {{ schema }}.clean__shopify_partner_orders AS
  SELECT
	p.*,
    CASE
        WHEN LOWER(p.shipping_address__city) = 'london' THEN 1
        ELSE 0
    END AS london,
    CASE
        WHEN payment_gateway_names = '["Harper Payments"]' THEN 'Harper'
        WHEN source_name = 'web' THEN 'Online Store'
    END AS channel,
    CASE WHEN ((items_ordered::bigint) - (items_returned::bigint)) >= 1 THEN 1 ELSE 0 END AS keep,
    	CASE
    WHEN tags IS NULL THEN NULL
    WHEN LOWER(tags) LIKE '%%harper_try%%'
        THEN 'harper_try'
    WHEN LOWER(tags) LIKE '%%harper_concierge%%'
        THEN 'harper_concierge'
    ELSE NULL
END AS harper_product_clean, -- Need to backfill this logic into raw in next rebuild
	{{ dim__time_columns | prefix_columns('pc', 'createdat') }}
FROM {{ schema }}.shopify_partner_orders p
LEFT JOIN
    {{ schema }}.dim__time pc ON p.created_at::date = pc.dim_date_id
;
