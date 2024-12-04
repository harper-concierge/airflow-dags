DROP VIEW IF EXISTS {{ schema }}.clean__shopify_partner_orders CASCADE;
CREATE VIEW {{ schema }}.clean__shopify_partner_orders AS
  SELECT
	p.*,
    CASE
        THEN LOWER(po.shipping_address__city) = 'london' THEN 1
        ELSE 0
    END AS london,
	{{ dim__time_columns | prefix_columns('pc', 'createdat') }}
FROM {{ schema }}.shopify_partner_orders p
LEFT JOIN
    {{ schema }}.dim__time pc ON p.created_at::date = pc.dim_date_id
;
