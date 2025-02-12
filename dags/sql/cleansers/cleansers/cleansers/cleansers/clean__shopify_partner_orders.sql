DROP VIEW IF EXISTS {{ schema }}.clean__shopify_partner_orders CASCADE;
CREATE VIEW {{ schema }}.clean__shopify_partner_orders AS
  SELECT
	p.*,
    SPLIT_PART(p.id, '/', 5) AS clean_id,
    app_title AS channel,
    CASE
        WHEN LOWER(shipping_city) IN ('london', 'ldn')  THEN 1
        ELSE 0
    END AS london,
    CASE WHEN (net_items_quantity) >= 1 THEN 1 ELSE 0 END AS keep,
    CASE WHEN (net_items_quantity) = 0 THEN 1 ELSE 0 END AS no_sale,
    co.harper_product_type,
    CASE
        WHEN customer_created_at IS NOT NULL THEN
            CASE
                WHEN ABS(EXTRACT(EPOCH FROM (customer_created_at - created_at))/3600) <= 12 THEN 'First-time'
                ELSE 'Returning'
            END
        ELSE NULL
    END AS customer_type,
	{{ dim__time_columns | prefix_columns('pc', 'createdat') }}
FROM {{ schema }}.shopify_partner_orders p
LEFT JOIN {{ schema }}.clean__order__summary co
   p.SPLIT_PART(p.id, '/', 5) = co.integration_order_id
LEFT JOIN
    {{ schema }}.dim__time pc ON p.created_at::date = pc.dim_date_id
;
