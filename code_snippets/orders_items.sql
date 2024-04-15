-- Create view 'completed order summary'
-- This view is for:
-- Viewing individual orders at the item level and their properties
-- Aggregation for sales data, cancelled and failed orders will be removed
-- Filtered to completed orders
{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.completed_order_item_summary CASCADE;
{% endif %}
CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.completed_order_item_summary AS
-- Create CTE 'orders'
WITH orders AS (
    SELECT
        id,
		customer_id,
        createdat,
        appointment__date,
        order_name,
        original_order_name,
        brand_name,
        link_order_child,
        order_status,
        order_type,
        parent_order,
        ship_direct
    FROM public.orders
    WHERE createdat >= '2022-01-01'
    AND order_status != 'cancelled' AND order_status != 'failed'
    AND link_order_child = FALSE
	)
SELECT
	o.customer_id AS customer_id,
	o.createdat AS order_created,
	oi.createdat AS item_created,
	oi.updatedat,
	o.appointment__date AS appointment_date,
	o.brand_name,
	o.original_order_name,
	o.order_name,
	oi.order_name AS order_name_item,
	oi.id AS unique_id,
	oi.order_id,
	o.order_type AS order_type_initial,
	oi.order_type AS order_type,
	ROW_NUMBER() OVER(PARTITION BY o.order_name ORDER BY oi.createdat) AS order_item_index,
	ROW_NUMBER() OVER(PARTITION BY o.order_name , oi.original_name ORDER BY oi.createdat) AS product_name_index,
	oi.original_name AS product_name, -- need to clean sizes for certain brands
	oi.sku,
	oi.price as item_price_prence,
	oi.discount AS discount_price_pence,
	oi.price - oi.discount AS item_value_pence,
	oi.qty,
	CASE WHEN oi.purchased = TRUE THEN 1 ELSE 0 END AS purchased,
	case WHEN oi.returned = TRUE THEN 1 ELSE 0 END AS returned,
    CASE WHEN oi.purchased = TRUE OR oi.returned = TRUE THEN 1 ELSE 0 END AS purchased_originally,
    CASE WHEN (oi.purchased != TRUE AND oi.returned != TRUE AND o.order_status = 'completed') THEN 1 ELSE 0 END AS unpurchased,
	CASE WHEN oi.return_sent_by_customer = TRUE THEN 1 ELSE 0 END AS return_sent,
	CASE WHEN oi.received_by_warehouse = TRUE THEN 1 ELSE 0 END AS return_received,
	CASE WHEN oi.is_initiated_sale = TRUE THEN 1 ELSE 0 END AS is_initiated_sale,
	CASE WHEN oi.out_of_stock = TRUE THEN 1 ELSE 0 END AS out_of_stock,
	CASE WHEN oi.preorder = TRUE THEN 1 ELSE 0 END AS preorder,
	CASE WHEN oi.received = TRUE THEN 1 ELSE 0 END AS received,
	CASE WHEN oi.initiated_sale__user_role like '%remote_sales%' THEN 1 ELSE 0 END AS inspire_me_flag,
	{{ dim__time_columns | prefix_columns('cdt', 'createdat')}}
FROM orders o
LEFT JOIN order__items oi
	ON o.id = oi.order_id
LEFT JOIN dim__time cdt ON o.createdat::date = cdt.dim_date_id
WHERE
	LOWER(oi.name) NOT LIKE '%undefined%'
AND oi.name IS NOT NULL AND oi.name != ''
AND oi.order_name IS NOT NULL AND TRIM (oi.order_name) != ''
AND o.brand_name != 'Harper Production'
AND o.order_status = 'completed' -- Does this include ship direct(?)
ORDER BY o.createdat DESC, o.order_name, ROW_NUMBER() OVER(PARTITION BY o.order_name ORDER BY oi.createdat) -- order item index
WITH NO DATA;
{% if is_modified %}
REFRESH MATERIALIZED VIEW {{ schema }}.completed_order_item_summary;
