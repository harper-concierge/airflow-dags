-- Create view 'completed order summary'
-- This view is for:
-- Viewing individual orders at the item level and their properties
-- Aggregation for sales data, cancelled and failed orders will be removed
-- Filtered to completed orders
--Filtered to orders from 2022
{% if is_modified %}
	DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__completed_order_item_summary CASCADE;
{% endif %}
CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__completed_order_item_summary AS
SELECT
	oi.id,
	oi.idx,
	o.customer_id AS customer_id,
	oi.createdat AS item_created,
	o.createdat AS order_created,
	oi.updatedat,
	o.appointment__date AS appointment_date,
	o.brand_name,
	o.order_type AS order__order_type,
	oi.order_type AS item__order_type,
	oi.order_id,
	oi.original_order_name AS partner_order_name,
	oi.order_name AS harper_order_name,
	oi.original_name AS product_name, -- need to clean sizes for certain brands in raw
	CASE
        WHEN POSITION('-' IN oi.original_name) > 0 THEN
            TRIM(SPLIT_PART(oi.original_name, ' - ', 1))
        ELSE
            oi.original_name
    END AS product_name_cleaned,
	oi.variant_id,
	oi.sku,
	oi.price as item_price_pence,
	oi.discount AS discount_price_pence,
	oi.price - oi.discount AS item_value_pence,
	oi.qty,
	CASE WHEN oi.purchased = TRUE THEN 1 ELSE 0 END AS purchased,
	case WHEN oi.returned = TRUE THEN 1 ELSE 0 END AS returned,
    CASE WHEN oi.purchased = TRUE OR oi.returned = TRUE THEN 1 ELSE 0 END AS purchased_originally,
    CASE WHEN (oi.purchased != TRUE AND oi.returned != TRUE AND o.order_status = 'completed') THEN 1 ELSE 0 END AS unpurchased,
	CASE WHEN oi.return_sent_by_customer = TRUE THEN 1 ELSE 0 END AS return_sent,
	CASE WHEN oi.received_by_warehouse = TRUE THEN 1 ELSE 0 END AS return_received,
	CASE WHEN oi.out_of_stock = TRUE THEN 1 ELSE 0 END AS out_of_stock,
	CASE WHEN oi.preorder = TRUE THEN 1 ELSE 0 END AS preorder,
	CASE WHEN oi.received = TRUE THEN 1 ELSE 0 END AS received,
	CASE WHEN oi.is_initiated_sale = TRUE THEN 1 ELSE 0 END AS is_initiated_sale,
	CASE WHEN (oi.initiated_sale__user_role like '%%remote_sales%%' OR oi.order_type = 'inspire_me') THEN 1 ELSE 0 END AS is_inspire_me,
	{{ dim__time_columns | prefix_columns('cdt', 'createdat')}}
FROM orders o
LEFT JOIN order__items oi
	ON o.id = oi.order_id
LEFT JOIN dim__time cdt ON o.createdat::date = cdt.dim_date_id
WHERE
	LOWER(oi.name) NOT LIKE '%%undefined%%'
	AND oi.name IS NOT NULL AND oi.name != ''
	AND oi.order_name IS NOT NULL AND oi.order_name != ''
	AND o.brand_name != 'Harper Production'
	AND o.link_order_child = FALSE
	AND oi.createdat >= '2022-01-01';

{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS order_item_summary_idx ON {{ schema }}.rep__completed_item_summary (order_id);
{% endif %}
REFRESH MATERIALIZED VIEW {{ schema }}.rep__completed_order_item_summary;
