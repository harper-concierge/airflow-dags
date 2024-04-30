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
	oi.createdat AS order_items__createdat,
	o.createdat AS orders__createdat,
	oi.updatedat AS orders_items__updatedat,
	o.appointment__date,
	o.brand_name,
	o.order_type AS orders__order_type,
	oi.order_type AS order__items__order_type,
	oi.order_id,
	oi.partner_order_name,
	oi.harper_order_name,
	oi.product_name,
	oi.product_name_cleaned,
	oi.variant_id,
	oi.sku,
	oi.item_price_pence,
	oi.discount_price_pence,
	oi.item_value_pence,
	oi.qty,
	oi.purchased,
	oi.returned,
    oi.purchased_originally,
	oi.is_inspire_me,
	{{ dim__time_columns | prefix_columns('cdt', 'createdat')}}
FROM orders o
LEFT JOIN cleansers.order__items oi
	ON o.id = oi.order_id
LEFT JOIN dim__time cdt ON o.createdat::date = cdt.dim_date_id
WHERE
	o.brand_name != 'Harper Production'
	AND o.link_order_child = FALSE
	AND oi.createdat >= '2022-01-01';

{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS order_item_summary_idx ON {{ schema }}.rep__completed_item_summary (order_id);
{% endif %}
REFRESH MATERIALIZED VIEW {{ schema }}.rep__completed_order_item_summary;
