-- This view is for:
-- Viewing individual orders at the item level and their properties
-- Aggregation for sales data, cancelled and failed orders will be removed

-- Create CTE 'orders'
WITH orders AS (
    SELECT
        id,
        createdat,
        appointment__date,
        order_name,
        original_order_name,
        brand_name,
        link_order__is_child,
        order_status,
        order_type,
        parent_order,
        ship_direct
    FROM public.orders
    WHERE createdat >= '2022-01-01'
        -- linked_order_is child = 0 for all orders except for ship_direct?? need clarification on this 
    AND order_status != 'cancelled' AND order_status != 'failed'
    AND  ((ship_direct = TRUE AND link_order__is_child = TRUE) 
        OR link_order__is_child = FALSE)
	)

SELECT 
	o.createdat AS order_created,
	oi.createdat AS item_created,
	TO_CHAR(o.createdat, 'Month') AS order_created_month,
    EXTRACT(YEAR FROM o.createdat) AS order_createdat_year,
	oi.updatedat,
	o.appointment__date AS appointment_date,
	TO_CHAR(o.appointment__date, 'Month') AS appointment_month,
    EXTRACT(YEAR FROM o.appointment__date) AS appointment_year,
	o.brand_name,
	o.original_order_name,
	o.order_name,
	oi.order_name AS order_name_item_level,
	oi.order_id, 
	o.order_type AS order_type_initial,	
	oi.order_type AS order_type,
	CONCAT(o.order_name,oi.sku,ROW_NUMBER() OVER(PARTITION BY o.order_name ORDER BY oi.createdat)) AS unique_item_id,
	ROW_NUMBER() OVER(PARTITION BY o.order_name ORDER BY oi.createdat) AS order_item_index,
	ROW_NUMBER() OVER(PARTITION BY o.order_name , oi.original_name ORDER BY oi.createdat) AS product_name_index,	
	oi.original_name AS product_name, -- need to clean sizes for certain brands 
	oi.sku,
	oi.price/100 as item_price,
	oi.discount/100 AS discount_price,
	oi.price/100 - oi.discount/100 AS item_value,
	oi.qty, 
	oi.purchased,
	oi.returned,
    CASE WHEN oi.purchased = TRUE OR oi.returned = TRUE THEN 1 ELSE 0 END AS purchased_originally,
    CASE WHEN (oi.purchased != TRUE AND oi.returned != TRUE AND o.order_status IN ('returned','completed')) THEN 1 ELSE 0 END AS unpurchased,
	oi.return_sent_by_customer,
	oi.received_by_warehouse,
	oi.initiated_sale__initiated_sale_type, 
	oi.initiated_sale__user_role,
	oi.is_initiated_sale,
	oi.out_of_stock,
	oi.preorder, 
	oi.received, 
	CASE WHEN oi.initiated_sale__user_role like '%remote_sales%' THEN 1 ELSE 0 END AS inspire_me_flag
FROM orders o
LEFT JOIN order__items oi
	ON o.id = oi.order_id
WHERE 
	LOWER(oi.name) NOT LIKE '%undefined%' 
AND oi.name IS NOT NULL AND oi.name != ''
AND oi.order_name IS NOT NULL AND TRIM(oi.order_name) != ''
AND o.brand_name != 'Harper Production'
ORDER BY o.createdat DESC, o.order_name, ROW_NUMBER() OVER(PARTITION BY o.order_name ORDER BY oi.createdat) -- order item index 