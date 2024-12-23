DROP VIEW IF EXISTS {{ schema }}.clean__order__items CASCADE;
CREATE VIEW {{ schema }}.clean__order__items AS
  SELECT
	oi.*,
	oi.order_type AS item__order_type,
	oi.original_order_name AS partner_order_name,
	oi.order_name AS harper_order_name,
	oi.original_name AS product_name,
  CASE WHEN  order_type = 'inspire_me' THEN 1 ELSE 0 END AS is_inspire_me,
	oi.price as item_price_pence,
	oi.discount AS item_discount_price_pence,
	oi.price - oi.discount AS item_value_pence,
  oi.calculated_discount AS calculated_item_discount_price_pence,
	oi.price - oi.calculated_discount AS calculated_item_value_pence,
	oi.qty AS item_quantity,
    -- CASE WHEN oi.commission__amount THEN oi.commission__amount
    --  oi.comission__amount
    -- ELSE
    CASE WHEN oi.commission__percentage IS NOT NULL THEN
         oi.price * ( oi.commission__percentage / 100)
    ELSE
        NULL
    -- END
    END AS commission__calculated_amount,
	CASE WHEN (purchased = 0 AND received = 1 AND received_by_warehouse = 1 AND returned = 0)  THEN 1 ELSE 0 END AS unpurchased_return,
	CASE
		WHEN (purchased = 1 AND received = 1 AND received_by_warehouse = 1) THEN 1
		WHEN (purchased = 1 AND returned = 1) THEN 1
		ELSE 0
	END AS post_purchase_return,
	 {{ dim__time_columns | prefix_columns('oc', 'createdat') }}
FROM {{ schema }}.order__items oi
LEFT JOIN
    {{ schema }}.dim__time oc ON oi.createdat::date = oc.dim_date_id
WHERE
	LOWER(oi.name) NOT LIKE '%%undefined%%'
	AND oi.name IS NOT NULL AND oi.name != ''
	AND oi.order_name IS NOT NULL AND oi.order_name != '' AND oi.order_name != ' ' AND oi.order_name != ' -L1'
	AND oi.original_order_name IS NOT NULL AND oi.original_order_name != '' AND oi.original_order_name != ' ' AND oi.original_order_name != ' -L1'
;
