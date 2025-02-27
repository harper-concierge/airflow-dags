DROP VIEW IF EXISTS {{ schema }}.clean__order__items CASCADE;
CREATE VIEW {{ schema }}.clean__order__items AS
  SELECT
	oi.*,
	oi.order_type AS item__order_type,
	oi.original_order_name AS partner_order_name,
	oi.order_name AS harper_order_name,
	oi.original_name AS product_name,
    CASE WHEN oi.order_type = 'inspire_me' THEN 1 ELSE 0 END AS is_inspire_me,
	oi.price as item_price_pence,
	oi.discount AS item_discount_price_pence,
	oi.price - oi.discount AS item_value_pence,
    oi.calculated_discount AS calculated_item_discount_price_pence,
	oi.price - oi.calculated_discount AS calculated_item_value_pence,
	oi.qty AS item_quantity,
    -- CASE WHEN oi.commission__amount THEN oi.commission__amount
    --  oi.comission__amount
    -- ELSE
    -- Need to make this brand dependent

    -- VAT inclusive calc for newer brands oi.price * ( oi.commission__percentage / 100)
    -- VAT exclusive calc for newer brands (oi.price / 1.2) * ( oi.commission__percentage / 100)
    -- THIS IS IMPOORTANT!!!!! - MARTIN
    -- WHEN p.commission__concierge_is_vat_inclusive THEN
    -- WHEN p.commission__try_is_vat_inclusive THEN
    -- CASE
    --     WHEN oi.commission__percentage IS NOT NULL THEN
    --         CASE
    --             WHEN p.commission__concierge_is_vat_inclusive THEN
    --                 oi.price * (oi.commission__percentage / 100)::INTEGER
    --             ELSE
    --                 (oi.price / 1.2) * (oi.commission__percentage / 100)::INTEGER
    --         END
    --     ELSE
    --         NULL
    -- END AS commission__calculated_amount

    CASE WHEN oi.commission__percentage IS NOT NULL THEN
         oi.price * ( oi.commission__percentage / 100)::INTEGER
    ELSE
        NULL
    -- END
    END AS commission__calculated_amount,
	CASE WHEN (oi.purchased = 0 AND oi.received = 1 AND oi.received_by_warehouse = 1 AND oi.returned = 0)  THEN 1 ELSE 0 END AS unpurchased_return,
	CASE
		WHEN (oi.purchased = 1 AND oi.received = 1 AND oi.received_by_warehouse = 1) THEN 1
		WHEN (oi.purchased = 1 AND oi.returned = 1) THEN 1
		ELSE 0
	END AS post_purchase_return,
	 {{ dim__time_columns | prefix_columns('oc', 'createdat') }}
FROM {{ schema }}.order__items oi
LEFT JOIN
    {{ schema }}.orders o ON oi.order_id = o.id
LEFT JOIN
    {{ schema }}.partner p ON o.partner_id = p.id
LEFT JOIN
    {{ schema }}.dim__time oc ON oi.createdat::date = oc.dim_date_id
WHERE
	LOWER(oi.name) NOT LIKE '%%undefined%%'
	AND oi.name IS NOT NULL AND oi.name != ''
	AND oi.order_name IS NOT NULL AND oi.order_name != '' AND oi.order_name != ' ' AND oi.order_name != ' -L1'
	AND oi.original_order_name IS NOT NULL AND oi.original_order_name != '' AND oi.original_order_name != ' ' AND oi.original_order_name != ' -L1'
;
