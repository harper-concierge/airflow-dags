DROP VIEW IF EXISTS {{ schema }}.clean__order__item__summary CASCADE;
CREATE VIEW {{ schema }}.clean__order__item__summary AS
    SELECT
        -- item summary
        o.id AS order_id,
        COUNT(DISTINCT oi.id) AS total_items,
	    COUNT(oi.id) AS num_items_ordered, -- for ease of use of the order summary (alphabetical)
        SUM(oi.fulfilled) AS num_items_fulfilled,
        SUM(oi.purchased) AS num_purchased,
        SUM(oi.returned) AS num_returned,
        SUM(oi.purchased)  - SUM(oi.returned) AS num_purchased_net,
        SUM(oi.purchased) AS num_actually_purchased, -- duplicate
        SUM(oi.preorder) AS num_preorder,
        SUM(oi.received) AS num_received_by_harper_warehouse,
        SUM(oi.received_by_warehouse) AS num_received_by_partner_warehouse,
        SUM(oi.return_requested_by_customer) AS num_return_requested_by_customer,
        SUM(oi.return_sent_by_customer) AS num_return_sent_by_customer,
		-- value summary
		SUM(oi.calculated_item_value_pence) AS total_value_ordered,
		SUM(CASE WHEN oi.purchased = 1 THEN oi.calculated_item_value_pence ELSE 0 END) AS total_value_purchased,
		SUM(CASE WHEN oi.returned = 1 THEN oi.calculated_item_value_pence ELSE 0 END) AS total_value_returned,
		SUM(CASE WHEN oi.received = 1 THEN oi.calculated_item_value_pence ELSE 0 END) AS total_value_received,
        -- (SUM(CASE WHEN oi.purchased = 1 THEN oi.calculated_item_value_pence ELSE 0 END)
        -- - SUM(CASE WHEN oi.returned = 1 THEN oi.calculated_item_value_pence ELSE 0 END)) AS total_value_purchased_net,
        (SUM(
          CASE
            WHEN oi.purchased = 1 THEN oi.calculated_item_value_pence
            ELSE 0
          END
        )
        -
        SUM(
          CASE
            WHEN oi.returned = 1 AND oi.purchased = 1 THEN oi.calculated_item_value_pence
            ELSE 0
          END
        )) AS total_value_purchased_net,

		SUM(CASE WHEN oi.received_by_warehouse = 1 THEN oi.calculated_item_value_pence ELSE 0 END) AS total_value_received_by_warehouse,
		-- item summary for initiated sale
		COUNT(DISTINCT CASE WHEN (oi.is_initiated_sale = 1) THEN oi.id ELSE NULL END) AS initiated_sale__num_ordered,
		SUM(CASE WHEN oi.is_initiated_sale = 1 THEN oi.fulfilled ELSE 0 END) AS initiated_sale__num_items_fulfilled,
		SUM(CASE WHEN oi.is_initiated_sale = 1 THEN oi.purchased ELSE 0 END) AS initiated_sale__num_purchased,
		SUM(CASE WHEN oi.is_initiated_sale = 1 THEN oi.returned ELSE 0 END) AS initiated_sale__num_returned,
		SUM(CASE WHEN oi.is_initiated_sale = 1 THEN oi.purchased ELSE 0 END) AS initiated_sale__num_actually_purchased,
		SUM(CASE WHEN oi.is_initiated_sale = 1 THEN oi.preorder ELSE 0 END) AS initiated_sale__num_preorder,
		SUM(CASE WHEN oi.is_initiated_sale = 1 THEN oi.received ELSE 0 END) AS initiated_sale__num_received_by_harper_warehouse,
		SUM(CASE WHEN oi.is_initiated_sale = 1 THEN oi.received_by_warehouse ELSE 0 END) AS initiated_sale__num_received_by_partner_warehouse,
		SUM(CASE WHEN oi.is_initiated_sale = 1 THEN oi.return_requested_by_customer ELSE 0 END) AS initiated_sale__num_return_requested_by_customer,
		SUM(CASE WHEN oi.is_initiated_sale = 1 THEN oi.return_sent_by_customer ELSE 0 END) AS initiated_sale__num_return_sent_by_customer,
		-- value summary for initiated sale
		SUM(CASE WHEN oi.is_initiated_sale = 1 THEN oi.calculated_item_value_pence ELSE 0 END) AS initiated_sale__total_value_ordered,
		SUM(CASE WHEN oi.is_initiated_sale = 1 AND oi.purchased = 1 THEN oi.calculated_item_value_pence ELSE 0 END) AS initiated_sale__total_value_purchased,
		SUM(CASE WHEN oi.is_initiated_sale = 1 AND oi.returned = 1 THEN oi.calculated_item_value_pence ELSE 0 END) AS initiated_sale__total_value_returned,
		SUM(CASE WHEN oi.is_initiated_sale = 1 AND oi.received = 1 THEN oi.calculated_item_value_pence ELSE 0 END) AS initiated_sale__total_value_received,
		SUM(CASE WHEN oi.is_initiated_sale = 1 AND oi.received_by_warehouse = 1 THEN oi.calculated_item_value_pence ELSE 0 END) AS initiated__total_value_received_wh, --renamed since too long
		-- item summary for inspire me
		COUNT(DISTINCT CASE WHEN (oi.is_inspire_me = 1) THEN oi.id ELSE NULL END) AS inspire_me__num_ordered,
		SUM(CASE WHEN oi.is_inspire_me = 1 THEN oi.fulfilled ELSE 0 END) AS inspire_me__num_items_fulfilled,
		SUM(CASE WHEN oi.is_inspire_me = 1 THEN oi.purchased ELSE 0 END) AS inspire_me__num_purchased,
		SUM(CASE WHEN oi.is_inspire_me = 1 THEN oi.returned ELSE 0 END) AS inspire_me__num_returned,
		SUM(CASE WHEN oi.is_inspire_me = 1 THEN oi.purchased ELSE 0 END) AS inspire_me__num_actually_purchased,
		SUM(CASE WHEN oi.is_inspire_me = 1 THEN oi.preorder ELSE 0 END) AS inspire_me__num_preorder,
		SUM(CASE WHEN oi.is_inspire_me = 1 THEN oi.received ELSE 0 END) AS inspire_me__num_received_by_harper_warehouse,
		SUM(CASE WHEN oi.is_inspire_me = 1 THEN oi.received_by_warehouse ELSE 0 END) AS inspire_me__num_received_by_partner_warehouse,
		SUM(CASE WHEN oi.is_inspire_me = 1 THEN oi.return_requested_by_customer ELSE 0 END) AS inspire_me__num_return_requested_by_customer,
		SUM(CASE WHEN oi.is_inspire_me = 1 THEN oi.return_sent_by_customer ELSE 0 END) AS inspire_me__num_return_sent_by_customer,
		-- value summary for inspire me
		SUM(CASE WHEN oi.is_inspire_me = 1 THEN oi.calculated_item_value_pence ELSE 0 END) AS inspire_me__total_value_ordered,
		SUM(CASE WHEN oi.is_inspire_me = 1 AND oi.purchased = 1 THEN oi.calculated_item_value_pence ELSE 0 END) AS inspire_me__total_value_purchased,
		SUM(CASE WHEN oi.is_inspire_me = 1 AND oi.returned = 1 THEN oi.calculated_item_value_pence ELSE 0 END) AS inspire_me__total_value_returned,
		SUM(CASE WHEN oi.is_inspire_me = 1 AND oi.received = 1 THEN oi.calculated_item_value_pence ELSE 0 END) AS inspire_me__total_value_received,
		SUM(CASE WHEN oi.is_inspire_me = 1 AND oi.received_by_warehouse = 1 THEN oi.calculated_item_value_pence ELSE 0 END) AS inspire_me__total_value_received_wh, --renamed since too long

        o.shipping_method__price,
        array_agg(DISTINCT(oi.tracking_url)) AS delivery_tracking_urls
    FROM
        {{ schema }}.orders o
    JOIN
        {{ schema }}.clean__order__items oi ON o.id = oi.order_id
    GROUP BY
        o.id,
        o.shipping_method__price;
