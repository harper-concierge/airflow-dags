CREATE INDEX IF NOT EXISTS raw__order_items_order_id_idx ON {{ schema }}.raw__order__items (order_id);
CREATE INDEX IF NOT EXISTS raw__order_items_item_id_idx ON {{ schema }}.raw__order__items (item_id);
CREATE INDEX IF NOT EXISTS raw__order_items_transactionlookup_idx ON {{ schema }}.raw__order__items (item_id, is_link_order_child_item);
CREATE INDEX IF NOT EXISTS raw__order_items_name_idx ON {{ schema }}.raw__order__items (name);
CREATE INDEX IF NOT EXISTS raw__order_items_order_name_idx ON {{ schema }}.raw__order__items (order_name);
CREATE INDEX IF NOT EXISTS raw__order_items_order_original_order_name_idx ON {{ schema }}.raw__order__items (original_order_name);
CREATE INDEX IF NOT EXISTS raw__order_items_order_type_idx ON {{ schema }}.raw__order__items (order_type);
CREATE INDEX IF NOT EXISTS raw__order_items_order_fulfilled_idx ON {{ schema }}.raw__order__items (fulfilled);
CREATE INDEX IF NOT EXISTS raw__order_items_order_is_initiated_sale_idx ON {{ schema }}.raw__order__items (is_initiated_sale);
CREATE INDEX IF NOT EXISTS raw__order_items_order_preorder_idx ON {{ schema }}.raw__order__items (preorder);
CREATE INDEX IF NOT EXISTS raw__order_items_order_purchased_idx ON {{ schema }}.raw__order__items (purchased);
CREATE INDEX IF NOT EXISTS raw__order_items_order_returned_idx ON {{ schema }}.raw__order__items (returned);
CREATE INDEX IF NOT EXISTS raw__order_items_order_received_by_warehouse_idx ON {{ schema }}.raw__order__items (received_by_warehouse);
CREATE INDEX IF NOT EXISTS raw__order_items_order_commission__commission_type_idx ON {{ schema }}.raw__order__items (commission__commission_type);
CREATE INDEX IF NOT EXISTS raw__order_items_order_is_link_order_child_item_idx ON {{ schema }}.raw__order__items (is_link_order_child_item);
