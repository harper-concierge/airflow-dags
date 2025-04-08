CREATE INDEX IF NOT EXISTS raw__orders__eventnameid_idx ON {{ schema }}.raw__orders (event_name_id);
CREATE INDEX IF NOT EXISTS raw__orders__orderid_idx ON {{ schema }}.raw__orders (order_id);
