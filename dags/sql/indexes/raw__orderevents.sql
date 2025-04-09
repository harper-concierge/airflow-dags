CREATE INDEX IF NOT EXISTS raw__orders__eventnameid_idx ON {{ schema }}.raw__orderevents (event_name_id);
CREATE INDEX IF NOT EXISTS raw__orders__orderid_idx ON {{ schema }}.raw__orderevents (order_id);
