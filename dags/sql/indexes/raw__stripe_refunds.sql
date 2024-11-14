CREATE INDEX IF NOT EXISTS raw__stripe__refunds__charge_idx ON {{ schema }}.raw__stripe__refunds (charge);
CREATE INDEX IF NOT EXISTS raw__stripe__refunds__created_idx ON {{ schema }}.raw__stripe__refunds (created);
