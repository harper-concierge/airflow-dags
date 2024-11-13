CREATE INDEX IF NOT EXISTS raw__stripe__charges__id_idx ON {{ schema }}.raw__stripe__charges (id);
CREATE INDEX IF NOT EXISTS raw__stripe__charges__invoice_idx ON {{ schema }}.raw__stripe__charges (invoice);
CREATE INDEX IF NOT EXISTS raw__stripe__charges__created_idx ON {{ schema }}.raw__stripe__charges (created);
