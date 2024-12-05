CREATE INDEX IF NOT EXISTS raw__transaction_purchases__transaction_id_idx ON {{ schema }}.raw__transaction__purchases (transaction_id);
CREATE INDEX IF NOT EXISTS raw__transaction_purchases__transactionitem_id_idx ON {{ schema }}.raw__transaction__purchases (transactionitem_id);
CREATE INDEX IF NOT EXISTS transaction_purchases_compound_idx ON {{schema}}.raw__transaction__purchases (transaction_id, transactionitem_id);
