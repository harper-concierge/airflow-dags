DROP VIEW IF EXISTS {{ schema }}.clean__transaction__item__summary CASCADE;
CREATE VIEW {{ schema }}.clean__transaction__item__summary AS
    SELECT
        transaction_id,
        count(*) as item_count,
        SUM(CASE
                WHEN transaction_type = 'purchase' AND lineitem_category IN ('item_discount', 'order_discount') THEN -lineitem_amount
                WHEN transaction_type = 'try_on' THEN 0
                ELSE lineitem_amount
            END
        ) AS total_purchased_amount,
        SUM(CASE
                WHEN transaction_type = 'refund' AND lineitem_category IN ('item_discount', 'order_discount') THEN +lineitem_amount
                WHEN transaction_type = 'try_on' THEN 0
                ELSE -lineitem_amount
            END
        ) AS total_refunded_amount,
        SUM(CASE
                WHEN transaction_type = 'try_on' AND lineitem_category IN ('item_discount', 'order_discount') THEN -lineitem_amount
                WHEN transaction_type = 'try_on' AND lineitem_category NOT IN ('item_discount', 'order_discount') THEN lineitem_amount
                ELSE 0
            END
        ) AS total_try_on_amount
    FROM
        {{ schema }}.clean__transaction__items
    GROUP BY
        transaction_id
    ;
