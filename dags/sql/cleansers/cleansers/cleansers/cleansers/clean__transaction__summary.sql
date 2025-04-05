DROP VIEW IF EXISTS {{ schema }}.clean__transaction__summary CASCADE;
CREATE VIEW {{ schema }}.clean__transaction__summary AS
    SELECT
        t.*,
        tis.*,
        t.payment_at::date as payment_at__dim_date,
        to_char(t.payment_at::date, 'YYYYIW') as payment_at__dim_yearcalendarweek_sc,
        to_char(t.payment_at::date, 'YYYYMM') as payment_at__dim_yearmonth_sc
    FROM
        {{ schema }}.transaction t
    LEFT JOIN {{ schema }}.clean__transaction__item__summary tis ON tis.transaction_id=t.id
;
