DROP VIEW IF EXISTS {{ schema }}.rep__transactionlog__sku_report CASCADE;
CREATE VIEW {{ schema }}.rep__transactionlog__sku_report AS

select
    t.harper_product_type,
    t.transaction_info__payment_at__dim_date,
    t.transaction_info__payment_at__dim_yearcalendarweek_sc,
    t.transaction_info__payment_at__dim_yearmonth_sc,
    tpe.dim_yearcalendarweek_sc AS trial_period_ended_at__dim_yearcalendarweek_sc,
    tpe.dim_yearmonth_sc AS trial_period_ended_at__dim_yearmonth_sc,
    cdt.dim_yearcalendarweek_sc AS createdat__dim_yearcalendarweek_sc,
    cdt.dim_yearmonth_sc AS createdat__dim_yearmonth_sc,
    t.transaction_info__payment_currency,
    t.partner_name,
    t.lineitem_type,
    t.lineitem_category,
    t.order_type,
    t.harper_order_name,
    t.partner_order_name,
    t.harper_order__customer__first_name,
    t.harper_order__customer__last_name,
    t.lineitem_name,
    t.calculated_discount,
    t.calculated_discount_code,
    t.calculated_discount_percent,
    t.lineitem_amount,
    t.lineitem_billed_quantity,
    t.item_info__sku,
    t.item_info__colour,
    t.item_info__size,
    t.item_info__discount,
    t.initiated_sale_type,
    t.initiated_sale_user_email,
    t.initiated_sale_user_role,
    t.commission_type AS commission_type,
    t.commission_percentage AS commission_percentage,
    t.commission_is_vat_inclusive AS commission_is_vat_inclusive,
    t.revenue_is_service_fee_inclusive AS revenue_is_service_fee_inclusive,
    t.harper_order__stripe_customer_link,
    t.harper_order__halo_link,
    t.harper_order__id,
    t.order_id,
    t.item_info__item_id,
    t.id,
    COALESCE(tpe.dim_yearcalendarweek_sc, t.transaction_info__payment_at__dim_yearcalendarweek_sc) AS metric_week,
    COALESCE(tpe.dim_yearmonth_sc, t.transaction_info__payment_at__dim_yearmonth_sc) AS metric_month

FROM rep__transactionlog t
LEFT JOIN
    {{ schema }}.dim__time tpe ON t.harper_order__trial_period_actually_ended_at::date = tpe.dim_date_id
LEFT JOIN
    {{ schema }}.dim__time cdt ON t.createdat::date = cdt.dim_date_id

WHERE lineitem_category='product'
AND lineitem_type <> 'try_on'
ORDER BY t.createdat DESC
