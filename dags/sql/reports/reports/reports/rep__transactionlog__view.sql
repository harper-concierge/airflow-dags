DROP VIEW IF EXISTS {{ schema }}.rep__transactionlog__view CASCADE;
CREATE VIEW {{ schema }}.rep__transactionlog__view AS

SELECT
    t.transaction_info__payment_at__dim_date,
    t.transaction_info__payment_at__dim_yearcalendarweek_sc,
    t.transaction_info__payment_at__dim_yearmonth_sc,
    t.transaction_info__payment_reference,
    t.transaction_info__payment_reference_id,
    t.transaction_info__payment_provider,
    t.transaction_info__payment_currency,
    t.lineitem_type,
    t.lineitem_category,
    t.partner_name,
    t.harper_product_type,
    t.order_type,
    t.harper_order__order_status,
    t.harper_order_name,
    t.partner_order_name,
    t.harper_order__createdat,
    t.harper_order__createdat__dim_date,
    t.harper_order__createdat__dim_yearcalendarweek_sc,
    t.harper_order__createdat__dim_yearmonth_sc,
    t.harper_order__customer__first_name,
    t.harper_order__customer__last_name,
    t.transaction_info__total_purchased_amount,
    t.transaction_info__total_try_on_amount,
    t.transaction_info__payment_invoiced_amount,
    t.transaction_info__item_count,
    t.harper_order__trial_period_actually_ended_at,
    t.harper_order__waive_reason,
    t.calculated_discount,
    t.calculated_discount_code,
    t.calculated_discount_percent,
    t.lineitem_name,
    t.lineitem_billed_quantity,
    t.lineitem_amount,
    t.item_info__price,
    t.item_info__sku,
    t.item_info__colour,
    t.item_info__size,
    t.item_info__billed_qty,
    t.item_info__discount,
    t.item_info__is_initiated_sale,
    t.initiated_sale_type,
    t.initiated_sale_user_email,
    t.initiated_sale_user_role,
    t.commission_type AS commission_type,
    t.commission_percentage AS commission_percentage,
    t.commission_is_vat_inclusive AS commission_is_vat_inclusive,
    t.revenue_is_service_fee_inclusive AS revenue_is_service_fee_inclusive,
    t.harper_order__style_concierge_name,
    t.try_commission_chargeable,
    t.try_commission_chargeable_at,
    t.try_chargeable_at__dim_date,
    t.try_chargeable_at__dim_yearcalendarweek_sc,
    t.try_chargeable_at__dim_yearmonth_sc,
    t.transaction_info__payment_at,
    t.harper_order__stripe_customer_link,
    t.harper_order__halo_link,
    t.harper_order__id,
    t.item_info__item_id,

    dt.dim_yearcalendarweek_sc AS trial_period_ended_at__dim_yearcalendarweek_sc,
    dt.dim_yearmonth_sc AS trial_period_ended_at__dim_yearmonth_sc,
    dt.dim_year AS trial_period_ended_at__dim_year,
    dt.dim_date_id AS trial_period_ended_at,
    cdt.dim_yearcalendarweek_sc AS createdat__dim_yearcalendarweek_sc,
    cdt.dim_yearmonth_sc AS createdat__dim_yearmonth_sc,
    cdt.dim_year AS createdat__dim_year,
    cdt.dim_date_id AS createdat,
    t.id

FROM
    rep__transactionlog t
LEFT JOIN
    {{ schema }}.dim__time dt ON t.harper_order__trial_period_actually_ended_at::date = dt.dim_date_id
LEFT JOIN
    {{ schema }}.dim__time cdt ON t.createdat::date = cdt.dim_date_id

ORDER BY
    t.createdat DESC
