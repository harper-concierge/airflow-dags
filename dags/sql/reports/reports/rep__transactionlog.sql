{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__transactionlog CASCADE;
{% endif %}

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__transactionlog AS
    SELECT
        ho.brand_name AS partner_name,
        ho.harper_product_type,
        ho.try_commission_chargeable as try_commission_chargeable,
        ho.try_commission_chargeable_at as try_commission_chargeable_at,
        ho.try_chargeable_at__dim_date,
        ho.try_chargeable_at__dim_yearcalendarweek_sc,
        ho.try_chargeable_at__dim_yearmonth_sc,
        ti.*,
        i.calculated_discount,
        i.calculated_discount_code,
        i.calculated_discount_percent,

        CASE WHEN i.harper_order_name IS NULL THEN
            ho.order_name
        ELSE
            i.harper_order_name
        END AS harper_order_name,

        CASE WHEN i.partner_order_name IS NULL THEN
            ho.original_order_name
        ELSE
            i.partner_order_name
        END AS partner_order_name,
        -- if discount percentage, take
        --price * discount_percentage = discount_amount

        i.commission__commission_type as commission_type,
        i.commission__percentage as commission_percentage,
        i.commission_is_vat_inclusive as commission_is_vat_inclusive,
        i.revenue_is_service_fee_inclusive as revenue_is_service_fee_inclusive,
        i.is_initiated_sale as is_initiated_sale,
        i.initiated_sale__initiated_sale_type as initiated_sale_type,
        i.initiated_sale__original_order_id as initiated_sale_original_order_id,
        i.initiated_sale__user_email as initiated_sale_user_email,
        i.initiated_sale__user_role as initiated_sale_user_role,
        i.is_inspire_me as is_inspire_me,
        i.initiated_sale__inspire_me_option_selected as inspire_me_option_selected,
        i.initiated_sale__inspire_me_description as inspire_me_description,
        i.order_type AS order_type,
        {{ clean__transaction__summary_columns | prefix_columns('t', 'transaction_info', exclude_columns=[]) }},
        {{ rep__deduped_order_items_columns | prefix_columns('i', 'item_info') }},
        {{ rep__order__summary_columns | prefix_columns('ho', 'harper_order') }}
    FROM
        {{ schema }}.clean__transaction__items ti
    LEFT JOIN {{ schema }}.clean__transaction__summary t ON t.id = ti.transaction_id
    LEFT JOIN {{ schema }}.rep__deduped_order_items i on ti.item_id = i.item_id AND i.is_link_order_child_item = 0
    LEFT JOIN {{ schema }}.rep__order__summary ho ON ti.order_id = ho.id

WITH NO DATA;

{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__transactionlog_idx ON {{ schema }}.rep__transactionlog (id);
CREATE INDEX IF NOT EXISTS rep__transactionlog_harper_order_name_idx ON {{ schema }}.rep__transactionlog (harper_order_name);
CREATE INDEX IF NOT EXISTS rep__transactionlog_harper_order_id_idx ON {{ schema }}.rep__transactionlog (order_id);
CREATE INDEX IF NOT EXISTS rep__transactionlog_partner_order_name_idx ON {{ schema }}.rep__transactionlog (partner_order_name);
CREATE INDEX IF NOT EXISTS rep__transactionlog_try_commission_chargeable_idx ON {{ schema }}.rep__transactionlog (try_commission_chargeable);

CREATE INDEX IF NOT EXISTS rep__transactionlog_is_initiated_sale_idx ON {{ schema }}.rep__transactionlog (is_initiated_sale);
CREATE INDEX IF NOT EXISTS rep__transactionlog_is_inspire_me_idx ON {{ schema }}.rep__transactionlog (is_inspire_me);
CREATE INDEX IF NOT EXISTS rep__transactionlog_order_type_idx ON {{ schema }}.rep__transactionlog (order_type);
CREATE INDEX IF NOT EXISTS rep__transactionlog_item_id ON {{ schema }}.rep__transactionlog (item_id);

{% endif %}

REFRESH MATERIALIZED VIEW {{ concurrently }} {{ schema }}.rep__transactionlog;
