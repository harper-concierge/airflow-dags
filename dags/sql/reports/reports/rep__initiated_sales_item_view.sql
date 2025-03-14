{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__initiated_sales_item_view CASCADE;
{% endif %}

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__initiated_sales_item_view AS
WITH ship_directs AS (
    SELECT
        previous_original_order_name,
        id
    FROM
        public.rep__ship_direct_orders
),

order_items AS (
    SELECT
        o.*,
        i.*,
        o.order_type AS order__type,
        --i.order_type AS item__order_type,
        o.order_name AS order__name,
        i.order_name AS item__order_name,
        i.createdat AS item__createdat,
        dt_order.dim_date_id AS order__date,
        dt_order.dim_year AS order__year,
        dt_order.dim_month AS order__month,
        dt_order.dim_yearmonth_sc AS order__dim_yearmonth,
        o.createdat AS order__createdat,
        i.initiated_sale__user_role AS item__initiated_sale__user_role,
        i.commission__percentage AS item__commission__percentage,
        i.initiated_sale__createdat AS item__initiated_sale__createdat,
        i.initiated_sale__original_order_id AS item__initiated_sale__original_order_id,
        i.initiated_sale__user_email AS item__initiated_sale__user_email,
		i.initiated_sale__inspire_me_description AS item__initiated_sale__inspire_me_description,
        i.is_initiated_sale AS item__is_initiated_sale,
        i.updatedat AS item__updatedat,
        i.is_inspire_me AS item__is_inspire_me,
        o.initiated_sale__inspire_me_option_selected AS order__initiated_sale__inspire_me_option_selected,
        -- Add trial period dates
        dt_tp_start.dim_date_id AS tp_actually_started__dim_date,
        CASE
            WHEN o.ship_direct = 1 AND (sd.previous_original_order_name IS NOT NULL AND sd.previous_original_order_name != '') THEN sd.previous_original_order_name
            ELSE o.original_order_name
        END AS original_order_name_merge,
        i.item_value_pence AS item__item_value_pence
    FROM
        {{ schema }}.rep__deduped_order_items i
    LEFT JOIN
        {{ schema }}.clean__order__summary o ON o.id = i.order_id
    LEFT JOIN
        ship_directs sd ON o.id = sd.id
    LEFT JOIN
        {{ schema }}.dim__time dt_order ON o.createdat::date = dt_order.dim_date_id
    LEFT JOIN
        {{ schema }}.dim__time dt_tp_start ON o.trial_period_actually_started_at::date = dt_tp_start.dim_date_id
    LEFT JOIN
        {{ schema }}.dim__time dt_tp_end ON o.trial_period_actually_ended_at::date = dt_tp_end.dim_date_id
    WHERE
        i.is_link_order_child_item = 0
        AND o.link_order__is_child = 0
        --AND i.is_initiated_sale = 1
)

SELECT
    appointment__date__dim_date,
    appointment_completed_at,
    brand_name,
    calculated_item_discount_price_pence,
    calculated_item_value_pence,
    colour,
    commission__calculated_amount,
    CASE
	WHEN order__type = 'harper_try' THEN
        CASE
            WHEN tp_actually_ended__dim_date IS NOT NULL THEN tp_actually_ended__dim_date
            ELSE trial_period_end_at
        END
    WHEN appointment_completed_at IS NULL  THEN appointment__date__dim_date
    ELSE DATE(appointment_completed_at)
    END AS completion_date,
	DATE(customer__createdat) AS customer__first_order,
    customer__shipping_address__postcode,
	customer__total_orders,
    ROUND(CAST(NULLIF(discount_total, ' ') AS NUMERIC) / 100.0, 2) AS discount_total,
    favourite_brands,
	happened,
    harper_order_name,
    harper_product_type,
    idx,
    images,
    order__initiated_sale__inspire_me_option_selected,
	CASE WHEN is_inspire_me = 1 THEN item__item_value_pence ELSE 0 END AS inspire_me_value_pence,
    is_inspire_me,
    item__commission__percentage,
    item__createdat,
    item__initiated_sale__createdat,
    item__initiated_sale__original_order_id,
	item__initiated_sale__inspire_me_description,
    item__initiated_sale__user_email,
    item__initiated_sale__user_role,
    item__is_initiated_sale,
    item__order_name,
    item__order_type,
    item__updatedat,
    item__item_value_pence,
    item_discount_price_pence,
    item_price_pence,
    item_quantity,
    item_value_pence,
    link_order__is_child,
    missing,
    not_available,
    order__createdat,
    order__date,
    order__dim_yearmonth,
    order__name,
    order__type,
    order__year,
    order_id,
    order_cancelled_status,
	order_status,
    original_order_name_merge,
    out_of_stock,
    partner_order_name,
    post_purchase_return,
    preorder,
    product_id,
    product_name,
    product_tags,
    product_type,
    purchased,
    qty,
    received,
    received_by_warehouse,
    return_reason,
    return_requested_by_customer,
    return_sent_by_customer,
    returned,
    return_status,
    size,
    sku,
    style_concierge,
    style_concierge_name,
    through_door_actual,
    time_in_appointment,
    time_late,
    tp_actually_ended__dim_date,
    tp_actually_started__dim_date,
    try_chargeable_at__dim_date,
    unpurchased_return,
    variant_id
FROM order_items

WITH NO DATA;

{% if is_modified %}
CREATE INDEX IF NOT EXISTS rep__initiated_sales_item_view_original_order_name_idx ON {{ schema }}.rep__initiated_sales_item_view (original_order_name_merge);

{% endif %}

REFRESH MATERIALIZED VIEW {{ schema }}.rep__initiated_sales_item_view;
