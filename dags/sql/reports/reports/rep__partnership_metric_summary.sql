{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__partnership_metric_summary CASCADE;
{% endif %}

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__partnership_metric_summary AS

    WITH

    ship_directs AS (
        SELECT
            previous_original_order_name,
            id
        FROM
        {{ schema }}.rep__ship_direct_orders
    ),

    orders AS (
        SELECT
            o.*,
            o.order_type AS order__type,
            o.order_name AS order__name,
            -- Create date fields from dim_time joins
            dt_created.dim_month as order__createdat__dim_month,
            dt_created.dim_year as order__createdat__dim_year,
            dt_created.dim_date_id as order__createdat__dim_date,
            dt_created.dim_yearmonth_sc as order__createdat__dim_yearmonth,
            -- Add appointment date fields
            dt_appt.dim_month as appointment__date__dim_month,
            dt_appt.dim_year as appointment__date__dim_year,
            dt_appt.dim_date_id as appointment__date__dim_date,
            dt_appt.dim_yearmonth_sc as appointment__date__dim_yearmonth,
            CASE
                WHEN o.ship_direct = 1 AND (sd.previous_original_order_name IS NOT NULL AND sd.previous_original_order_name != '') THEN sd.previous_original_order_name
                ELSE o.original_order_name
            END AS original_order_name_merge -- Parent order_name

        FROM
            {{ schema }}.clean__order__summary o
        LEFT JOIN
            ship_directs sd ON o.id = sd.id
        LEFT JOIN
            {{ schema }}.dim__time dt_created ON o.createdat::date = dt_created.dim_date_id
        LEFT JOIN
            {{ schema }}.dim__time dt_appt ON o.appointment__date::date = dt_appt.dim_date_id
        WHERE
        o.link_order__is_child = 0
    )
    SELECT
        appointment__date__dim_month,
        appointment__date__dim_year,
        order__createdat__dim_month,
        order__createdat__dim_year,
        brand_name,
        happened,
        harper_product_type,
        COUNT(DISTINCT original_order_name_merge) AS num_merged_order_name,
        COUNT(DISTINCT order__name) AS num_order_name,
        COUNT( DISTINCT CASE WHEN success = 1 THEN original_order_name_merge ELSE NULL END ) AS num_success_orders,
        COUNT( DISTINCT CASE WHEN itemsummary__total_value_purchased > 250 THEN original_order_name_merge ELSE NULL END ) AS num_orders_over_250,
        COUNT( DISTINCT CASE WHEN itemsummary__total_value_purchased = 0 THEN original_order_name_merge ELSE NULL END ) AS num_no_sale_order,
        SUM(new_harper_customer) AS new_harper_customers,
        SUM(time_in_appointment) AS total_time_in_appointment,
        SUM(time_to_appointment) AS total_time_to_appointment,
        SUM(itemsummary__num_items_ordered) AS num_items_ordered,
        SUM(itemsummary__num_items_fulfilled) AS num_items_fulfilled,
        SUM(itemsummary__num_purchased) AS num_purchased,
        SUM(itemsummary__num_returned) AS num_returned,
        SUM(itemsummary__num_purchased_net) AS num_purchased_net,
        SUM(itemsummary__num_actually_purchased) AS num_actually_purchased,
        SUM(itemsummary__num_preorder) AS num_preorder,
        SUM(itemsummary__num_received_by_harper_warehouse) AS num_received_by_harper_warehouse,
        SUM(itemsummary__num_received_by_partner_warehouse) AS num_received_by_partner_warehouse,
        SUM(itemsummary__num_return_requested_by_customer) AS num_return_requested_by_customer,
        SUM(itemsummary__num_return_sent_by_customer) AS num_return_sent_by_customer,

        -- Value Summary
        SUM(itemsummary__total_value_ordered) AS total_value_ordered,
        SUM(itemsummary__total_value_purchased) AS total_value_purchased,
        SUM(itemsummary__total_value_returned) AS total_value_returned,
        SUM(itemsummary__total_value_received) AS total_value_received,
        SUM(itemsummary__total_value_purchased_net) AS total_value_purchased_net,
        SUM(itemsummary__total_value_received_by_warehouse) AS total_value_received_by_warehouse,

        -- Initiated Sale Summary
        SUM(itemsummary__initiated_sale__num_ordered) AS initiated_sale__num_ordered,
        SUM(itemsummary__initiated_sale__num_items_fulfilled) AS initiated_sale__num_items_fulfilled,
        SUM(itemsummary__initiated_sale__num_purchased) AS initiated_sale__num_purchased,
        SUM(itemsummary__initiated_sale__num_returned) AS initiated_sale__num_returned,
        SUM(itemsummary__initiated_sale__num_actually_purchased) AS initiated_sale__num_actually_purchased,
        SUM(itemsummary__initiated_sale__num_preorder) AS initiated_sale__num_preorder,
        SUM(itemsummary__initiated_sale__num_received_by_harper_warehouse) AS initiated_sale__num_received_by_harper_warehouse,
        SUM(itemsummary__initiated_sale__num_received_by_partner_warehouse) AS initiated_sale__num_received_by_partner_warehouse,
        SUM(itemsummary__initiated_sale__num_return_requested_by_customer) AS initiated_sale__num_return_requested_by_customer,
        SUM(itemsummary__initiated_sale__num_return_sent_by_customer) AS initiated_sale__num_return_sent_by_customer,

        -- Initiated Sale Value Summary
        SUM(itemsummary__initiated_sale__total_value_ordered) AS initiated_sale__total_value_ordered,
        SUM(itemsummary__initiated_sale__total_value_purchased) AS initiated_sale__total_value_purchased,
        SUM(itemsummary__initiated_sale__total_value_returned) AS initiated_sale__total_value_returned,
        SUM(itemsummary__initiated_sale__total_value_received) AS initiated_sale__total_value_received,
        SUM(itemsummary__initiated__total_value_received_wh) AS initiated__total_value_received_wh,

        -- Inspire Me Summary
        SUM(itemsummary__inspire_me__num_ordered) AS inspire_me__num_ordered,
        SUM(itemsummary__inspire_me__num_items_fulfilled) AS inspire_me__num_items_fulfilled,
        SUM(itemsummary__inspire_me__num_purchased) AS inspire_me__num_purchased,
        SUM(itemsummary__inspire_me__num_returned) AS inspire_me__num_returned,
        SUM(itemsummary__inspire_me__num_actually_purchased) AS inspire_me__num_actually_purchased,
        SUM(itemsummary__inspire_me__num_preorder) AS inspire_me__num_preorder,
        SUM(itemsummary__inspire_me__num_received_by_harper_warehouse) AS inspire_me__num_received_by_harper_warehouse,
        SUM(itemsummary__inspire_me__num_received_by_partner_warehouse) AS inspire_me__num_received_by_partner_warehouse,
        SUM(itemsummary__inspire_me__num_return_requested_by_customer) AS inspire_me__num_return_requested_by_customer,
        SUM(itemsummary__inspire_me__num_return_sent_by_customer) AS inspire_me__num_return_sent_by_customer,

        -- Inspire Me Value Summary
        SUM(itemsummary__inspire_me__total_value_ordered) AS inspire_me__total_value_ordered,
        SUM(itemsummary__inspire_me__total_value_purchased) AS inspire_me__total_value_purchased,
        SUM(itemsummary__inspire_me__total_value_returned) AS inspire_me__total_value_returned,
        SUM(itemsummary__inspire_me__total_value_received) AS inspire_me__total_value_received,
        SUM(itemsummary__inspire_me__total_value_received_wh) AS inspire_me__total_value_received_wh

    FROM
        orders o
    GROUP BY
        appointment__date__dim_month,
        appointment__date__dim_year,
        order__createdat__dim_month,
        order__createdat__dim_year,
        brand_name,
        happened,
        harper_product_type

WITH NO DATA;

{% if is_modified %}
CREATE INDEX IF NOT EXISTS rep__partnership_metric_summary_brand_name ON {{ schema }}.rep__partnership_metric_summary (brand_name);
CREATE INDEX IF NOT EXISTS rep__partnership_metric_summary_appointment__date__dim_year ON {{ schema }}.rep__partnership_metric_summary (appointment__date__dim_year);
CREATE INDEX IF NOT EXISTS rep__partnership_metric_summary_appointment__date__dim_month ON {{ schema }}.rep__partnership_metric_summary (appointment__date__dim_month);
CREATE INDEX IF NOT EXISTS rep__partnership_metric_summary_order__createdat__dim_year ON {{ schema }}.rep__partnership_metric_summary (order__createdat__dim_year);
CREATE INDEX IF NOT EXISTS rep__partnership_metric_summary_order__createdat__dim_month ON {{ schema }}.rep__partnership_metric_summary (order__createdat__dim_month);
{% endif %}

REFRESH MATERIALIZED VIEW {{ schema }}.rep__partnership_metric_summary;
