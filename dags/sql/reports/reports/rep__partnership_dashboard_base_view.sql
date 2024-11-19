{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__partnership_dashboard_base_view CASCADE;
{% endif %}

-- Create optimized materialized view with partitioning
CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__partnership_dashboard_base_view AS
    WITH
    ship_directs AS (
        SELECT previous_original_order_name, id
        FROM {{ schema }}.rep__ship_direct_orders
    ),

    base_orders AS (
        SELECT
            o.*,
            i.*,
            o.order_type AS order__type,
            i.order_type AS item___order_type,
            o.order_name AS order__name,
            i.order_name AS item__order_name,
            o.order_status AS order__status,
            i.createdat AS item__createdat,
            o.createdat AS order__createdat,
            o.createdat__dim_year AS order__createdat__dim_year,
            o.createdat__dim_month AS order__createdat__dim_month,
            o.createdat__dim_date AS order__createdat__dim_date,
            o.createdat__dim_yearmonth AS order__createdat__dim_yearmonth,
            CASE
                WHEN o.ship_direct = 1 AND sd.previous_original_order_name != ''
                THEN sd.previous_original_order_name
                ELSE o.original_order_name
            END AS original_order_name_merge,
            CASE
                WHEN order_cancelled_status LIKE '%%Cancelled%%' OR o.order_status = 'cancelled'
                THEN 1 ELSE 0
            END AS is_cancelled,
            CASE
                WHEN o.order_type = 'ship_direct' THEN o.createdat
                WHEN o.order_type = 'harper_try' THEN
                    COALESCE(o.tp_actually_ended__dim_date, o.trial_period_end_at)
                ELSE
                    COALESCE(o.appointment_completed_at::date, o.appointment__date__dim_date)
            END AS completion_date,
            i.item_value_pence AS item__item_value_pence,
            i.is_inspire_me AS item_is_inspire_me,
            i.is_initiated_sale AS item_is_initiated_sale

        FROM {{ schema }}.rep__deduped_order_items i
        LEFT JOIN {{ schema }}.clean__order__summary o ON o.id = i.order_id
        LEFT JOIN ship_directs sd ON o.id = sd.id
        WHERE i.is_link_order_child_item = 0 AND o.link_order__is_child = 0
    )

    SELECT
        original_order_name_merge,
        order__name,
        customer_id,
        brand_name,
        order__type,
        order__status,
        happened,
        order__createdat__dim_date,
        REPLACE(bo.order__createdat__dim_yearmonth, '/', '-') AS order__createdat__dim_yearmonth,
        order__createdat__dim_year,
        order__createdat__dim_month,
        TO_CHAR(completion_date, 'YYYY-MM-DD')::date as completion_date,
        MAX(appointment__date__dim_date) AS appointment__date__dim_date,
        MAX(appointment__date__dim_month) AS appointment__date__dim_month,
        MAX(appointment__date__dim_year) AS appointment__date__dim_year,
        MAX(appointment__date__dim_yearmonth) AS appointment__date__dim_yearmonth,
        MAX(time_in_appointment) AS time_in_appointment,
        MAX(time_to_appointment) AS time_to_appointment,
        ROUND(CAST(NULLIF(discount_total, ' ') AS NUMERIC) / 100.0, 2) AS discount_total,

        -- Inspire Me metrics
        SUM(CASE WHEN item_is_inspire_me = 1 THEN 1 ELSE 0 END) AS inspire_me_items_ordered,
        SUM(CASE WHEN item_is_inspire_me = 1 THEN item__item_value_pence ELSE 0 END) AS inspire_me_value,
        SUM(CASE WHEN item_is_inspire_me = 1 AND purchased = 1 THEN 1 ELSE 0 END) AS inspire_me_items_purchased,
        SUM(CASE WHEN item_is_inspire_me = 1 AND returned = 1 THEN 1 ELSE 0 END) AS inspire_me_items_returned,

        -- Initiated Sale metrics
        SUM(CASE WHEN item_is_initiated_sale = 1 THEN 1 ELSE 0 END) AS initiated_sale_ordered,
        SUM(CASE WHEN item_is_initiated_sale = 1 THEN item__item_value_pence ELSE 0 END) AS initiated_sale_value,
        SUM(CASE WHEN item_is_initiated_sale = 1 AND purchased = 1 THEN 1 ELSE 0 END) AS initiated_sale_items_purchased,
        SUM(CASE WHEN item_is_initiated_sale = 1 AND returned = 1 THEN 1 ELSE 0 END) AS initiated_sale_items_returned,

        -- Item status counts
        SUM(missing) AS number_items_missing,
        SUM(not_available) AS number_items_not_available,
        SUM(out_of_stock) AS number_items_out_of_stock,
        SUM(post_purchase_return) AS number_items_post_purchase_return,
        SUM(preorder) AS number_items_preorder,
        SUM(purchased) AS number_items_purchased,
        SUM(qty) AS number_items_ordered,
        SUM(received) AS number_items_received,
        SUM(received_by_warehouse) AS number_items_received_by_warehouse,
        SUM(returned) AS number_items_returned,
        SUM(unpurchased_return) AS number_items_unpurchased_return,

        -- Value calculations
        SUM(item__item_value_pence) AS total_value,
        SUM(CASE WHEN purchased = 1 THEN item__item_value_pence ELSE 0 END) AS purchased_value,
        SUM(CASE WHEN returned = 1 THEN item__item_value_pence ELSE 0 END) AS returned_value,
        SUM(CASE WHEN missing = 1 THEN item__item_value_pence ELSE 0 END) AS missing_value,

        -- Brand Metrics
        ss.orders AS regional_orders,
        ss.total_value_ordered AS regional_value_ordered,
        ss.net_ATV AS regional_net_ATV,
        ss.net_UPT AS regional_net_UPT,
        sl.orders AS london_orders,
        sl.total_value_ordered AS london_value_ordered,
        sl.net_ATV AS london_net_atv,
        sl.net_UPT AS london_net_UPT

    FROM base_orders bo
    LEFT JOIN rep__shopify_partner_monthly_london sl -- Joining to get online store values
        ON sl.partner__name = bo.brand_name
        AND REPLACE(bo.order__createdat__dim_yearmonth, '/', '-') = sl.month
    LEFT JOIN rep__shopify_partner_monthly_summary ss
        ON ss.partner__name = bo.brand_name
        AND REPLACE(bo.order__createdat__dim_yearmonth, '/', '-') = ss.month
	WHERE ss.source = 'Web'
	AND sl.source = 'Web'
    GROUP BY
        original_order_name_merge,
        order__name,
        customer_id,
        brand_name,
        order__type,
        order__status,
        happened,
        order__createdat__dim_date,
        order__createdat__dim_yearmonth,
        order__createdat__dim_year,
        order__createdat__dim_month,
        completion_date,
        discount_total,
 		ss.orders,
        ss.total_value_ordered,
        ss.net_ATV ,
        ss.net_UPT ,
        sl.orders ,
        sl.total_value_ordered,
        sl.net_ATV ,
        sl.net_UPT

WITH NO DATA;

-- Create optimized indexes
{% if is_modified %}
CREATE INDEX IF NOT EXISTS rep__partnership_dashboard_base_view_original_order_name_idx ON {{ schema }}.rep__partnership_dashboard_base_view (original_order_name_merge);
CREATE INDEX IF NOT EXISTS rep__partnership_dashboard_base_view_brand_name_idx ON {{ schema }}.rep__partnership_dashboard_base_view (brand_name);
CREATE INDEX IF NOT EXISTS rep__partnership_dashboard_base_view_order__type_idx ON {{ schema }}.rep__partnership_dashboard_base_view (order__type);
CREATE INDEX IF NOT EXISTS rep__partnership_dashboard_base_view_order__status_idx ON {{ schema }}.rep__partnership_dashboard_base_view (order__status);
CREATE INDEX IF NOT EXISTS rep__partnership_dashboard_base_view_order__order_created_date_idx ON {{ schema }}.rep__partnership_dashboard_base_view (order__createdat__dim_date);
CREATE INDEX IF NOT EXISTS rep__partnership_dashboard_base_view_completion_date_idx ON {{ schema }}.rep__partnership_dashboard_base_view (completion_date);
-- Composite indexes for common query patterns
CREATE INDEX idx_brand_date ON {{ schema }}.rep__partnership_dashboard_base_view
    (brand_name, order__createdat__dim_date);
CREATE INDEX idx_brand_type ON {{ schema }}.rep__partnership_dashboard_base_view
    (brand_name, order__type);
-- For time-based analysis
CREATE INDEX idx_time_analysis ON {{ schema }}.rep__partnership_dashboard_base_view
(order__createdat__dim_date, order__createdat__dim_month, order__createdat__dim_year);
-- For brand/order analysis
CREATE INDEX idx_brand_metrics ON {{ schema }}.rep__partnership_dashboard_base_view
(completion_date, brand_name, order__type) INCLUDE (total_value, purchased_value, number_items_ordered);
-- Unique index
CREATE UNIQUE INDEX rep__partnership_dashboard_base_view_unique_idx
ON {{ schema }}.rep__partnership_dashboard_base_view (order__name, order__createdat__dim_date,order__type);
{% endif %}

-- Refresh the view
REFRESH MATERIALIZED VIEW {{ schema }}.rep__partnership_dashboard_base_view;

-- Analyze for query optimization
ANALYZE {{ schema }}.rep__partnership_dashboard_base_view;
