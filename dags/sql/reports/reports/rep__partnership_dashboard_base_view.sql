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
-- monthly shopify brand data
monthly_kpi_data AS (
   SELECT DISTINCT
       partner_name,
       year_month_created,
       analysis_region,
       orders AS monthly_brand_orders,
       total_value_ordered AS monthly_brand_value,
       total_value_kept AS monthly_net_value_purchased,
       total_items_kept AS monthly_net_items_purchased,
       total_items_ordered AS monthly_items_ordered,
       new_customer_orders AS monthly_new_customer_orders
   FROM {{ schema }}.rep__shopify_partner_monthly_summary
   WHERE channel = 'Online Store'
),
-- daily shopify brand data
daily_summary AS (
   SELECT DISTINCT
       partner_name,
       day_created,
       analysis_region,
       orders as daily_brand_orders,
       total_value_ordered as daily_brand_value,
       total_value_kept as daily_net_value_purchased,
       total_items_kept as daily_net_items_purchased,
       total_items_ordered as daily_items_ordered,
       new_customer_orders as daily_new_customer_orders
   FROM {{ schema }}.rep__shopify_partner_daily_summary
   WHERE channel = 'Online Store'
),
-- Harper orders
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
       dt.dim_year AS order__createdat__dim_year,
       dt.dim_month AS order__createdat__dim_month,
       dt.dim_date_id AS order__createdat__dim_date,
       dt.dim_yearmonth_sc AS order__createdat__dim_yearmonth,
       dt_appt.dim_month as appointment__date__dim_month_,
       dt_appt.dim_year as appointment__date__dim_year_,
       dt_appt.dim_date_id as appointment__date__dim_date_,
       dt_appt.dim_yearmonth_sc as appointment__date__dim_yearmonth_,
       dt_tp_start.dim_date_id AS tp_actually_started__dim_date,
       CASE
           WHEN o.ship_direct = 1 AND sd.previous_original_order_name != ''
           THEN sd.previous_original_order_name
           ELSE o.original_order_name
       END AS original_order_name_merge,
       CASE
           WHEN order_cancelled_status IN ('Cancelled post shipment', 'Cancelled - no email triggered', 'Cancelled pre shipment')
            OR o.order_status = 'cancelled'
           THEN 1 ELSE 0
       END AS is_cancelled,
       CASE
           WHEN o.order_type = 'ship_direct' THEN o.createdat
           WHEN o.order_type = 'harper_try' THEN
               COALESCE(dt_tp_end.dim_date_id, o.trial_period_end_at)
           ELSE
               COALESCE(o.appointment_completed_at::date, dt_appt.dim_date_id)
       END AS completion_date,
       i.item_value_pence AS item__item_value_pence,
       i.is_inspire_me AS item_is_inspire_me,
       i.is_initiated_sale AS item_is_initiated_sale

   FROM {{ schema }}.rep__deduped_order_items i
   LEFT JOIN {{ schema }}.clean__order__summary o ON o.id = i.order_id
   LEFT JOIN ship_directs sd ON o.id = sd.id
   LEFT JOIN {{ schema }}.dim__time dt ON o.createdat::date = dt.dim_date_id
   LEFT JOIN {{ schema }}.dim__time dt_appt ON o.appointment__date::date = dt_appt.dim_date_id
   LEFT JOIN {{ schema }}.dim__time dt_tp_start ON o.trial_period_actually_started_at::date = dt_tp_start.dim_date_id
   LEFT JOIN {{ schema }}.dim__time dt_tp_end ON o.trial_period_actually_ended_at::date = dt_tp_end.dim_date_id
   WHERE i.is_link_order_child_item = 0 AND o.link_order__is_child = 0
)

SELECT
   original_order_name_merge,
   order__name,
   customer_id,
   brand_name,
   order__type,
   harper_product_type,
   order__status,
   happened,
   order__createdat__dim_date AS order_created_date,
   is_cancelled,
   REPLACE(order__createdat__dim_yearmonth, '/', '-') AS order__createdat__dim_yearmonth,
   order__createdat__dim_year,
   order__createdat__dim_month,
   tp_actually_ended__dim_date,
   tp_actually_started__dim_date,
   try_commission_chargeable,
   try_commission_chargeable_at,
   shipping_address__postcode,
   CASE
           WHEN MAX(CAST(item_is_initiated_sale AS INT)) = 1 THEN 1 ELSE 0
           END AS contains_initiated_sale,
   CASE
       WHEN MAX(CAST(item_is_inspire_me AS INT)) = 1 THEN 1 ELSE 0
   END AS contains_inspire_me,
   CASE
       WHEN MAX(new_harper_customer) = 1 THEN 'New Harper Customer'
       ELSE 'Returning Harper Customer'
   END AS customer_type_,
   TO_CHAR(completion_date, 'YYYY-MM-DD')::date as completion_date,
   MAX(appointment__date__dim_date_) AS appointment__date__dim_date_,
   MAX(appointment__date__dim_month_) AS appointment__date__dim_month_,
   MAX(appointment__date__dim_year_) AS appointment__date__dim_year_,
   MAX(appointment__date__dim_yearmonth_) AS appointment__date__dim_yearmonth_,
   MAX(time_in_appointment) AS time_in_appointment,
   MAX(time_to_appointment) AS time_to_appointment,
   CAST(NULLIF(discount_total, ' ') AS NUMERIC) AS discount_total,

   -- Inspire Me metrics
   SUM(CASE WHEN item_is_inspire_me = 1 THEN 1 ELSE 0 END) AS inspire_me_items_ordered,
   SUM(CASE WHEN item_is_inspire_me = 1 AND purchased = 1 THEN 1 ELSE 0 END) AS inspire_me_items_purchased,
   SUM(CASE WHEN item_is_inspire_me = 1 AND returned = 1 THEN 1 ELSE 0 END) AS inspire_me_items_returned,
   SUM(CASE WHEN item_is_inspire_me = 1 THEN item__item_value_pence ELSE 0 END)/100 AS inspire_me_ordered_value,
   SUM(CASE WHEN item_is_inspire_me = 1 AND purchased = 1 THEN item__item_value_pence ELSE 0 END)/100 AS inspire_me_purchased_value,
   SUM(CASE WHEN item_is_inspire_me = 1 AND returned = 1 THEN item__item_value_pence ELSE 0 END)/100 AS inspire_me_returned_value,

   -- Initiated Sale metrics
   SUM(CASE WHEN item_is_initiated_sale = 1 THEN 1 ELSE 0 END) AS initiated_sale_ordered,
   SUM(CASE WHEN item_is_initiated_sale = 1 AND purchased = 1 THEN 1 ELSE 0 END) AS initiated_sale_items_purchased,
   SUM(CASE WHEN item_is_initiated_sale = 1 AND returned = 1 THEN 1 ELSE 0 END) AS initiated_sale_items_returned,
   SUM(CASE WHEN item_is_initiated_sale = 1 THEN item__item_value_pence ELSE 0 END)/100 AS initiated_sale_ordered_value,
   SUM(CASE WHEN item_is_initiated_sale = 1 AND purchased = 1 THEN item__item_value_pence ELSE 0 END)/100 AS initiated_sale_purchased_value,
   SUM(CASE WHEN item_is_initiated_sale = 1 AND returned = 1 THEN item__item_value_pence ELSE 0 END)/100 AS initiated_sale_returned_value,

   -- Item counts
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
   SUM(item__item_value_pence)/100 AS ordered_value,
   SUM(CASE WHEN purchased = 1 THEN item__item_value_pence ELSE 0 END)/100 AS purchased_value,
   SUM(CASE WHEN returned = 1 THEN item__item_value_pence ELSE 0 END)/100 AS returned_value,
   SUM(CASE WHEN missing = 1 THEN item__item_value_pence ELSE 0 END)/100 AS missing_value,
   SUM(CASE WHEN received = 1 THEN item__item_value_pence ELSE 0 END)/100 AS received_value,

   CASE
       WHEN harper_product_type = 'harper_concierge' THEN 'London'
       ELSE 'Regional'
   END as kpi_source,

   -- Daily KPIs
   ds.daily_brand_orders,
   ds.daily_brand_value,
   ds.daily_net_value_purchased,
   ds.daily_net_items_purchased,
   ds.daily_items_ordered,
   ds.daily_new_customer_orders,

   -- Monthly KPIs
   m.monthly_brand_orders,
   m.monthly_brand_value,
   m.monthly_net_value_purchased,
   m.monthly_net_items_purchased,
   m.monthly_items_ordered,
   m.monthly_new_customer_orders

FROM base_orders bo
LEFT JOIN daily_summary ds
   ON ds.partner_name = bo.brand_name
   AND DATE(ds.day_created) = bo.order__createdat__dim_date
   AND ds.analysis_region = CASE
       WHEN harper_product_type = 'harper_concierge' THEN 'London'
       ELSE 'Regional'
   END
LEFT JOIN monthly_kpi_data m
   ON m.partner_name = bo.brand_name
   AND TO_CHAR(DATE(m.year_month_created), 'YYYY/MM') = bo.order__createdat__dim_yearmonth
   AND CASE
       WHEN harper_product_type = 'harper_concierge' THEN m.analysis_region = 'London'
       ELSE m.analysis_region = 'Regional'
   END
GROUP BY
   original_order_name_merge,
   order__name,
   customer_id,
   brand_name,
   order__type,
   order__status,
   happened,
   is_cancelled,
   harper_product_type,
   shipping_address__postcode,
   order__createdat__dim_date,
   order__createdat__dim_yearmonth,
   order__createdat__dim_year,
   order__createdat__dim_month,
   appointment__date__dim_date_,
   appointment__date__dim_month_,
   appointment__date__dim_year_,
   appointment__date__dim_yearmonth_,
   completion_date,
   tp_actually_ended__dim_date,
   tp_actually_started__dim_date,
   try_commission_chargeable,
   try_commission_chargeable_at,
   discount_total,
   ds.daily_brand_orders,
   ds.daily_brand_value,
   ds.daily_net_value_purchased,
   ds.daily_net_items_purchased,
   ds.daily_items_ordered,
   ds.daily_new_customer_orders,
   m.monthly_brand_orders,
   m.monthly_brand_value,
   m.monthly_net_value_purchased,
   m.monthly_net_items_purchased,
   m.monthly_items_ordered,
   m.monthly_new_customer_orders

WITH NO DATA;

-- Create optimized indexes
{% if is_modified %}
CREATE INDEX IF NOT EXISTS rep__partnership_dashboard_base_view_original_order_name_idx ON {{ schema }}.rep__partnership_dashboard_base_view (original_order_name_merge);
CREATE INDEX IF NOT EXISTS rep__partnership_dashboard_base_view_order__name_idx ON {{ schema }}.rep__partnership_dashboard_base_view (order__name);
CREATE INDEX IF NOT EXISTS rep__partnership_dashboard_base_view_happened_idx ON {{ schema }}.rep__partnership_dashboard_base_view (happened);
CREATE INDEX IF NOT EXISTS rep__partnership_dashboard_base_view_brand_name_idx ON {{ schema }}.rep__partnership_dashboard_base_view (brand_name);
CREATE INDEX IF NOT EXISTS rep__partnership_dashboard_base_view_harper_product_type_idx ON {{ schema }}.rep__partnership_dashboard_base_view (harper_product_type);
CREATE INDEX IF NOT EXISTS rep__partnership_dashboard_base_view_order__type_idx ON {{ schema }}.rep__partnership_dashboard_base_view (order__type);
CREATE INDEX IF NOT EXISTS rep__partnership_dashboard_base_view_is_cancelled_idx ON {{ schema }}.rep__partnership_dashboard_base_view (is_cancelled);
CREATE INDEX IF NOT EXISTS rep__partnership_dashboard_base_view_order__status_idx ON {{ schema }}.rep__partnership_dashboard_base_view (order__status);
CREATE INDEX IF NOT EXISTS rep__partnership_dashboard_base_view_order__order_created_date_idx ON {{ schema }}.rep__partnership_dashboard_base_view (order_created_date);
CREATE INDEX IF NOT EXISTS rep__partnership_dashboard_base_view_completion_date_idx ON {{ schema }}.rep__partnership_dashboard_base_view (completion_date);

-- Composite indexes for common query patterns
CREATE INDEX idx_brand_kpi_daily ON {{ schema }}.rep__partnership_dashboard_base_view
(brand_name, order_created_date) INCLUDE (daily_brand_orders, daily_brand_value);

CREATE INDEX idx_brand_kpi_monthly ON {{ schema }}.rep__partnership_dashboard_base_view
(brand_name, order_created_date) INCLUDE (monthly_brand_orders, monthly_brand_value);

CREATE INDEX idx_brand_date ON {{ schema }}.rep__partnership_dashboard_base_view
    (brand_name, order_created_date);
CREATE INDEX idx_brand_type ON {{ schema }}.rep__partnership_dashboard_base_view
    (brand_name, order__type);

CREATE INDEX idx_time_analysis ON {{ schema }}.rep__partnership_dashboard_base_view
(order_created_date, order__createdat__dim_month, order__createdat__dim_year);

CREATE INDEX idx_brand_metrics ON {{ schema }}.rep__partnership_dashboard_base_view
(completion_date, brand_name, order__type) INCLUDE (ordered_value, purchased_value, number_items_ordered);

CREATE INDEX idx_daily_composite ON rep__partnership_dashboard_base_view
(brand_name, order_created_date, kpi_source);

CREATE INDEX idx_monthly_composite ON rep__partnership_dashboard_base_view
(brand_name, order__createdat__dim_yearmonth, kpi_source);

{% endif %}

-- Refresh the view
REFRESH MATERIALIZED VIEW {{ schema }}.rep__partnership_dashboard_base_view;
