--DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__shopify_partner_daily_summary CASCADE;
DROP VIEW IF EXISTS {{ schema }}.rep__shopify_partner_daily_summary CASCADE;

CREATE VIEW {{ schema }}.rep__shopify_partner_daily_summary AS
WITH london_data AS (
   SELECT *,
   CASE
       WHEN london = 1 THEN 'London'
       ELSE NULL
   END AS analysis_region
   FROM {{ schema }}.clean__shopify_partner_orders
   UNION ALL
   SELECT *,
   'Regional' as analysis_region
   FROM {{ schema }}.clean__shopify_partner_orders
)

SELECT
   ROW_NUMBER() OVER (ORDER BY TO_CHAR(created_at, 'YYYY-MM-dd'), partner_name) as id,
   TO_CHAR(created_at, 'YYYY-MM-dd') AS day_created,
   TO_CHAR(created_at, 'YYYY-MM-01') AS year_month_created,
   partner_name,
   partner_reference,
   channel,
   harper_product_type,
   analysis_region,
   COUNT(DISTINCT name) AS orders,
   COALESCE(SUM(items_quantity), 0) AS total_items_ordered,
   COALESCE(SUM(total_refund_quantity), 0) AS total_items_returned,
   COALESCE(SUM(net_items_quantity), 0) AS total_items_kept,
   COALESCE((ROUND(SUM(total_price_ex_shipping)::decimal, 2))::decimal, 0) AS total_value_ordered,
   COALESCE((ROUND(SUM(total_refunded)::decimal, 2))::decimal, 0) AS total_value_returned,
   COALESCE((ROUND((SUM(total_price_ex_shipping) - SUM(total_refunded))::decimal, 2))::decimal, 0) AS total_value_kept,
   COALESCE((ROUND((SUM(total_price_ex_shipping)/NULLIF(COUNT(DISTINCT name), 0))::decimal, 2))::decimal, 0) AS gross_AOV,
   COALESCE((ROUND(((SUM(total_price_ex_shipping) - SUM(total_refunded))/NULLIF(COUNT(DISTINCT CASE WHEN cancelled_at IS NULL THEN name ELSE NULL END), 0))::decimal, 2))::decimal, 0) AS net_ATV,
   COALESCE((ROUND((SUM(items_quantity)/NULLIF(COUNT(DISTINCT name), 0))::decimal, 2))::decimal, 0) AS gross_UPT,
   COALESCE((ROUND(((SUM(items_quantity) - SUM(total_refund_quantity))/NULLIF(COUNT(DISTINCT CASE WHEN cancelled_at IS NULL THEN name ELSE NULL END), 0))::decimal, 2))::decimal, 0) AS net_UPT,
   COALESCE((ROUND((SUM(total_price_ex_shipping)/NULLIF(SUM(items_quantity), 0))::decimal, 2))::decimal, 0) AS gross_ASP,
   COALESCE((ROUND(((SUM(total_price_ex_shipping) - SUM(total_refunded))/NULLIF((SUM(items_quantity) - SUM(total_refund_quantity)), 0))::decimal, 2))::decimal, 0) AS net_ASP,
   COALESCE((ROUND(((SUM(items_quantity) - SUM(total_refund_quantity))/NULLIF(SUM(items_quantity), 0))::decimal, 2))::decimal, 0) AS purchase_rate_items,
   COALESCE((ROUND(((SUM(total_price_ex_shipping) - SUM(total_refunded))/NULLIF(SUM(total_price_ex_shipping), 0))::decimal, 2))::decimal, 0) AS purchase_rate_value,
   COALESCE((ROUND((SUM(total_refund_quantity)/NULLIF(SUM(items_quantity), 0))::decimal, 2))::decimal, 0) AS return_rate_items,
   COALESCE((ROUND((SUM(total_refunded)/NULLIF(SUM(total_price_ex_shipping), 0))::decimal, 2))::decimal, 0) AS return_rate_value,
   ROUND((SUM(keep)::numeric/NULLIF(COUNT(DISTINCT name), 0)),2) AS keep_rate
FROM london_data
WHERE publication_name IN ('Online Store','Harper Concierge')
AND analysis_region IS NOT NULL
GROUP BY
   partner_name,
   partner_reference,
   TO_CHAR(created_at, 'YYYY-MM-dd'),
   TO_CHAR(created_at, 'YYYY-MM-01'),
   channel,
   harper_product_type,
   analysis_region;
