--DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__shopify_partner_daily_summary CASCADE;
DROP VIEW IF EXISTS {{ schema }}.rep__shopify_partner_daily_summary CASCADE;

CREATE VIEW {{ schema }}.rep__shopify_partner_daily_summary AS
    WITH base_orders AS (
   SELECT
    po.harper_product AS harper__product,
    po.name,
    po.london,
    po.created_at,
    po.partner__name,
    po.partner__reference,
    po.items_ordered,
    po.items_returned,
    po.value_ordered,
    po.value_returned,
    po.cancelled_at,
    po.publication_name,
    po.keep
   FROM {{ schema }}.clean__shopify_partner_orders po
   LEFT JOIN {{ schema }}.clean__order__summary co ON po.name = co.order_name
   WHERE DATE(po.created_at) >= CURRENT_DATE - INTERVAL '6 months'
),

london_data AS (
   SELECT *,
   CASE
       WHEN london = 1 THEN 'London'
       ELSE NULL
   END AS analysis_region
   FROM base_orders
   UNION ALL
   SELECT *,
   'Regional' as analysis_region
   FROM base_orders
)

SELECT
   ROW_NUMBER() OVER (ORDER BY TO_CHAR(created_at, 'YYYY-MM-dd'), partner__name) as id,
   TO_CHAR(created_at, 'YYYY-MM-dd') AS day,
   TO_CHAR(created_at, 'YYYY-MM-01') AS year_month,
   partner__name,
   partner__reference,
   publication_name,
   harper__product,
   analysis_region,
   COUNT(DISTINCT name) AS orders,
   COALESCE(SUM(items_ordered), 0) AS total_items_ordered,
   COALESCE(SUM(items_returned), 0) AS total_items_returned,
   COALESCE(SUM(items_ordered) - SUM(items_returned), 0) AS total_items_kept,
   COALESCE((ROUND(SUM(value_ordered)::decimal, 2))::decimal, 0) AS total_value_ordered,
   COALESCE((ROUND(SUM(value_returned)::decimal, 2))::decimal, 0) AS total_value_returned,
   COALESCE((ROUND((SUM(value_ordered) - SUM(value_returned))::decimal, 2))::decimal, 0) AS total_value_kept,
   COALESCE((ROUND((SUM(value_ordered)/NULLIF(COUNT(DISTINCT name), 0))::decimal, 2))::decimal, 0) AS gross_AOV,
   COALESCE((ROUND(((SUM(value_ordered) - SUM(value_returned))/NULLIF(COUNT(DISTINCT CASE WHEN cancelled_at IS NULL THEN name ELSE NULL END), 0))::decimal, 2))::decimal, 0) AS net_ATV,
   COALESCE((ROUND((SUM(items_ordered)/NULLIF(COUNT(DISTINCT name), 0))::decimal, 2))::decimal, 0) AS gross_UPT,
   COALESCE((ROUND(((SUM(items_ordered) - SUM(items_returned))/NULLIF(COUNT(DISTINCT CASE WHEN cancelled_at IS NULL THEN name ELSE NULL END), 0))::decimal, 2))::decimal, 0) AS net_UPT,
   COALESCE((ROUND((SUM(value_ordered)/NULLIF(SUM(items_ordered), 0))::decimal, 2))::decimal, 0) AS gross_ASP,
   COALESCE((ROUND(((SUM(value_ordered) - SUM(value_returned))/NULLIF((SUM(items_ordered) - SUM(items_returned)), 0))::decimal, 2))::decimal, 0) AS net_ASP,
   COALESCE((ROUND(((SUM(items_ordered) - SUM(items_returned))/NULLIF(SUM(items_ordered), 0))::decimal, 2))::decimal, 0) AS purchase_rate_items,
   COALESCE((ROUND(((SUM(value_ordered) - SUM(value_returned))/NULLIF(SUM(value_ordered), 0))::decimal, 2))::decimal, 0) AS purchase_rate_value,
   COALESCE((ROUND((SUM(items_returned)/NULLIF(SUM(items_ordered), 0))::decimal, 2))::decimal, 0) AS return_rate_items,
   COALESCE((ROUND((SUM(value_returned)/NULLIF(SUM(value_ordered), 0))::decimal, 2))::decimal, 0) AS return_rate_value,
   ROUND((SUM(keep)::numeric/NULLIF(COUNT(DISTINCT name), 0)),2) AS keep_rate
FROM london_data
WHERE publication_name IN ('Online Store','Harper Concierge')
AND analysis_region IS NOT NULL
GROUP BY
   partner__name,
   partner__reference,
   TO_CHAR(created_at, 'YYYY-MM-dd'),
   TO_CHAR(created_at, 'YYYY-MM-01'),
   channel,
   harper__product,
   analysis_region;
