{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__shopify_partner_monthly_summary CASCADE;
{% endif %}

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__shopify_partner_monthly_summary AS
    WITH
orders AS (
	SELECT
        CASE
            WHEN order_type = 'harper_try' then 'harper_try'
            WHEN (order_type  != 'harper_try' AND co.order_name IS NOT NULL) THEN 'harper_concierge'
            ELSE NULL
        END AS harper__product,
        po.name,
        po.created_at,
        po.partner__name,
        po.partner__reference,
        po.items_ordered,
        po.items_returned,
        po.value_ordered,
        po.value_returned,
        po.cancelled_at,
        CASE
            WHEN tags LIKE '%%harper%%' OR payment_gateway_names LIKE '%%Harper Payments%%' OR co.order_name IS NOT NULL THEN 'Harper'
            WHEN source_name = 'web' THEN 'Web'
        END AS source,
        CASE WHEN ((items_ordered::bigint) - (items_returned::bigint)) >= 1 THEN 1 ELSE 0 END AS keep
    FROM
        {{ schema }}.shopify_partner_orders po
    LEFT JOIN {{ schema }}.clean__order__summary co
    ON po.name = co.order_name
            )

SELECT
    ROW_NUMBER() OVER (ORDER BY TO_CHAR(created_at, 'YYYY-MM'), partner__name) as id,
    TO_CHAR(created_at, 'YYYY-MM') AS month,
    partner__name,
    source,
    harper__product,
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
FROM
    orders
WHERE source IN ('Web','Harper')
GROUP BY
    partner__name,
    partner__reference,
    TO_CHAR(created_at, 'YYYY-MM'),
    source,
    harper__product
    ;

{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__shopify_partner_monthly_summary_id_idx ON {{ schema }}.rep__shopify_partner_monthly_summary(id);

CREATE INDEX IF NOT EXISTS rep__shopify_partner_monthly_summary_month_idx ON {{ schema }}.rep__shopify_partner_monthly_summary(month);
CREATE INDEX IF NOT EXISTS rep__shopify_partner_monthly_summary_partner_idx ON {{ schema }}.rep__shopify_partner_monthly_summary(partner__name);
CREATE INDEX IF NOT EXISTS rep__shopify_partner_monthly_summary_source_idx ON {{ schema }}.rep__shopify_partner_monthly_summary(source);

{% endif %}

REFRESH MATERIALIZED VIEW {{ schema }}.rep__shopify_partner_monthly_summary;
