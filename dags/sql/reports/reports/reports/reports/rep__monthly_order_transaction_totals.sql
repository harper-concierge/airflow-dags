{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__monthly_order_transaction_totals CASCADE;
{% endif %}
CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__monthly_order_transaction_totals AS
WITH try_orders AS (
    SELECT
        trial_period_ended_at__dim_yearmonth_sc AS metric_date,
        partner_order_name,
        harper_product_type,
        sum(
            CASE
                WHEN lineitem_category IN ('item_discount', 'order_discount', 'harper_item_discount') THEN lineitem_amount
                ELSE 0
            END
        )::integer AS total_discount,
        sum(
            CASE
                WHEN lineitem_category IN ('item_discount', 'order_discount', 'harper_item_discount') THEN 0
                ELSE lineitem_amount
            END
        )::integer AS total_value,
        SUM(
            CASE
                WHEN lineitem_category IN ('item_discount', 'order_discount', 'harper_item_discount') THEN -lineitem_amount
                ELSE lineitem_amount
            END
            * (COALESCE(NULLIF(commission_percentage, 0), 2.4) / 100)
        )::integer AS total_revenue,
        STRING_AGG(DISTINCT calculated_discount_code, ', ') AS discount_codes,
        STRING_AGG(DISTINCT partner_name, ', ') AS partner_name,
        COUNT(DISTINCT CASE
            WHEN lineitem_category != 'fee' THEN lineitem_name
        END)::integer AS total_items,
        COUNT(DISTINCT CASE
            WHEN lineitem_category != 'fee' AND lineitem_type = 'purchase' THEN lineitem_name
        END)::integer AS purchased_items,
        COUNT(DISTINCT CASE
            WHEN lineitem_category != 'fee' AND lineitem_type = 'refund' THEN lineitem_name
        END)::integer AS refunded_items,
        MAX(commission_percentage) as commission_percentage
    FROM
        {{ schema }}.rep__transactionlog__view t
    WHERE
        lineitem_type = 'try_on'
        AND harper_product_type = 'harper_try'
    GROUP BY
        trial_period_ended_at__dim_yearmonth_sc,
        partner_order_name,
        harper_product_type
),

concierge_orders AS (
    SELECT
        trial_period_ended_at__dim_yearmonth_sc AS metric_date,
        partner_order_name,
        harper_product_type,
        sum(
            CASE
                WHEN lineitem_category IN ('item_discount', 'order_discount', 'harper_item_discount') THEN lineitem_amount
                ELSE 0
            END
        )::integer AS total_discount,
        sum(
            CASE
                WHEN lineitem_category IN ('item_discount', 'order_discount', 'harper_item_discount') THEN 0
                ELSE lineitem_amount
            END
        )::integer AS total_value,
        SUM(
            CASE
                WHEN lineitem_category = 'fee' THEN
                    CASE
                        WHEN revenue_is_service_fee_inclusive=1 THEN lineitem_amount
                        ELSE 0
                    END
                WHEN lineitem_category IN ('item_discount', 'order_discount', 'harper_item_discount') THEN
                    CASE
                        WHEN lineitem_type = 'purchase' THEN -lineitem_amount
                        WHEN lineitem_type = 'refund'   THEN lineitem_amount
                        ELSE lineitem_amount
                    END
                ELSE
                    CASE
                        WHEN lineitem_type = 'purchase' THEN lineitem_amount
                        WHEN lineitem_type = 'refund'   THEN -lineitem_amount
                        ELSE lineitem_amount
                    END
            END
            / CASE
                WHEN commission_is_vat_inclusive=1 THEN 1
                ELSE 1.2
            END
            * (commission_percentage / 100)
        )::integer AS total_revenue,
        STRING_AGG(DISTINCT calculated_discount_code, ', ') AS discount_codes,
        STRING_AGG(DISTINCT partner_name, ', ') AS partner_name,
        COUNT(DISTINCT CASE
            WHEN lineitem_category != 'fee' THEN lineitem_name
        END)::integer AS total_items,
        COUNT(DISTINCT CASE
            WHEN lineitem_category != 'fee' AND lineitem_type = 'purchase' THEN lineitem_name
        END)::integer AS purchased_items,
        COUNT(DISTINCT CASE
            WHEN lineitem_category != 'fee' AND lineitem_type = 'refund' THEN lineitem_name
        END)::integer AS refunded_items,
        MAX(commission_percentage) as commission_percentage
    FROM
        {{ schema }}.rep__transactionlog__view t
    WHERE
        lineitem_type <> 'try_on'
        AND harper_product_type = 'harper_concierge'
    GROUP BY
        trial_period_ended_at__dim_yearmonth_sc,
        partner_order_name,
        harper_product_type
)

SELECT
    metric_date AS trial_period_ended_at__dim_yearmonth_sc,
    partner_order_name,
    harper_product_type AS product_type,
    partner_name,
    discount_codes,
    total_discount,
    total_value,
    total_revenue,
    total_items,
    purchased_items,
    refunded_items,
    commission_percentage

FROM (
    SELECT * FROM try_orders
    UNION ALL
    SELECT * FROM concierge_orders
) combined_orders

ORDER BY
    metric_date DESC,
    partner_order_name ASC

WITH NO DATA;

{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__monthly_order_transaction_totals_idx ON {{ schema }}.rep__monthly_order_transaction_totals (dim_yearmonth_sc, partner_order_name, product_type);
{% endif %}

REFRESH MATERIALIZED VIEW {{ schema }}.rep__monthly_order_transaction_totals;
