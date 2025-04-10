DROP VIEW IF EXISTS {{ schema }}.clean__order__summary CASCADE;
CREATE VIEW {{ schema }}.clean__order__summary AS
    SELECT
        o.*,
        --ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.createdat) AS customer_order_seq,
        CASE WHEN
            ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.createdat) = 1 THEN 1
            ELSE 0 END AS new_harper_customer, --This will only note the first order recorded. Check linked orders, could be issue.
        CASE WHEN o.order_type IN ('harper_try') THEN
            'harper_try'
        ELSE
            'harper_concierge'
        END AS harper_product_type,
        CASE
            WHEN  (order_cancelled_status IN ('Cancelled post shipment','Cancelled - no email triggered','Cancelled pre shipment') OR order_status = 'Cancelled') THEN 'Cancelled'
            WHEN order_status IN ('completed', 'returned', 'unpurchased_processed', 'return_prepared','return_required') THEN 'Happened' --return required
            WHEN order_status = 'failed' THEN 'Failed'
            ELSE NULL
        END AS happened,
        CASE
        WHEN clean__ois.num_purchased > 0 THEN 1 ELSE 0
        END AS success,
        get_halo_url(o.id, o.order_type) AS halo_link,
        get_stripe_customer_url(c.stripe_customer_id) AS stripe_customer_link,
        get_locate2u_url(o.appointment__locate2u_stop_id) AS locate2u_link,
        {{ clean__order__item__summary_columns | prefix_columns('clean__ois', 'itemsummary', exclude_columns=['order_id']) }},
        {{ clean__order__status_events_columns | prefix_columns('clean__ose', 'orderstatusevent', exclude_columns=['order_id']) }},
        -- Date fields
        o.createdat::date as createdat__dim_date,
        o.appointment__date::date as appointment__date__dim_date,
        o.trial_period_actually_ended_at::date as tp_actually_ended__dim_date,
        o.try_commission_chargeable_at::date as try_chargeable_at__dim_date,
        to_char(o.try_commission_chargeable_at::date, 'YYYYIW')::int as try_chargeable_at__dim_yearcalendarweek_sc,
        to_char(o.try_commission_chargeable_at::date, 'YYYYMM')::int as try_chargeable_at__dim_yearmonth_sc
    FROM
        {{ schema }}.orders o
    LEFT JOIN
        clean__order__item__summary clean__ois ON clean__ois.order_id = o.id
    LEFT JOIN
        clean__order__status_events clean__ose ON clean__ose.order_id = o.id
    LEFT JOIN customer c ON c.id = o.customer_id
    WHERE o.brand_name IS NOT NULL
    AND o.brand_name NOT IN ('ME+EM UAT', 'Harper UAT Shopify','Harper Production','Harper-concierge-demo','',' ')
    AND o.order_name IS NOT NULL
    AND o.order_name NOT IN ('',' ','  ',' -L1')
    -- DO NOT DO LIKE comparisons without checking for NULL
    AND (c.first_name IS NULL OR lower(c.first_name) NOT LIKE '%%test%%')
    ;
