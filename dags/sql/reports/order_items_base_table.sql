{% if is_modified %}
    DROP VIEW IF EXISTS {{ schema }}.base__order__items CASCADE;
{% endif %}
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_catalog.pg_class c
        JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE  n.nspname = '{{ schema }}'
        AND    c.relname = 'transaction__items'
        AND    c.relkind = 'v' -- 'v' stands for view
    ) THEN
    EXECUTE 'CREATE VIEW {{ schema }}.base__order__items AS
    -- BEGIN
    WITH orders AS (
        SELECT
            id,
            customer_id,
            createdat,
            appointment__date,
            order_name,
            original_order_name,
            brand_name,
            link_order_child,
            order_status,
            order_type,
            parent_order,
            ship_direct
        FROM public.orders
    )
    SELECT
        o.customer_id AS customer_id,
        o.order_status,
        CASE WHEN link_order_child = 'TRUE' THEN 1 ELSE 0 END AS link_order_child,
        o.createdat AS order_created,
        oi.createdat AS item_created,
        oi.updatedat,
        o.appointment__date AS appointment_date,
        o.brand_name,
        o.original_order_name,
        o.order_name,
        oi.order_name AS order_name_item,
        oi.id AS unique_id,
        oi.order_id,
        o.order_type AS order_type_initial,
        oi.order_type AS order_type,
        ROW_NUMBER() OVER(PARTITION BY o.order_name ORDER BY oi.createdat) AS order_item_index,
        ROW_NUMBER() OVER(PARTITION BY o.order_name , oi.original_name ORDER BY oi.createdat) AS product_name_index,
        oi.original_name AS product_name,
        oi.sku,
        oi.price as item_price_pence,
        oi.discount AS discount_price_pence,
        oi.price - oi.discount AS item_value_pence,
        oi.qty,
        CASE WHEN oi.purchased = TRUE THEN 1 ELSE 0 END AS purchased,
        CASE WHEN oi.returned = TRUE THEN 1 ELSE 0 END AS returned,
        CASE WHEN oi.purchased = TRUE OR oi.returned = TRUE THEN 1 ELSE 0 END AS purchased_originally,
        CASE WHEN (oi.purchased != TRUE AND oi.returned != TRUE AND o.order_status = 'completed') THEN 1 ELSE 0 END AS unpurchased,
        CASE WHEN oi.return_sent_by_customer = TRUE THEN 1 ELSE 0 END AS return_sent,
        CASE WHEN oi.received_by_warehouse = TRUE THEN 1 ELSE 0 END AS return_received,
        CASE WHEN oi.is_initiated_sale = TRUE THEN 1 ELSE 0 END AS is_initiated_sale,
        CASE WHEN oi.out_of_stock = TRUE THEN 1 ELSE 0 END AS out_of_stock,
        CASE WHEN oi.preorder = TRUE THEN 1 ELSE 0 END AS preorder,
        CASE WHEN oi.received = TRUE THEN 1 ELSE 0 END AS received,
        CASE WHEN oi.initiated_sale__user_role like '%%remote_sales%%' THEN 1 ELSE 0 END AS inspire_me_flag,
        {{ dim__time_columns | prefix_columns('cdt', 'createdat')}}
    FROM orders o
        LEFT JOIN order__items oi
        ON o.id = oi.order_id
    LEFT JOIN dim__time cdt ON o.createdat::date = cdt.dim_date_id
    WHERE o.brand_name != 'Harper Production'
    WITH NO DATA
    --END
;';
    END IF;
END
$$;
