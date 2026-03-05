-- 01_load_hubs.sql  —  Hub ETL procedure (insert-only, business key dedup)

CREATE OR REPLACE PROCEDURE dv.load_hubs()
LANGUAGE plpgsql AS $$
DECLARE v_rows INTEGER;
BEGIN
    RAISE NOTICE '>>> Loading Hubs...';

    INSERT INTO dv.hub_customer (hub_customer_hk, customer_bk, load_date, record_source)
    SELECT MD5(UPPER(TRIM(CAST(customer_id AS TEXT)))),
           CAST(customer_id AS TEXT), created_at, 'src.customers'
    FROM src.customers
    WHERE NOT EXISTS (
        SELECT 1 FROM dv.hub_customer h WHERE h.customer_bk = CAST(src.customers.customer_id AS TEXT)
    );
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE '    hub_customer: % rows inserted', v_rows;

    INSERT INTO dv.hub_product (hub_product_hk, product_bk, load_date, record_source)
    SELECT MD5(UPPER(TRIM(product_code))), product_code, created_at, 'src.products'
    FROM src.products
    WHERE NOT EXISTS (
        SELECT 1 FROM dv.hub_product h WHERE h.product_bk = src.products.product_code
    );
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE '    hub_product: % rows inserted', v_rows;

    INSERT INTO dv.hub_order (hub_order_hk, order_bk, load_date, record_source)
    SELECT MD5(UPPER(TRIM(CAST(order_id AS TEXT)))),
           CAST(order_id AS TEXT), created_at, 'src.orders'
    FROM src.orders
    WHERE NOT EXISTS (
        SELECT 1 FROM dv.hub_order h WHERE h.order_bk = CAST(src.orders.order_id AS TEXT)
    );
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE '    hub_order: % rows inserted', v_rows;

    RAISE NOTICE '>>> Hubs done.';
END;
$$;
