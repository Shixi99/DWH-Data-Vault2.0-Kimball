-- 04_load_links.sql  —  Link ETL (insert-only, composite hash key dedup)

CREATE OR REPLACE PROCEDURE dv.load_links()
LANGUAGE plpgsql AS $$
DECLARE v_rows INTEGER;
BEGIN
    RAISE NOTICE '>>> Loading Links...';

    INSERT INTO dv.lnk_order_customer_product
        (lnk_order_hk, hub_order_hk, hub_customer_hk, hub_product_hk, load_date, record_source)
    SELECT
        MD5(CONCAT_WS('||',
            MD5(UPPER(TRIM(CAST(o.order_id AS TEXT)))),
            MD5(UPPER(TRIM(CAST(o.customer_id AS TEXT)))),
            MD5(UPPER(TRIM(p.product_code))))),
        MD5(UPPER(TRIM(CAST(o.order_id AS TEXT)))),
        MD5(UPPER(TRIM(CAST(o.customer_id AS TEXT)))),
        MD5(UPPER(TRIM(p.product_code))),
        o.created_at, 'src.orders'
    FROM src.orders o JOIN src.products p ON p.product_id = o.product_id
    WHERE NOT EXISTS (
        SELECT 1 FROM dv.lnk_order_customer_product l
        WHERE l.lnk_order_hk = MD5(CONCAT_WS('||',
            MD5(UPPER(TRIM(CAST(o.order_id AS TEXT)))),
            MD5(UPPER(TRIM(CAST(o.customer_id AS TEXT)))),
            MD5(UPPER(TRIM(p.product_code)))))
    );
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE '    lnk_order_customer_product: % rows inserted', v_rows;

    RAISE NOTICE '>>> Links done.';
END;
$$;
