-- 02_load_satellites.sql
-- Satellite ETL: incremental insert via hash_diff + soft delete detection

CREATE OR REPLACE PROCEDURE dv.load_satellites()
LANGUAGE plpgsql AS $$
DECLARE v_rows INTEGER;
BEGIN
    RAISE NOTICE '>>> Loading Satellites...';

    -- ── sat_customer_details ──────────────────────────────────────
    INSERT INTO dv.sat_customer_details
        (hub_customer_hk, load_date, load_end_date, record_source,
         hash_diff, full_name, email, phone, country, tier)
    SELECT MD5(UPPER(TRIM(CAST(c.customer_id AS TEXT)))),
           c.updated_at, NULL, 'src.customers',
           MD5(CONCAT_WS('|', COALESCE(c.full_name,''), COALESCE(c.email,''),
                              COALESCE(c.phone,''), COALESCE(c.country,''), COALESCE(c.tier,''))),
           c.full_name, c.email, c.phone, c.country, c.tier
    FROM src.customers c
    WHERE NOT EXISTS (
        SELECT 1 FROM dv.sat_customer_details s
        WHERE s.hub_customer_hk = MD5(UPPER(TRIM(CAST(c.customer_id AS TEXT))))
          AND s.hash_diff = MD5(CONCAT_WS('|', COALESCE(c.full_name,''), COALESCE(c.email,''),
                                               COALESCE(c.phone,''), COALESCE(c.country,''), COALESCE(c.tier,'')))
    );
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE '    sat_customer_details: % rows inserted', v_rows;

    -- delete detection: hub key gone from source
    INSERT INTO dv.sat_customer_deleted (hub_customer_hk, load_date, load_end_date, record_source, is_deleted)
    SELECT h.hub_customer_hk, NOW(), NULL, 'src.customers#delete_detector', TRUE
    FROM dv.hub_customer h
    WHERE NOT EXISTS (SELECT 1 FROM src.customers c WHERE CAST(c.customer_id AS TEXT) = h.customer_bk)
      AND NOT EXISTS (SELECT 1 FROM dv.sat_customer_deleted d
                      WHERE d.hub_customer_hk = h.hub_customer_hk AND d.load_end_date IS NULL AND d.is_deleted);
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE '    sat_customer_deleted (new): % rows', v_rows;

    -- resurrection detection: deleted key reappears in source
    UPDATE dv.sat_customer_deleted d SET load_end_date = NOW()
    FROM src.customers c
    WHERE d.hub_customer_hk = MD5(UPPER(TRIM(CAST(c.customer_id AS TEXT))))
      AND d.load_end_date IS NULL AND d.is_deleted;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE '    sat_customer_deleted (resurrections): % rows', v_rows;

    -- ── sat_product_details ───────────────────────────────────────
    INSERT INTO dv.sat_product_details
        (hub_product_hk, load_date, load_end_date, record_source,
         hash_diff, product_name, category, unit_price, currency, is_active)
    SELECT MD5(UPPER(TRIM(p.product_code))), p.updated_at, NULL, 'src.products',
           MD5(CONCAT_WS('|', COALESCE(p.product_name,''), COALESCE(p.category,''),
                              COALESCE(CAST(p.unit_price AS TEXT),''),
                              COALESCE(p.currency,''), COALESCE(CAST(p.is_active AS TEXT),''))) ,
           p.product_name, p.category, p.unit_price, p.currency, p.is_active
    FROM src.products p
    WHERE NOT EXISTS (
        SELECT 1 FROM dv.sat_product_details s
        WHERE s.hub_product_hk = MD5(UPPER(TRIM(p.product_code)))
          AND s.hash_diff = MD5(CONCAT_WS('|', COALESCE(p.product_name,''), COALESCE(p.category,''),
                                               COALESCE(CAST(p.unit_price AS TEXT),''),
                                               COALESCE(p.currency,''), COALESCE(CAST(p.is_active AS TEXT),'')))
    );
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE '    sat_product_details: % rows inserted', v_rows;

    INSERT INTO dv.sat_product_deleted (hub_product_hk, load_date, load_end_date, record_source, is_deleted)
    SELECT h.hub_product_hk, NOW(), NULL, 'src.products#delete_detector', TRUE
    FROM dv.hub_product h
    WHERE NOT EXISTS (SELECT 1 FROM src.products p WHERE p.product_code = h.product_bk)
      AND NOT EXISTS (SELECT 1 FROM dv.sat_product_deleted d
                      WHERE d.hub_product_hk = h.hub_product_hk AND d.load_end_date IS NULL AND d.is_deleted);
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE '    sat_product_deleted (new): % rows', v_rows;

    UPDATE dv.sat_product_deleted d SET load_end_date = NOW()
    FROM src.products p
    WHERE d.hub_product_hk = MD5(UPPER(TRIM(p.product_code)))
      AND d.load_end_date IS NULL AND d.is_deleted;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE '    sat_product_deleted (resurrections): % rows', v_rows;

    -- ── sat_order_details ─────────────────────────────────────────
    INSERT INTO dv.sat_order_details
        (hub_order_hk, load_date, load_end_date, record_source,
         hash_diff, quantity, unit_price, total_amount, order_date, status)
    SELECT MD5(UPPER(TRIM(CAST(o.order_id AS TEXT)))), o.updated_at, NULL, 'src.orders',
           MD5(CONCAT_WS('|', COALESCE(CAST(o.quantity AS TEXT),''),
                              COALESCE(CAST(o.unit_price AS TEXT),''),
                              COALESCE(CAST(o.total_amount AS TEXT),''),
                              COALESCE(CAST(o.order_date AS TEXT),''),
                              COALESCE(o.status,''))),
           o.quantity, o.unit_price, o.total_amount, o.order_date, o.status
    FROM src.orders o
    WHERE NOT EXISTS (
        SELECT 1 FROM dv.sat_order_details s
        WHERE s.hub_order_hk = MD5(UPPER(TRIM(CAST(o.order_id AS TEXT))))
          AND s.hash_diff = MD5(CONCAT_WS('|', COALESCE(CAST(o.quantity AS TEXT),''),
                                               COALESCE(CAST(o.unit_price AS TEXT),''),
                                               COALESCE(CAST(o.total_amount AS TEXT),''),
                                               COALESCE(CAST(o.order_date AS TEXT),''),
                                               COALESCE(o.status,'')))
    );
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE '    sat_order_details: % rows inserted', v_rows;

    INSERT INTO dv.sat_order_deleted (hub_order_hk, load_date, load_end_date, record_source, is_deleted)
    SELECT h.hub_order_hk, NOW(), NULL, 'src.orders#delete_detector', TRUE
    FROM dv.hub_order h
    WHERE NOT EXISTS (SELECT 1 FROM src.orders o WHERE CAST(o.order_id AS TEXT) = h.order_bk)
      AND NOT EXISTS (SELECT 1 FROM dv.sat_order_deleted d
                      WHERE d.hub_order_hk = h.hub_order_hk AND d.load_end_date IS NULL AND d.is_deleted);
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE '    sat_order_deleted (new): % rows', v_rows;

    UPDATE dv.sat_order_deleted d SET load_end_date = NOW()
    FROM src.orders o
    WHERE d.hub_order_hk = MD5(UPPER(TRIM(CAST(o.order_id AS TEXT))))
      AND d.load_end_date IS NULL AND d.is_deleted;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE '    sat_order_deleted (resurrections): % rows', v_rows;

    RAISE NOTICE '>>> Satellites done.';
END;
$$;
