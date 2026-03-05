-- 05_rebuild_pits.sql
-- PIT rebuild using LATERAL + pre-computed upper_bound (index-safe, no cast on load_date)

CREATE OR REPLACE PROCEDURE dv.rebuild_pits()
LANGUAGE plpgsql AS $$
DECLARE v_rows INTEGER;
BEGIN
    RAISE NOTICE '>>> Rebuilding PIT tables...';

    -- ── pit_customer ──────────────────────────────────────────────
    TRUNCATE TABLE dv.pit_customer;
    INSERT INTO dv.pit_customer (hub_customer_hk, snapshot_date, sat_customer_details_ldts, sat_customer_deleted_ldts)
    WITH snapshot_dates AS (
        SELECT generate_series(
            (SELECT LEAST((SELECT MIN(load_date) FROM dv.sat_customer_details),
                          (SELECT MIN(load_date) FROM dv.sat_customer_deleted)))::date,
            (SELECT GREATEST((SELECT MAX(load_date) FROM dv.sat_customer_details),
                             (SELECT MAX(load_date) FROM dv.sat_customer_deleted)))::date,
            '1 day'::interval)::date AS snapshot_date
    ),
    hxs AS (
        SELECT h.hub_customer_hk, s.snapshot_date, (s.snapshot_date + 1)::timestamp AS upper_bound
        FROM dv.hub_customer h CROSS JOIN snapshot_dates s
    )
    SELECT hxs.hub_customer_hk, hxs.snapshot_date, det.load_date, del.load_date
    FROM hxs
    LEFT JOIN LATERAL (SELECT sd.load_date FROM dv.sat_customer_details sd
                       WHERE sd.hub_customer_hk = hxs.hub_customer_hk AND sd.load_date < hxs.upper_bound
                       ORDER BY sd.load_date DESC LIMIT 1) det ON true
    LEFT JOIN LATERAL (SELECT sd.load_date FROM dv.sat_customer_deleted sd
                       WHERE sd.hub_customer_hk = hxs.hub_customer_hk AND sd.load_date < hxs.upper_bound
                       ORDER BY sd.load_date DESC LIMIT 1) del ON true;
    GET DIAGNOSTICS v_rows = ROW_COUNT; RAISE NOTICE '    pit_customer: % rows', v_rows;

    -- ── pit_product ───────────────────────────────────────────────
    TRUNCATE TABLE dv.pit_product;
    INSERT INTO dv.pit_product (hub_product_hk, snapshot_date, sat_product_details_ldts, sat_product_deleted_ldts)
    WITH snapshot_dates AS (
        SELECT generate_series(
            (SELECT LEAST((SELECT MIN(load_date) FROM dv.sat_product_details),
                          (SELECT MIN(load_date) FROM dv.sat_product_deleted)))::date,
            (SELECT GREATEST((SELECT MAX(load_date) FROM dv.sat_product_details),
                             (SELECT MAX(load_date) FROM dv.sat_product_deleted)))::date,
            '1 day'::interval)::date AS snapshot_date
    ),
    hxs AS (
        SELECT h.hub_product_hk, s.snapshot_date, (s.snapshot_date + 1)::timestamp AS upper_bound
        FROM dv.hub_product h CROSS JOIN snapshot_dates s
    )
    SELECT hxs.hub_product_hk, hxs.snapshot_date, det.load_date, del.load_date
    FROM hxs
    LEFT JOIN LATERAL (SELECT sd.load_date FROM dv.sat_product_details sd
                       WHERE sd.hub_product_hk = hxs.hub_product_hk AND sd.load_date < hxs.upper_bound
                       ORDER BY sd.load_date DESC LIMIT 1) det ON true
    LEFT JOIN LATERAL (SELECT sd.load_date FROM dv.sat_product_deleted sd
                       WHERE sd.hub_product_hk = hxs.hub_product_hk AND sd.load_date < hxs.upper_bound
                       ORDER BY sd.load_date DESC LIMIT 1) del ON true;
    GET DIAGNOSTICS v_rows = ROW_COUNT; RAISE NOTICE '    pit_product: % rows', v_rows;

    -- ── pit_order ─────────────────────────────────────────────────
    TRUNCATE TABLE dv.pit_order;
    INSERT INTO dv.pit_order (hub_order_hk, snapshot_date, sat_order_details_ldts, sat_order_deleted_ldts)
    WITH snapshot_dates AS (
        SELECT generate_series(
            (SELECT LEAST((SELECT MIN(load_date) FROM dv.sat_order_details),
                          (SELECT MIN(load_date) FROM dv.sat_order_deleted)))::date,
            (SELECT GREATEST((SELECT MAX(load_date) FROM dv.sat_order_details),
                             (SELECT MAX(load_date) FROM dv.sat_order_deleted)))::date,
            '1 day'::interval)::date AS snapshot_date
    ),
    hxs AS (
        SELECT h.hub_order_hk, s.snapshot_date, (s.snapshot_date + 1)::timestamp AS upper_bound
        FROM dv.hub_order h CROSS JOIN snapshot_dates s
    )
    SELECT hxs.hub_order_hk, hxs.snapshot_date, det.load_date, del.load_date
    FROM hxs
    LEFT JOIN LATERAL (SELECT sd.load_date FROM dv.sat_order_details sd
                       WHERE sd.hub_order_hk = hxs.hub_order_hk AND sd.load_date < hxs.upper_bound
                       ORDER BY sd.load_date DESC LIMIT 1) det ON true
    LEFT JOIN LATERAL (SELECT sd.load_date FROM dv.sat_order_deleted sd
                       WHERE sd.hub_order_hk = hxs.hub_order_hk AND sd.load_date < hxs.upper_bound
                       ORDER BY sd.load_date DESC LIMIT 1) del ON true;
    GET DIAGNOSTICS v_rows = ROW_COUNT; RAISE NOTICE '    pit_order: % rows', v_rows;

    RAISE NOTICE '>>> PIT rebuild done.';
END;
$$;
