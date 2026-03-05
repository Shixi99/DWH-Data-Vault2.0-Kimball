-- 03_close_versions.sql
-- Backfill load_end_date using LEAD() window function.
-- Permitted by DV2.0 spec — load_end_date is a derived temporal column.

CREATE OR REPLACE PROCEDURE dv.close_satellite_versions()
LANGUAGE plpgsql AS $$
DECLARE v_rows INTEGER;
BEGIN
    RAISE NOTICE '>>> Closing satellite versions...';

    UPDATE dv.sat_customer_details curr
    SET load_end_date = n.next_load_date
    FROM (SELECT hub_customer_hk, load_date,
                 LEAD(load_date) OVER (PARTITION BY hub_customer_hk ORDER BY load_date) AS next_load_date
          FROM dv.sat_customer_details) n
    WHERE curr.hub_customer_hk = n.hub_customer_hk
      AND curr.load_date = n.load_date AND n.next_load_date IS NOT NULL;
    GET DIAGNOSTICS v_rows = ROW_COUNT; RAISE NOTICE '    sat_customer_details: % closed', v_rows;

    UPDATE dv.sat_product_details curr
    SET load_end_date = n.next_load_date
    FROM (SELECT hub_product_hk, load_date,
                 LEAD(load_date) OVER (PARTITION BY hub_product_hk ORDER BY load_date) AS next_load_date
          FROM dv.sat_product_details) n
    WHERE curr.hub_product_hk = n.hub_product_hk
      AND curr.load_date = n.load_date AND n.next_load_date IS NOT NULL;
    GET DIAGNOSTICS v_rows = ROW_COUNT; RAISE NOTICE '    sat_product_details: % closed', v_rows;

    UPDATE dv.sat_order_details curr
    SET load_end_date = n.next_load_date
    FROM (SELECT hub_order_hk, load_date,
                 LEAD(load_date) OVER (PARTITION BY hub_order_hk ORDER BY load_date) AS next_load_date
          FROM dv.sat_order_details) n
    WHERE curr.hub_order_hk = n.hub_order_hk
      AND curr.load_date = n.load_date AND n.next_load_date IS NOT NULL;
    GET DIAGNOSTICS v_rows = ROW_COUNT; RAISE NOTICE '    sat_order_details: % closed', v_rows;

    RAISE NOTICE '>>> Close versions done.';
END;
$$;
