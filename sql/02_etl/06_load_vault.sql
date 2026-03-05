-- 06_load_vault.sql  —  Master orchestration procedure

CREATE OR REPLACE PROCEDURE dv.load_vault()
LANGUAGE plpgsql AS $$
DECLARE v_start TIMESTAMP := clock_timestamp();
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'DV LOAD STARTED: %', v_start;
    RAISE NOTICE '========================================';
    CALL dv.load_hubs();
    CALL dv.load_satellites();
    CALL dv.close_satellite_versions();
    CALL dv.load_links();
    CALL dv.rebuild_pits();
    RAISE NOTICE '========================================';
    RAISE NOTICE 'DV LOAD FINISHED. Duration: %', clock_timestamp() - v_start;
    RAISE NOTICE '========================================';
END;
$$;
