-- 01_refresh_gold.sql  —  Gold layer: all views + refresh procedure

-- Recreates all Gold layer views so any structural changes
-- to satellites are immediately reflected in the mart.
-- ============================================================
CREATE OR REPLACE PROCEDURE gold.refresh_gold()
LANGUAGE plpgsql AS $$
DECLARE
    v_start TIMESTAMP := clock_timestamp();
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'GOLD REFRESH STARTED: %', v_start;
    RAISE NOTICE '========================================';

    -- --------------------------------------------------------
    -- dim_customer
    -- --------------------------------------------------------
    RAISE NOTICE '>>> Refreshing dim_customer...';
    CREATE OR REPLACE VIEW gold.dim_customer AS
    WITH latest_snapshot AS (
        -- Latest snapshot per customer covering both satellites
        SELECT DISTINCT ON (hub_customer_hk)
            hub_customer_hk,
            snapshot_date,
            sat_customer_details_ldts,
            sat_customer_deleted_ldts
        FROM dv.pit_customer
        WHERE sat_customer_details_ldts IS NOT NULL
        ORDER BY hub_customer_hk, snapshot_date DESC
    )
    SELECT
        h.hub_customer_hk                               AS customer_key,
        h.customer_bk                                   AS customer_id,
        s.full_name,
        s.email,
        s.phone,
        s.country,
        s.tier,
        ls.snapshot_date                                AS valid_as_of,
        -- is_deleted: TRUE if deletion satellite has an open record
        -- (load_end_date IS NULL means currently deleted)
        CASE
            WHEN d.is_deleted IS TRUE
             AND d.load_end_date IS NULL THEN TRUE
            ELSE FALSE
        END                                             AS is_deleted,
        -- When was this customer deleted? NULL if active or resurrected
        CASE
            WHEN d.is_deleted IS TRUE
             AND d.load_end_date IS NULL THEN d.load_date
            ELSE NULL
        END                                             AS deleted_at
    FROM latest_snapshot                ls
    JOIN dv.hub_customer                h   ON h.hub_customer_hk  = ls.hub_customer_hk
    JOIN dv.sat_customer_details        s   ON s.hub_customer_hk  = ls.hub_customer_hk
                                           AND s.load_date        = ls.sat_customer_details_ldts
    -- LEFT JOIN: if no deletion record exists, customer is active
    LEFT JOIN dv.sat_customer_deleted   d   ON d.hub_customer_hk  = ls.hub_customer_hk
                                           AND d.load_date        = ls.sat_customer_deleted_ldts;

    RAISE NOTICE '    dim_customer refreshed.';

    -- --------------------------------------------------------
    -- dim_product
    -- --------------------------------------------------------
    RAISE NOTICE '>>> Refreshing dim_product...';
    CREATE OR REPLACE VIEW gold.dim_product AS
    WITH latest_snapshot AS (
        SELECT DISTINCT ON (hub_product_hk)
            hub_product_hk,
            snapshot_date,
            sat_product_details_ldts,
            sat_product_deleted_ldts
        FROM dv.pit_product
        WHERE sat_product_details_ldts IS NOT NULL
        ORDER BY hub_product_hk, snapshot_date DESC
    )
    SELECT
        h.hub_product_hk                                AS product_key,
        h.product_bk                                    AS product_code,
        s.product_name,
        s.category,
        s.unit_price,
        s.currency,
        s.is_active,
        CASE
            WHEN d.is_deleted IS TRUE
             AND d.load_end_date IS NULL THEN TRUE
            ELSE FALSE
        END                                             AS is_deleted,
        CASE
            WHEN d.is_deleted IS TRUE
             AND d.load_end_date IS NULL THEN d.load_date
            ELSE NULL
        END                                             AS deleted_at
    FROM latest_snapshot                ls
    JOIN dv.hub_product                 h  ON h.hub_product_hk  = ls.hub_product_hk
    JOIN dv.sat_product_details         s  ON s.hub_product_hk  = ls.hub_product_hk
                                          AND s.load_date       = ls.sat_product_details_ldts
    LEFT JOIN dv.sat_product_deleted    d  ON d.hub_product_hk  = ls.hub_product_hk
                                          AND d.load_date       = ls.sat_product_deleted_ldts;

    RAISE NOTICE '    dim_product refreshed.';

    -- --------------------------------------------------------
    -- fact_orders
    -- --------------------------------------------------------
    RAISE NOTICE '>>> Refreshing fact_orders...';
    CREATE OR REPLACE VIEW gold.fact_orders AS
    WITH latest_snapshot AS (
        SELECT DISTINCT ON (hub_order_hk)
            hub_order_hk,
            snapshot_date,
            sat_order_details_ldts,
            sat_order_deleted_ldts
        FROM dv.pit_order
        WHERE sat_order_details_ldts IS NOT NULL
        ORDER BY hub_order_hk, snapshot_date DESC
    )
    SELECT
        lnk.lnk_order_hk                               AS order_key,
        lnk.hub_customer_hk                             AS customer_key,
        lnk.hub_product_hk                              AS product_key,
        ho.order_bk                                     AS order_id,
        so.order_date,
        so.quantity,
        so.unit_price,
        so.total_amount,
        so.status,
        lnk.load_date                                   AS order_loaded_at,
        CASE
            WHEN d.is_deleted IS TRUE
             AND d.load_end_date IS NULL THEN TRUE
            ELSE FALSE
        END                                             AS is_deleted,
        CASE
            WHEN d.is_deleted IS TRUE
             AND d.load_end_date IS NULL THEN d.load_date
            ELSE NULL
        END                                             AS deleted_at
    FROM dv.lnk_order_customer_product     lnk
    JOIN dv.hub_order                       ho   ON ho.hub_order_hk   = lnk.hub_order_hk
    JOIN latest_snapshot                    ls   ON ls.hub_order_hk   = lnk.hub_order_hk
    JOIN dv.sat_order_details               so   ON so.hub_order_hk   = lnk.hub_order_hk
                                                AND so.load_date      = ls.sat_order_details_ldts
    LEFT JOIN dv.sat_order_deleted          d    ON d.hub_order_hk    = lnk.hub_order_hk
                                                AND d.load_date       = ls.sat_order_deleted_ldts;
    RAISE NOTICE '    fact_orders refreshed.';

    -- --------------------------------------------------------
    -- fact_order_status_timeline
    -- --------------------------------------------------------
    RAISE NOTICE '>>> Refreshing fact_order_status_timeline...';
    CREATE OR REPLACE VIEW gold.fact_order_status_timeline AS
    SELECT
        ho.order_bk                         AS order_id,
        lnk.hub_customer_hk                 AS customer_key,
        lnk.hub_product_hk                  AS product_key,
        s.status,
        s.load_date                         AS status_start,
        COALESCE(s.load_end_date, NOW())    AS status_end,
        CASE
            WHEN s.load_end_date IS NOT NULL THEN
                ROUND(EXTRACT(EPOCH FROM s.load_end_date - s.load_date) / 3600.0, 2)
            WHEN s.status IN ('delivered', 'cancelled') THEN
                ROUND(EXTRACT(EPOCH FROM NOW() - s.load_date) / 3600.0, 2)
            ELSE NULL  -- in_progress: no misleading duration
        END                                 AS duration_hours,
        CASE
            WHEN s.load_end_date IS NOT NULL          THEN 'completed'
            WHEN s.status IN ('delivered','cancelled') THEN 'final'
            ELSE 'in_progress'
        END                                 AS status_state,
        ROW_NUMBER() OVER (
            PARTITION BY s.hub_order_hk
            ORDER BY s.load_date
        )                                   AS status_step_number
    FROM       dv.sat_order_details              s
    JOIN       dv.hub_order                      ho  ON ho.hub_order_hk  = s.hub_order_hk
    LEFT JOIN  dv.lnk_order_customer_product     lnk ON lnk.hub_order_hk = s.hub_order_hk;
    RAISE NOTICE '    fact_order_status_timeline refreshed.';

    -- --------------------------------------------------------
    -- rpt_revenue_by_tier_and_category
    -- --------------------------------------------------------
    RAISE NOTICE '>>> Refreshing rpt_revenue_by_tier_and_category...';
    CREATE OR REPLACE VIEW gold.rpt_revenue_by_tier_and_category AS
    SELECT
        c.tier                          AS customer_tier,
        p.category                      AS product_category,
        p.product_name,
        COUNT(f.order_id)               AS total_orders,
        SUM(f.quantity)                 AS total_units_sold,
        ROUND(SUM(f.total_amount), 2)   AS total_revenue_usd,
        ROUND(AVG(f.total_amount), 2)   AS avg_order_value
    FROM gold.fact_orders    f
    JOIN gold.dim_customer   c ON c.customer_key = f.customer_key
    JOIN gold.dim_product    p ON p.product_key  = f.product_key
    WHERE f.status != 'cancelled'
    GROUP BY c.tier, p.category, p.product_name
    ORDER BY total_revenue_usd DESC;
    RAISE NOTICE '    rpt_revenue_by_tier_and_category refreshed.';

    -- --------------------------------------------------------
    -- rpt_order_status_durations
    -- --------------------------------------------------------
    RAISE NOTICE '>>> Refreshing rpt_order_status_durations...';
    CREATE OR REPLACE VIEW gold.rpt_order_status_durations AS
    SELECT
        f.status,
        f.status_state,
        COUNT(DISTINCT f.order_id)                              AS order_count,
        ROUND(AVG(f.duration_hours), 2)                         AS avg_hours_in_status,
        ROUND(MIN(f.duration_hours), 2)                         AS min_hours,
        ROUND(MAX(f.duration_hours), 2)                         AS max_hours,
        COUNT(*) FILTER (WHERE f.status_state = 'in_progress')  AS currently_in_progress,
        COUNT(*) FILTER (WHERE f.status_state = 'final')        AS reached_final,
        COUNT(*) FILTER (WHERE f.status_state = 'completed')    AS transitioned_out
    FROM gold.fact_order_status_timeline f
    WHERE f.duration_hours IS NOT NULL
    GROUP BY f.status, f.status_state
    ORDER BY f.status, f.status_state;
    RAISE NOTICE '    rpt_order_status_durations refreshed.';

    RAISE NOTICE '>>> Refreshing rpt_customer_tier_history...';
    CREATE OR REPLACE VIEW gold.rpt_customer_tier_history AS
    WITH
    tier_rank AS (
        SELECT
            unnest(ARRAY['bronze','silver','gold','platinum']) AS tier,
            generate_subscripts(ARRAY['bronze','silver','gold','platinum'], 1) AS tier_rank
    ),
    tier_changes AS (
        SELECT
            h.customer_bk                                       AS customer_id,
            s.full_name,
            s.tier,
            s.load_date                                         AS tier_start,
            s.load_end_date                                     AS tier_end,
            ROUND(
                EXTRACT(EPOCH FROM
                    COALESCE(s.load_end_date, NOW()) - s.load_date
                ) / 86400.0, 1
            )                                                   AS days_in_tier,
            ROW_NUMBER() OVER (
                PARTITION BY h.hub_customer_hk
                ORDER BY s.load_date
            )                                                   AS tier_step,
            COUNT(*) OVER (
                PARTITION BY h.hub_customer_hk
            )                                                   AS total_tier_changes,
            CASE WHEN s.load_end_date IS NULL THEN TRUE ELSE FALSE END
                                                                AS is_current_tier,
            LAG(s.tier) OVER (
                PARTITION BY h.hub_customer_hk ORDER BY s.load_date
            )                                                   AS previous_tier,
            CASE
                WHEN LAG(trk.tier_rank) OVER (
                    PARTITION BY h.hub_customer_hk ORDER BY s.load_date
                ) IS NULL THEN 'initial'
                WHEN trk.tier_rank > LAG(trk.tier_rank) OVER (
                    PARTITION BY h.hub_customer_hk ORDER BY s.load_date
                ) THEN 'upgrade'
                WHEN trk.tier_rank < LAG(trk.tier_rank) OVER (
                    PARTITION BY h.hub_customer_hk ORDER BY s.load_date
                ) THEN 'downgrade'
                ELSE 'lateral'
            END                                                 AS change_direction,
            -- pull deletion status from dim_customer
            dc.is_deleted,
            dc.deleted_at
        FROM dv.sat_customer_details  s
        JOIN dv.hub_customer          h   ON h.hub_customer_hk = s.hub_customer_hk
        JOIN tier_rank                trk ON trk.tier          = s.tier
        LEFT JOIN gold.dim_customer   dc  ON dc.customer_key   = s.hub_customer_hk
    )
    SELECT
        customer_id,
        full_name,
        tier_step,
        previous_tier,
        tier            AS current_tier,
        change_direction,
        tier_start,
        tier_end,
        days_in_tier,
        is_current_tier,
        total_tier_changes,
        is_deleted,
        deleted_at
    FROM tier_changes
    ORDER BY customer_id, tier_step;
    RAISE NOTICE '    rpt_order_status_durations refreshed.';

    RAISE NOTICE '========================================';
    RAISE NOTICE 'GOLD REFRESH FINISHED. Duration: %', clock_timestamp() - v_start;
    RAISE NOTICE '========================================';
END;
$$;

