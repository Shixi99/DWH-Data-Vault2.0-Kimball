-- 01_indexes.sql
-- Composite indexes enabling index-only scans in LATERAL PIT lookups.
-- Critical for performance at scale.

CREATE INDEX IF NOT EXISTS idx_sat_customer_hk_ldts ON dv.sat_customer_details (hub_customer_hk, load_date DESC);
CREATE INDEX IF NOT EXISTS idx_sat_product_hk_ldts  ON dv.sat_product_details  (hub_product_hk,  load_date DESC);
CREATE INDEX IF NOT EXISTS idx_sat_order_hk_ldts    ON dv.sat_order_details    (hub_order_hk,    load_date DESC);

CREATE INDEX IF NOT EXISTS idx_sat_customer_del_hk  ON dv.sat_customer_deleted (hub_customer_hk, load_date DESC);
CREATE INDEX IF NOT EXISTS idx_sat_product_del_hk   ON dv.sat_product_deleted  (hub_product_hk,  load_date DESC);
CREATE INDEX IF NOT EXISTS idx_sat_order_del_hk     ON dv.sat_order_deleted    (hub_order_hk,    load_date DESC);

CREATE INDEX IF NOT EXISTS idx_lnk_order_hk         ON dv.lnk_order_customer_product (hub_order_hk);
CREATE INDEX IF NOT EXISTS idx_lnk_customer_hk      ON dv.lnk_order_customer_product (hub_customer_hk);
CREATE INDEX IF NOT EXISTS idx_lnk_product_hk       ON dv.lnk_order_customer_product (hub_product_hk);
