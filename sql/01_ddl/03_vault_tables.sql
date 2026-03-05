-- 03_vault_tables.sql  —  All DV layer tables: Hubs, Satellites, Links, PITs

-- ================================================================
-- HUBS
-- ================================================================
DROP TABLE IF EXISTS dv.hub_customer CASCADE;
CREATE TABLE dv.hub_customer (
    hub_customer_hk  CHAR(32)      NOT NULL,
    customer_bk      VARCHAR(50)   NOT NULL,
    load_date        TIMESTAMP     NOT NULL,
    record_source    VARCHAR(100)  NOT NULL,
    CONSTRAINT pk_hub_customer PRIMARY KEY (hub_customer_hk)
);
CREATE UNIQUE INDEX uix_hub_customer_bk ON dv.hub_customer (customer_bk);

DROP TABLE IF EXISTS dv.hub_product CASCADE;
CREATE TABLE dv.hub_product (
    hub_product_hk   CHAR(32)      NOT NULL,
    product_bk       VARCHAR(50)   NOT NULL,
    load_date        TIMESTAMP     NOT NULL,
    record_source    VARCHAR(100)  NOT NULL,
    CONSTRAINT pk_hub_product PRIMARY KEY (hub_product_hk)
);
CREATE UNIQUE INDEX uix_hub_product_bk ON dv.hub_product (product_bk);

DROP TABLE IF EXISTS dv.hub_order CASCADE;
CREATE TABLE dv.hub_order (
    hub_order_hk     CHAR(32)      NOT NULL,
    order_bk         VARCHAR(50)   NOT NULL,
    load_date        TIMESTAMP     NOT NULL,
    record_source    VARCHAR(100)  NOT NULL,
    CONSTRAINT pk_hub_order PRIMARY KEY (hub_order_hk)
);
CREATE UNIQUE INDEX uix_hub_order_bk ON dv.hub_order (order_bk);

-- ================================================================
-- SATELLITES — details
-- ================================================================
DROP TABLE IF EXISTS dv.sat_customer_details CASCADE;
CREATE TABLE dv.sat_customer_details (
    hub_customer_hk  CHAR(32)      NOT NULL,
    load_date        TIMESTAMP     NOT NULL,
    load_end_date    TIMESTAMP,
    record_source    VARCHAR(100)  NOT NULL,
    hash_diff        CHAR(32)      NOT NULL,
    full_name        VARCHAR(120),
    email            VARCHAR(200),
    phone            VARCHAR(30),
    country          VARCHAR(80),
    tier             VARCHAR(20),
    CONSTRAINT pk_sat_customer_details PRIMARY KEY (hub_customer_hk, load_date),
    CONSTRAINT fk_sat_cust_hub FOREIGN KEY (hub_customer_hk) REFERENCES dv.hub_customer(hub_customer_hk)
);

DROP TABLE IF EXISTS dv.sat_product_details CASCADE;
CREATE TABLE dv.sat_product_details (
    hub_product_hk   CHAR(32)      NOT NULL,
    load_date        TIMESTAMP     NOT NULL,
    load_end_date    TIMESTAMP,
    record_source    VARCHAR(100)  NOT NULL,
    hash_diff        CHAR(32)      NOT NULL,
    product_name     VARCHAR(200),
    category         VARCHAR(80),
    unit_price       NUMERIC(10,2),
    currency         CHAR(3),
    is_active        BOOLEAN,
    CONSTRAINT pk_sat_product_details PRIMARY KEY (hub_product_hk, load_date),
    CONSTRAINT fk_sat_prod_hub FOREIGN KEY (hub_product_hk) REFERENCES dv.hub_product(hub_product_hk)
);

DROP TABLE IF EXISTS dv.sat_order_details CASCADE;
CREATE TABLE dv.sat_order_details (
    hub_order_hk     CHAR(32)      NOT NULL,
    load_date        TIMESTAMP     NOT NULL,
    load_end_date    TIMESTAMP,
    record_source    VARCHAR(100)  NOT NULL,
    hash_diff        CHAR(32)      NOT NULL,
    quantity         INT,
    unit_price       NUMERIC(10,2),
    total_amount     NUMERIC(12,2),
    order_date       DATE,
    status           VARCHAR(30),
    CONSTRAINT pk_sat_order_details PRIMARY KEY (hub_order_hk, load_date),
    CONSTRAINT fk_sat_ord_hub FOREIGN KEY (hub_order_hk) REFERENCES dv.hub_order(hub_order_hk)
);

-- ================================================================
-- SATELLITES — soft delete (one per hub, DV2.0 pattern)
-- ================================================================
CREATE TABLE IF NOT EXISTS dv.sat_customer_deleted (
    hub_customer_hk  CHAR(32)      NOT NULL,
    load_date        TIMESTAMP     NOT NULL,
    load_end_date    TIMESTAMP,
    record_source    VARCHAR(100)  NOT NULL,
    is_deleted       BOOLEAN       NOT NULL DEFAULT TRUE,
    CONSTRAINT pk_sat_customer_deleted PRIMARY KEY (hub_customer_hk, load_date),
    CONSTRAINT fk_sat_cust_del FOREIGN KEY (hub_customer_hk) REFERENCES dv.hub_customer(hub_customer_hk)
);

CREATE TABLE IF NOT EXISTS dv.sat_product_deleted (
    hub_product_hk   CHAR(32)      NOT NULL,
    load_date        TIMESTAMP     NOT NULL,
    load_end_date    TIMESTAMP,
    record_source    VARCHAR(100)  NOT NULL,
    is_deleted       BOOLEAN       NOT NULL DEFAULT TRUE,
    CONSTRAINT pk_sat_product_deleted PRIMARY KEY (hub_product_hk, load_date),
    CONSTRAINT fk_sat_prod_del FOREIGN KEY (hub_product_hk) REFERENCES dv.hub_product(hub_product_hk)
);

CREATE TABLE IF NOT EXISTS dv.sat_order_deleted (
    hub_order_hk     CHAR(32)      NOT NULL,
    load_date        TIMESTAMP     NOT NULL,
    load_end_date    TIMESTAMP,
    record_source    VARCHAR(100)  NOT NULL,
    is_deleted       BOOLEAN       NOT NULL DEFAULT TRUE,
    CONSTRAINT pk_sat_order_deleted PRIMARY KEY (hub_order_hk, load_date),
    CONSTRAINT fk_sat_ord_del FOREIGN KEY (hub_order_hk) REFERENCES dv.hub_order(hub_order_hk)
);

-- ================================================================
-- LINK
-- ================================================================
DROP TABLE IF EXISTS dv.lnk_order_customer_product CASCADE;
CREATE TABLE dv.lnk_order_customer_product (
    lnk_order_hk     CHAR(32)      NOT NULL,
    hub_order_hk     CHAR(32)      NOT NULL,
    hub_customer_hk  CHAR(32)      NOT NULL,
    hub_product_hk   CHAR(32)      NOT NULL,
    load_date        TIMESTAMP     NOT NULL,
    record_source    VARCHAR(100)  NOT NULL,
    CONSTRAINT pk_lnk_order     PRIMARY KEY (lnk_order_hk),
    CONSTRAINT fk_lnk_order     FOREIGN KEY (hub_order_hk)    REFERENCES dv.hub_order(hub_order_hk),
    CONSTRAINT fk_lnk_customer  FOREIGN KEY (hub_customer_hk) REFERENCES dv.hub_customer(hub_customer_hk),
    CONSTRAINT fk_lnk_product   FOREIGN KEY (hub_product_hk)  REFERENCES dv.hub_product(hub_product_hk)
);

-- ================================================================
-- PIT TABLES
-- ================================================================
DROP TABLE IF EXISTS dv.pit_customer CASCADE;
CREATE TABLE dv.pit_customer (
    hub_customer_hk           CHAR(32)   NOT NULL,
    snapshot_date             DATE       NOT NULL,
    sat_customer_details_ldts TIMESTAMP,
    sat_customer_deleted_ldts TIMESTAMP,
    CONSTRAINT pk_pit_customer PRIMARY KEY (hub_customer_hk, snapshot_date)
);

DROP TABLE IF EXISTS dv.pit_product CASCADE;
CREATE TABLE dv.pit_product (
    hub_product_hk            CHAR(32)   NOT NULL,
    snapshot_date             DATE       NOT NULL,
    sat_product_details_ldts  TIMESTAMP,
    sat_product_deleted_ldts  TIMESTAMP,
    CONSTRAINT pk_pit_product PRIMARY KEY (hub_product_hk, snapshot_date)
);

DROP TABLE IF EXISTS dv.pit_order CASCADE;
CREATE TABLE dv.pit_order (
    hub_order_hk              CHAR(32)   NOT NULL,
    snapshot_date             DATE       NOT NULL,
    sat_order_details_ldts    TIMESTAMP,
    sat_order_deleted_ldts    TIMESTAMP,
    CONSTRAINT pk_pit_order PRIMARY KEY (hub_order_hk, snapshot_date)
);
