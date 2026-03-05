-- 02_source_tables.sql  —  Simulated operational source tables + seed data

DROP TABLE IF EXISTS src.orders    CASCADE;
DROP TABLE IF EXISTS src.customers CASCADE;
DROP TABLE IF EXISTS src.products  CASCADE;

CREATE TABLE src.customers (
    customer_id  INT           PRIMARY KEY,
    full_name    VARCHAR(120)  NOT NULL,
    email        VARCHAR(200)  NOT NULL,
    phone        VARCHAR(30),
    country      VARCHAR(80),
    tier         VARCHAR(20),
    created_at   TIMESTAMP     NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMP     NOT NULL DEFAULT NOW()
);
INSERT INTO src.customers VALUES
(1,'Alice Johnson','alice@example.com','+1-555-0101','USA','gold','2023-01-10 08:00:00','2023-01-10 08:00:00'),
(2,'Bob Martinez','bob@example.com','+1-555-0202','USA','silver','2023-02-14 09:15:00','2023-02-14 09:15:00'),
(3,'Clara Schmidt','clara@example.com','+49-30-1234','Germany','bronze','2023-03-01 10:00:00','2023-03-01 10:00:00'),
(4,'David Okafor','david@example.com','+234-800-00','Nigeria','silver','2023-03-20 11:30:00','2023-03-20 11:30:00'),
(5,'Eva Chen','eva@example.com','+86-21-5555','China','gold','2023-04-05 07:45:00','2023-04-05 07:45:00');

CREATE TABLE src.products (
    product_id    INT            PRIMARY KEY,
    product_code  VARCHAR(30)    NOT NULL UNIQUE,
    product_name  VARCHAR(200)   NOT NULL,
    category      VARCHAR(80),
    unit_price    NUMERIC(10,2)  NOT NULL,
    currency      CHAR(3)        NOT NULL DEFAULT 'USD',
    is_active     BOOLEAN        NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMP      NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP      NOT NULL DEFAULT NOW()
);
INSERT INTO src.products VALUES
(101,'PROD-A001','Wireless Keyboard','Electronics',49.99,'USD',TRUE,'2023-01-05 08:00:00','2023-01-05 08:00:00'),
(102,'PROD-A002','USB-C Hub 7-port','Electronics',34.99,'USD',TRUE,'2023-01-05 08:00:00','2023-01-05 08:00:00'),
(103,'PROD-B001','Ergonomic Chair','Furniture',299.00,'USD',TRUE,'2023-02-01 09:00:00','2023-02-01 09:00:00'),
(104,'PROD-B002','Standing Desk','Furniture',549.00,'USD',TRUE,'2023-02-01 09:00:00','2023-02-01 09:00:00'),
(105,'PROD-C001','Noise-Cancel Headset','Electronics',89.99,'USD',TRUE,'2023-03-10 10:00:00','2023-03-10 10:00:00');

CREATE TABLE src.orders (
    order_id      INT            PRIMARY KEY,
    customer_id   INT            NOT NULL REFERENCES src.customers(customer_id),
    product_id    INT            NOT NULL REFERENCES src.products(product_id),
    quantity      INT            NOT NULL DEFAULT 1,
    unit_price    NUMERIC(10,2)  NOT NULL,
    total_amount  NUMERIC(12,2)  NOT NULL,
    order_date    DATE           NOT NULL,
    status        VARCHAR(30)    NOT NULL DEFAULT 'pending',
    created_at    TIMESTAMP      NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP      NOT NULL DEFAULT NOW()
);
INSERT INTO src.orders VALUES
(1001,1,101,1,49.99,49.99,'2023-05-01','delivered','2023-05-01 10:00:00','2023-05-03 14:00:00'),
(1002,1,103,1,299.00,299.00,'2023-05-02','delivered','2023-05-02 11:00:00','2023-05-05 09:00:00'),
(1003,2,102,2,34.99,69.98,'2023-05-10','shipped','2023-05-10 12:00:00','2023-05-11 08:00:00'),
(1004,3,105,1,89.99,89.99,'2023-05-15','pending','2023-05-15 15:00:00','2023-05-15 15:00:00'),
(1005,4,104,1,549.00,549.00,'2023-05-20','delivered','2023-05-20 09:00:00','2023-05-25 10:00:00'),
(1006,5,101,3,49.99,149.97,'2023-06-01','delivered','2023-06-01 08:00:00','2023-06-04 11:00:00'),
(1007,2,103,1,299.00,299.00,'2023-06-05','cancelled','2023-06-05 13:00:00','2023-06-06 10:00:00'),
(1008,5,105,2,89.99,179.98,'2023-06-10','shipped','2023-06-10 09:00:00','2023-06-11 07:00:00');
