-- TASK 6: DDL SCRIPT FOR 3NF LAYER (INCLUDES DEFAULT ROWS)
-- Global Retail Superstore Sales
-- Tamar Tutisani
-- Schema: BL_3NF
-- Purpose: Create normalized entity tables for the Business Layer 3NF
--          and insert default (-1) rows into all dimension tables
-- This script is rerunnable: DROP TABLE IF EXISTS CASCADE ensures clean slate
-- Convention for default rows:
--   Numeric IDs  : -1
--   Text fields  : 'n.a.'
--   insert/update: '1900-01-01'
--   end_dt       : '9999-12-31'
--   source_system: 'MANUAL'
--   source_entity: 'MANUAL'
-----------------------------------------------------------------------------


-- STEP 0: Create the BL_3NF schema
-----------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS bl_3nf;


-- STEP 1: Create SEQUENCES for surrogate key generation
-- One sequence per entity table. SEQUENCES are used instead of SERIAL type.
-----------------------------------------------------------------------------

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_date_id                START 100 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_product_category_id    START 100 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_product_subcategory_id START 100 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_product_id             START 100 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_market_id              START 100 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_region_id              START 100 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_country_id             START 100 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_state_id               START 100 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_city_id                START 100 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_customer_id            START 100 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_employee_id            START 100 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_order_attr_id          START 100 INCREMENT 1;


-- STEP 2: Create CE_ TABLES
-- Naming convention: ce_<entity_plural>
-- SCD Type 1 : ce_<entity>
-- SCD Type 2 : ce_<entity>_scd
-- Fact table : ce_sales
-- All tables include SOURCE_SYSTEM and SOURCE_ENTITY for lineage tracking.
-- All non-KPI columns are NOT NULL.
-- KPI/metric columns in the fact table are the ONLY nullable columns.
-----------------------------------------------------------------------------


-- CE_DATES (SCD Type 0 - calendar dates never change)
-- Populated independently by a date generation script for 2024-2025.
-- Structure defined here; data loaded separately.
-- Includes a default (-1) row so fact rows with a missing/unresolvable
-- order_date never fail the FK constraint.
DROP TABLE IF EXISTS bl_3nf.ce_dates CASCADE;

CREATE TABLE bl_3nf.ce_dates (
    date_id          INTEGER      NOT NULL PRIMARY KEY,
    date_dt          DATE         NOT NULL,
    day_of_week_no   INTEGER      NOT NULL,
    day_of_week_desc VARCHAR(25)  NOT NULL,
    weekend_flag     INTEGER      NOT NULL,
    iso_week_no      INTEGER      NOT NULL,
    day_of_month_no  INTEGER      NOT NULL,
    month_value      VARCHAR(2)   NOT NULL,
    month_desc       VARCHAR(25)  NOT NULL,
    quarter_value    VARCHAR(1)   NOT NULL,
    quarter_desc     VARCHAR(2)   NOT NULL,
    year_value       VARCHAR(4)   NOT NULL,
    insert_dt        DATE         NOT NULL,
    update_dt        DATE         NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ce_dates_date_dt
    ON bl_3nf.ce_dates(date_dt);


-- CE_PRODUCT_CATEGORIES (SCD Type 1 - product hierarchy level 1)
-- Furniture, Office Supplies, Technology
DROP TABLE IF EXISTS bl_3nf.ce_product_categories CASCADE;

CREATE TABLE bl_3nf.ce_product_categories (
    product_category_id     BIGINT       NOT NULL PRIMARY KEY,
    product_category_src_id VARCHAR(100) NOT NULL UNIQUE,
    product_category_name   VARCHAR(100) NOT NULL,
    insert_dt               DATE         NOT NULL,
    update_dt               DATE         NOT NULL,
    source_system           VARCHAR(100) NOT NULL,
    source_entity           VARCHAR(100) NOT NULL
);


-- CE_PRODUCT_SUBCATEGORIES (SCD Type 1 - product hierarchy level 2)
DROP TABLE IF EXISTS bl_3nf.ce_product_subcategories CASCADE;

CREATE TABLE bl_3nf.ce_product_subcategories (
    product_subcategory_id     BIGINT       NOT NULL PRIMARY KEY,
    product_subcategory_src_id VARCHAR(100) NOT NULL UNIQUE,
    product_subcategory_name   VARCHAR(100) NOT NULL,
    product_category_id        BIGINT       NOT NULL,
    insert_dt                  DATE         NOT NULL,
    update_dt                  DATE         NOT NULL,
    source_system              VARCHAR(100) NOT NULL,
    source_entity              VARCHAR(100) NOT NULL,
    CONSTRAINT fk_subcategory_to_category
        FOREIGN KEY (product_category_id)
        REFERENCES bl_3nf.ce_product_categories(product_category_id)
);


-- CE_PRODUCTS (SCD Type 1 - leaf of product hierarchy, conformed across both sources)
DROP TABLE IF EXISTS bl_3nf.ce_products CASCADE;

CREATE TABLE bl_3nf.ce_products (
    product_id             BIGINT       NOT NULL PRIMARY KEY,
    product_src_id         VARCHAR(50)  NOT NULL UNIQUE,
    product_name           VARCHAR(255) NOT NULL,
    product_subcategory_id BIGINT       NOT NULL,
    insert_dt              DATE         NOT NULL,
    update_dt              DATE         NOT NULL,
    source_system          VARCHAR(100) NOT NULL,
    source_entity          VARCHAR(100) NOT NULL,
    CONSTRAINT fk_product_to_subcategory
        FOREIGN KEY (product_subcategory_id)
        REFERENCES bl_3nf.ce_product_subcategories(product_subcategory_id)
);


-- CE_MARKETS (SCD Type 1 - geography hierarchy level 1)
-- US, Canada, APAC, EU, EMEA, LATAM, Africa
DROP TABLE IF EXISTS bl_3nf.ce_markets CASCADE;

CREATE TABLE bl_3nf.ce_markets (
    market_id     BIGINT       NOT NULL PRIMARY KEY,
    market_src_id VARCHAR(100) NOT NULL UNIQUE,
    market_name   VARCHAR(50)  NOT NULL,
    insert_dt     DATE         NOT NULL,
    update_dt     DATE         NOT NULL,
    source_system VARCHAR(100) NOT NULL,
    source_entity VARCHAR(100) NOT NULL
);


-- CE_REGIONS (SCD Type 1 - geography hierarchy level 2)
-- region_src_id = region_name || '_' || market_name to ensure global uniqueness
-- because the same region name (e.g. 'Central') can exist in multiple markets.
DROP TABLE IF EXISTS bl_3nf.ce_regions CASCADE;

CREATE TABLE bl_3nf.ce_regions (
    region_id     BIGINT       NOT NULL PRIMARY KEY,
    region_src_id VARCHAR(255) NOT NULL UNIQUE,
    region_name   VARCHAR(100) NOT NULL,
    market_id     BIGINT       NOT NULL,
    insert_dt     DATE         NOT NULL,
    update_dt     DATE         NOT NULL,
    source_system VARCHAR(100) NOT NULL,
    source_entity VARCHAR(100) NOT NULL,
    CONSTRAINT fk_region_to_market
        FOREIGN KEY (market_id)
        REFERENCES bl_3nf.ce_markets(market_id)
);


-- CE_COUNTRIES (SCD Type 1 - geography hierarchy level 3)
DROP TABLE IF EXISTS bl_3nf.ce_countries CASCADE;

CREATE TABLE bl_3nf.ce_countries (
    country_id     BIGINT       NOT NULL PRIMARY KEY,
    country_src_id VARCHAR(100) NOT NULL UNIQUE,
    country_name   VARCHAR(100) NOT NULL,
    region_id      BIGINT       NOT NULL,
    insert_dt      DATE         NOT NULL,
    update_dt      DATE         NOT NULL,
    source_system  VARCHAR(100) NOT NULL,
    source_entity  VARCHAR(100) NOT NULL,
    CONSTRAINT fk_country_to_region
        FOREIGN KEY (region_id)
        REFERENCES bl_3nf.ce_regions(region_id)
);


-- CE_STATES (SCD Type 1 - geography hierarchy level 4)
-- state_src_id = state_name || '_' || country_name to ensure global uniqueness
-- because the same state name can exist in multiple countries.
-- Domestic rows: state_name = 'N/A', state_src_id = 'N/A_<country>'.
DROP TABLE IF EXISTS bl_3nf.ce_states CASCADE;

CREATE TABLE bl_3nf.ce_states (
    state_id      BIGINT       NOT NULL PRIMARY KEY,
    state_src_id  VARCHAR(255) NOT NULL UNIQUE,
    state_name    VARCHAR(100) NOT NULL,
    country_id    BIGINT       NOT NULL,
    insert_dt     DATE         NOT NULL,
    update_dt     DATE         NOT NULL,
    source_system VARCHAR(100) NOT NULL,
    source_entity VARCHAR(100) NOT NULL,
    CONSTRAINT fk_state_to_country
        FOREIGN KEY (country_id)
        REFERENCES bl_3nf.ce_countries(country_id)
);


-- CE_CITIES (SCD Type 1 - geography hierarchy level 5, leaf)
-- city_src_id = city || '_' || country || '_' || region (composite natural key)
DROP TABLE IF EXISTS bl_3nf.ce_cities CASCADE;

CREATE TABLE bl_3nf.ce_cities (
    city_id       BIGINT       NOT NULL PRIMARY KEY,
    city_src_id   VARCHAR(255) NOT NULL UNIQUE,
    city_name     VARCHAR(100) NOT NULL,
    state_id      BIGINT       NOT NULL,
    insert_dt     DATE         NOT NULL,
    update_dt     DATE         NOT NULL,
    source_system VARCHAR(100) NOT NULL,
    source_entity VARCHAR(100) NOT NULL,
    CONSTRAINT fk_city_to_state
        FOREIGN KEY (state_id)
        REFERENCES bl_3nf.ce_states(state_id)
);


-- CE_CUSTOMERS_SCD (SCD Type 2 - tracks historical changes to customer_segment)
-- Composite PK: (customer_id, start_dt) per SCD2 convention on 3NF layer.
-- No FK database constraint from fact table to this table (SCD2 rule).
DROP TABLE IF EXISTS bl_3nf.ce_customers_scd CASCADE;

CREATE TABLE bl_3nf.ce_customers_scd (
    customer_id      BIGINT       NOT NULL,
    customer_src_id  VARCHAR(100) NOT NULL,
    customer_name    VARCHAR(255) NOT NULL,
    customer_segment VARCHAR(50)  NOT NULL,
    start_dt         DATE         NOT NULL,
    end_dt           DATE         NOT NULL,
    is_active        VARCHAR(1)   NOT NULL,
    insert_dt        DATE         NOT NULL,
    source_system    VARCHAR(100) NOT NULL,
    source_entity    VARCHAR(100) NOT NULL,
    PRIMARY KEY (customer_id, start_dt)
);

CREATE INDEX IF NOT EXISTS idx_ce_customers_scd_src_id
    ON bl_3nf.ce_customers_scd(customer_src_id);
CREATE INDEX IF NOT EXISTS idx_ce_customers_scd_active
    ON bl_3nf.ce_customers_scd(is_active, end_dt);


-- CE_EMPLOYEES (SCD Type 1 - conformed across both sources)
-- Domestic provides full name; International provides ID only -> name = 'n.a.'
DROP TABLE IF EXISTS bl_3nf.ce_employees CASCADE;

CREATE TABLE bl_3nf.ce_employees (
    employee_id     BIGINT       NOT NULL PRIMARY KEY,
    employee_src_id VARCHAR(50)  NOT NULL UNIQUE,
    employee_name   VARCHAR(255) NOT NULL,
    insert_dt       DATE         NOT NULL,
    update_dt       DATE         NOT NULL,
    source_system   VARCHAR(100) NOT NULL,
    source_entity   VARCHAR(100) NOT NULL
);


-- CE_ORDER_ATTRIBUTES (SCD Type 0 - junk dimension)
-- Ship_Mode x Order_Priority combinations, from Domestic source only.
-- International fact rows reference the default (-1) row.
DROP TABLE IF EXISTS bl_3nf.ce_order_attributes CASCADE;

CREATE TABLE bl_3nf.ce_order_attributes (
    order_attr_id  BIGINT       NOT NULL PRIMARY KEY,
    ship_mode      VARCHAR(50)  NOT NULL,
    order_priority VARCHAR(25)  NOT NULL,
    insert_dt      DATE         NOT NULL,
    update_dt      DATE         NOT NULL,
    source_system  VARCHAR(100) NOT NULL,
    source_entity  VARCHAR(100) NOT NULL,
    UNIQUE (ship_mode, order_priority)
);


-- CE_SALES (fact table at 3NF level - transaction grain)
-- One row per product line item on a customer order.
-- Loaded from both SA_DOMESTIC and SA_INTERNATIONAL.
-- KPI columns are the ONLY nullable columns per naming conventions.
-- No FK constraint to CE_CUSTOMERS_SCD (SCD2 composite PK, logical FK only).
DROP TABLE IF EXISTS bl_3nf.ce_sales CASCADE;

CREATE TABLE bl_3nf.ce_sales (
    event_dt          DATE         NOT NULL,
    date_id           INTEGER      NOT NULL,
    product_id        BIGINT       NOT NULL,
    customer_id       BIGINT       NOT NULL,
    city_id           BIGINT       NOT NULL,
    employee_id       BIGINT       NOT NULL,
    order_attr_id     BIGINT       NOT NULL,
    order_id          VARCHAR(50)  NOT NULL,
    sales_amt         NUMERIC(15,2),
    cost_amt          NUMERIC(15,2),
    profit_amt        NUMERIC(15,2),
    shipping_cost_amt NUMERIC(15,2),
    quantity_cnt      NUMERIC(10,0),
    discount_amt      NUMERIC(5,4),
    insert_dt         DATE         NOT NULL,
    update_dt         DATE         NOT NULL,
    source_system     VARCHAR(100) NOT NULL,
    source_entity     VARCHAR(100) NOT NULL,
    CONSTRAINT fk_sales_to_date
        FOREIGN KEY (date_id)
        REFERENCES bl_3nf.ce_dates(date_id),
    CONSTRAINT fk_sales_to_product
        FOREIGN KEY (product_id)
        REFERENCES bl_3nf.ce_products(product_id),
    CONSTRAINT fk_sales_to_employee
        FOREIGN KEY (employee_id)
        REFERENCES bl_3nf.ce_employees(employee_id),
    CONSTRAINT fk_sales_to_city
        FOREIGN KEY (city_id)
        REFERENCES bl_3nf.ce_cities(city_id),
    CONSTRAINT fk_sales_to_order_attr
        FOREIGN KEY (order_attr_id)
        REFERENCES bl_3nf.ce_order_attributes(order_attr_id)
    -- No FK to CE_CUSTOMERS_SCD: SCD2 table has composite PK,
    -- relationship enforced at ETL level only per naming convention rules
);

CREATE INDEX IF NOT EXISTS idx_ce_sales_event_dt    ON bl_3nf.ce_sales(event_dt);
CREATE INDEX IF NOT EXISTS idx_ce_sales_product_id  ON bl_3nf.ce_sales(product_id);
CREATE INDEX IF NOT EXISTS idx_ce_sales_customer_id ON bl_3nf.ce_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_ce_sales_city_id     ON bl_3nf.ce_sales(city_id);
CREATE INDEX IF NOT EXISTS idx_ce_sales_employee_id ON bl_3nf.ce_sales(employee_id);
CREATE INDEX IF NOT EXISTS idx_ce_sales_order_id    ON bl_3nf.ce_sales(order_id);


-- STEP 3: Populate CE_DATES for full 2024-2025 range
-- Generates one row per calendar day from 2024-01-01 to 2025-12-31.
-- date_id is in YYYYMMDD integer format (e.g. 20240101).
-- This runs once as part of DDL; tables are dropped and recreated on each DDL run.
-- quarter_value is VARCHAR(1). TO_CHAR(int, '9') returns a value padded
-- with a leading space (e.g. ' 1' -> 2 chars), which overflowed VARCHAR(1).
-- Wrapped in TRIM() to strip the padding.
-----------------------------------------------------------------------------

INSERT INTO bl_3nf.ce_dates (
    date_id,
    date_dt,
    day_of_week_no,
    day_of_week_desc,
    weekend_flag,
    iso_week_no,
    day_of_month_no,
    month_value,
    month_desc,
    quarter_value,
    quarter_desc,
    year_value,
    insert_dt,
    update_dt
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER AS date_id,
    d AS date_dt,
    EXTRACT(ISODOW FROM d)::INTEGER AS day_of_week_no,
    TRIM(TO_CHAR(d, 'Day')) AS day_of_week_desc,
    CASE WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN 1
         ELSE 0 END AS weekend_flag,
    EXTRACT(WEEK FROM d)::INTEGER AS iso_week_no,
    EXTRACT(DAY FROM d)::INTEGER AS day_of_month_no,
    TO_CHAR(d, 'MM') AS month_value,
    TRIM(TO_CHAR(d, 'Month')) AS month_desc,
    TRIM(TO_CHAR(EXTRACT(QUARTER FROM d)::INTEGER, '9')) AS quarter_value,
    'Q' || EXTRACT(QUARTER FROM d)::INTEGER AS quarter_desc,
    TO_CHAR(d, 'YYYY') AS year_value,
    CURRENT_DATE AS insert_dt,
    CURRENT_DATE AS update_dt
FROM GENERATE_SERIES(
    '2024-01-01'::DATE,
    '2025-12-31'::DATE,
    '1 day'::INTERVAL
) AS d
ON CONFLICT (date_id) DO NOTHING;

COMMIT;


-- STEP 4: Verification - confirm all tables created
-----------------------------------------------------------------------------

SELECT table_schema,
       table_name,
       table_type
FROM information_schema.tables
WHERE table_schema = 'bl_3nf'
ORDER BY table_name;


-- STEP 5: Insert default (-1) rows into all dimension tables
-- Default rows are required in every dimension table except the fact table.
-- They serve as FK targets when a dimension value is unknown or not applicable.
-- ON CONFLICT DO NOTHING makes this block safe to re-run.
-- FIX: added a default (-1) row for CE_DATES so any fact row whose order_date
-- cannot be resolved to a real calendar date still satisfies fk_sales_to_date
-- instead of failing (previous DML silently computed a bogus date_id that was
-- never inserted into CE_DATES, causing the FK violation).
-----------------------------------------------------------------------------

-- CE_DATES default row
INSERT INTO bl_3nf.ce_dates (
    date_id,
    date_dt,
    day_of_week_no,
    day_of_week_desc,
    weekend_flag,
    iso_week_no,
    day_of_month_no,
    month_value,
    month_desc,
    quarter_value,
    quarter_desc,
    year_value,
    insert_dt,
    update_dt
)
VALUES (
    -1,
    '1900-01-01',
    -1,
    'n.a.',
    -1,
    -1,
    -1,
    'n.',
    'n.a.',
    'n',
    'n.',
    'n.a.',
    '1900-01-01',
    '1900-01-01'
)
ON CONFLICT (date_id) DO NOTHING;

COMMIT;


-- CE_PRODUCT_CATEGORIES default row
INSERT INTO bl_3nf.ce_product_categories (
    product_category_id,
    product_category_src_id,
    product_category_name,
    insert_dt,
    update_dt,
    source_system,
    source_entity
)
VALUES (
    -1,
    'n.a.',
    'n.a.',
    '1900-01-01',
    '1900-01-01',
    'MANUAL',
    'MANUAL'
)
ON CONFLICT (product_category_id) DO NOTHING;

COMMIT;


-- CE_PRODUCT_SUBCATEGORIES default row
INSERT INTO bl_3nf.ce_product_subcategories (
    product_subcategory_id,
    product_subcategory_src_id,
    product_subcategory_name,
    product_category_id,
    insert_dt,
    update_dt,
    source_system,
    source_entity
)
VALUES (
    -1,
    'n.a.',
    'n.a.',
    -1,
    '1900-01-01',
    '1900-01-01',
    'MANUAL',
    'MANUAL'
)
ON CONFLICT (product_subcategory_id) DO NOTHING;

COMMIT;


-- CE_PRODUCTS default row
INSERT INTO bl_3nf.ce_products (
    product_id,
    product_src_id,
    product_name,
    product_subcategory_id,
    insert_dt,
    update_dt,
    source_system,
    source_entity
)
VALUES (
    -1,
    'n.a.',
    'n.a.',
    -1,
    '1900-01-01',
    '1900-01-01',
    'MANUAL',
    'MANUAL'
)
ON CONFLICT (product_id) DO NOTHING;

COMMIT;


-- CE_MARKETS default row
INSERT INTO bl_3nf.ce_markets (
    market_id,
    market_src_id,
    market_name,
    insert_dt,
    update_dt,
    source_system,
    source_entity
)
VALUES (
    -1,
    'n.a.',
    'n.a.',
    '1900-01-01',
    '1900-01-01',
    'MANUAL',
    'MANUAL'
)
ON CONFLICT (market_id) DO NOTHING;

COMMIT;


-- CE_REGIONS default row
INSERT INTO bl_3nf.ce_regions (
    region_id,
    region_src_id,
    region_name,
    market_id,
    insert_dt,
    update_dt,
    source_system,
    source_entity
)
VALUES (
    -1,
    'n.a.',
    'n.a.',
    -1,
    '1900-01-01',
    '1900-01-01',
    'MANUAL',
    'MANUAL'
)
ON CONFLICT (region_id) DO NOTHING;

COMMIT;


-- CE_COUNTRIES default row
INSERT INTO bl_3nf.ce_countries (
    country_id,
    country_src_id,
    country_name,
    region_id,
    insert_dt,
    update_dt,
    source_system,
    source_entity
)
VALUES (
    -1,
    'n.a.',
    'n.a.',
    -1,
    '1900-01-01',
    '1900-01-01',
    'MANUAL',
    'MANUAL'
)
ON CONFLICT (country_id) DO NOTHING;

COMMIT;


-- CE_STATES default row
INSERT INTO bl_3nf.ce_states (
    state_id,
    state_src_id,
    state_name,
    country_id,
    insert_dt,
    update_dt,
    source_system,
    source_entity
)
VALUES (
    -1,
    'n.a.',
    'n.a.',
    -1,
    '1900-01-01',
    '1900-01-01',
    'MANUAL',
    'MANUAL'
)
ON CONFLICT (state_id) DO NOTHING;

COMMIT;


-- CE_CITIES default row
INSERT INTO bl_3nf.ce_cities (
    city_id,
    city_src_id,
    city_name,
    state_id,
    insert_dt,
    update_dt,
    source_system,
    source_entity
)
VALUES (
    -1,
    'n.a.',
    'n.a.',
    -1,
    '1900-01-01',
    '1900-01-01',
    'MANUAL',
    'MANUAL'
)
ON CONFLICT (city_id) DO NOTHING;

COMMIT;


-- CE_CUSTOMERS_SCD default row (SCD Type 2)
INSERT INTO bl_3nf.ce_customers_scd (
    customer_id,
    customer_src_id,
    customer_name,
    customer_segment,
    start_dt,
    end_dt,
    is_active,
    insert_dt,
    source_system,
    source_entity
)
VALUES (
    -1,
    'n.a.',
    'n.a.',
    'n.a.',
    '1900-01-01',
    '9999-12-31',
    'Y',
    '1900-01-01',
    'MANUAL',
    'MANUAL'
)
ON CONFLICT (customer_id, start_dt) DO NOTHING;

COMMIT;


-- CE_EMPLOYEES default row
INSERT INTO bl_3nf.ce_employees (
    employee_id,
    employee_src_id,
    employee_name,
    insert_dt,
    update_dt,
    source_system,
    source_entity
)
VALUES (
    -1,
    'n.a.',
    'n.a.',
    '1900-01-01',
    '1900-01-01',
    'MANUAL',
    'MANUAL'
)
ON CONFLICT (employee_id) DO NOTHING;

COMMIT;


-- CE_ORDER_ATTRIBUTES default row (junk dimension SCD Type 0)
INSERT INTO bl_3nf.ce_order_attributes (
    order_attr_id,
    ship_mode,
    order_priority,
    insert_dt,
    update_dt,
    source_system,
    source_entity
)
VALUES (
    -1,
    'n.a.',
    'n.a.',
    '1900-01-01',
    '1900-01-01',
    'MANUAL',
    'MANUAL'
)
ON CONFLICT (order_attr_id) DO NOTHING;

COMMIT;


-- STEP 6: Verify default rows are present in all dimension tables
-- Every table below must return count = 1
-----------------------------------------------------------------------------

SELECT 'CE_DATES' AS table_name, COUNT(*) AS default_row_count
FROM bl_3nf.ce_dates
WHERE date_id = -1
UNION ALL
SELECT 'CE_PRODUCT_CATEGORIES', COUNT(*)
FROM bl_3nf.ce_product_categories 
WHERE product_category_id = -1
UNION ALL
SELECT 'CE_PRODUCT_SUBCATEGORIES', COUNT(*)
FROM bl_3nf.ce_product_subcategories
WHERE product_subcategory_id = -1
UNION ALL
SELECT 'CE_PRODUCTS', COUNT(*)
FROM bl_3nf.ce_products
WHERE product_id = -1
UNION ALL
SELECT 'CE_MARKETS',COUNT(*)
FROM bl_3nf.ce_markets
WHERE market_id = -1
UNION ALL
SELECT 'CE_REGIONS', COUNT(*)
FROM bl_3nf.ce_regions
WHERE region_id= -1
UNION ALL
SELECT 'CE_COUNTRIES',COUNT(*)
FROM bl_3nf.ce_countries
WHERE country_id = -1
UNION ALL
SELECT 'CE_STATES', COUNT(*)
FROM bl_3nf.ce_states
WHERE state_id = -1
UNION ALL
SELECT 'CE_CITIES', COUNT(*)
FROM bl_3nf.ce_cities
WHERE city_id = -1
UNION ALL
SELECT 'CE_CUSTOMERS_SCD', COUNT(*)
FROM bl_3nf.ce_customers_scd
WHERE customer_id = -1
UNION ALL
SELECT 'CE_EMPLOYEES', COUNT(*)
FROM bl_3nf.ce_employees
WHERE employee_id = -1
UNION ALL
SELECT 'CE_ORDER_ATTRIBUTES', COUNT(*)
FROM bl_3nf.ce_order_attributes
WHERE order_attr_id = -1;