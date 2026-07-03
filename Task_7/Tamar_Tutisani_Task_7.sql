-- TASK 7: DDL SCRIPT FOR DM (DIMENSIONAL) LAYER (INCLUDES DEFAULT ROWS)
-- Global Retail Superstore Sales
-- Tamar Tutisani
-- Schema: BL_DM
-- 
-- Convention for default rows:
--   Numeric IDs (surrogate) : -1
--   Text fields             : 'n.a.'  (never truncated - see STEP 5 note)
--   insert/update dates     : '1900-01-01'
--   start_dt                : '1900-01-01'
--   end_dt                  : '9999-12-31'
--   source_system           : 'MANUAL'
--   source_entity           : 'MANUAL'
-----------------------------------------------------------------------------


-- STEP 0: Create the BL_DM schema
-----------------------------------------------------------------------------

BEGIN;

CREATE SCHEMA IF NOT EXISTS bl_dm;

COMMIT;


-- STEP 1: Create SEQUENCES for surrogate key generation
-- One sequence per dimension/fact table. SEQUENCES only, never SERIAL.
-- DIM_TIME_DAY uses YYYYMMDD integer surrogate keys (no sequence needed,
-- same pattern as BL_3NF.CE_DATES) - dates are a natural, stable key here.
-----------------------------------------------------------------------------

BEGIN;

CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_product_surr_id      START 100 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_customer_surr_id     START 100 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_geography_surr_id    START 100 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_employee_surr_id     START 100 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_order_attr_surr_id   START 100 INCREMENT 1;

COMMIT;


-- STEP 2: Create DIM_ / FCT_ TABLES
-- Naming convention:
--   Dimensions SCD Type 0/1 : DIM_<Name in Plural>
--   Dimensions SCD Type 2   : DIM_<Name in Plural>_SCD
--   Calendar dimension      : DIM_TIME_<Level>
--   Fact tables             : FCT_<Name in Plural>_<Level>
-- All non-KPI columns are NOT NULL. KPI/metric columns in the fact table
-- are the ONLY nullable columns.
-----------------------------------------------------------------------------

BEGIN;

-- DIM_TIME_DAY (SCD Type 0 - calendar dates never change)
-- Populated independently via a SQL date-generation script (not from 3NF),
-- per Business Template design decisions (section 3.1).
-- month_value/quarter_value widened enough to hold real generated values
-- AND the full honest 'n.a.' placeholder without truncation (see STEP 5).
DROP TABLE IF EXISTS bl_dm.dim_time_day CASCADE;

CREATE TABLE bl_dm.dim_time_day (
    date_id          INTEGER      NOT NULL PRIMARY KEY,
    date_dt          DATE         NOT NULL,
    day_of_week_no   INTEGER      NOT NULL CHECK (day_of_week_no BETWEEN -1 AND 7),
    day_of_week_desc VARCHAR(25)  NOT NULL,
    weekend_flag     INTEGER      NOT NULL CHECK (weekend_flag IN (-1, 0, 1)),
    iso_week_no      INTEGER      NOT NULL,
    day_of_month_no  INTEGER      NOT NULL,
    month_value      VARCHAR(5)   NOT NULL,
    month_desc       VARCHAR(25)  NOT NULL,
    quarter_value    VARCHAR(5)   NOT NULL,
    quarter_desc     VARCHAR(5)   NOT NULL,
    year_value       VARCHAR(4)   NOT NULL,
    insert_dt        DATE         NOT NULL,
    update_dt        DATE         NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_time_day_date_dt
    ON bl_dm.dim_time_day(date_dt);


-- DIM_PRODUCTS (SCD Type 1 - hierarchy flattened: Category > Sub-Category > Product)
-- Overwrite on change. If a category is renamed upstream, this dimension is
-- simply updated in place (UPDATE_DT refreshed).
DROP TABLE IF EXISTS bl_dm.dim_products CASCADE;

CREATE TABLE bl_dm.dim_products (
    product_surr_id          BIGINT       NOT NULL PRIMARY KEY,
    product_src_id           VARCHAR(50)  NOT NULL UNIQUE,
    product_name             VARCHAR(255) NOT NULL,
    product_category_id      BIGINT       NOT NULL,
    product_category_name    VARCHAR(100) NOT NULL,
    product_subcategory_id   BIGINT       NOT NULL,
    product_subcategory_name VARCHAR(100) NOT NULL,
    insert_dt                DATE         NOT NULL,
    update_dt                DATE         NOT NULL,
    source_system            VARCHAR(100) NOT NULL,
    source_entity            VARCHAR(100) NOT NULL
);


-- DIM_CUSTOMERS_SCD (SCD Type 2)
-- Versions on any change to customer_name or customer_segment (not segment
-- only) - see load procedure for the comparison logic.
-- START_DT/END_DT/IS_ACTIVE reflect this dimension's own history, not
-- copied from CE_CUSTOMERS_SCD.
-- No FK from FCT_SALES_DD: FK constraints to SCD2 tables are prohibited
-- (Naming_Conventions.docx). Enforced at ETL level only.

DROP TABLE IF EXISTS bl_dm.dim_customers_scd CASCADE;

CREATE TABLE bl_dm.dim_customers_scd (
    customer_surr_id  BIGINT       NOT NULL,
    customer_src_id   VARCHAR(100) NOT NULL,
    customer_name     VARCHAR(255) NOT NULL,
    customer_segment  VARCHAR(50)  NOT NULL,
    start_dt          DATE         NOT NULL,
    end_dt            DATE         NOT NULL,
    is_active         VARCHAR(1)   NOT NULL CHECK (is_active IN ('Y', 'N')),
    insert_dt         DATE         NOT NULL,
    update_dt         DATE         NOT NULL,
    source_system     VARCHAR(100) NOT NULL,
    source_entity     VARCHAR(100) NOT NULL,
    PRIMARY KEY (customer_surr_id, start_dt),
    CHECK (end_dt > start_dt)
);

CREATE INDEX IF NOT EXISTS idx_dim_customers_scd_src_id
    ON bl_dm.dim_customers_scd(customer_src_id);
CREATE INDEX IF NOT EXISTS idx_dim_customers_scd_active
    ON bl_dm.dim_customers_scd(is_active, end_dt);


-- DIM_GEOGRAPHY (SCD Type 1 - hierarchy flattened: Market > Region > Country > State > City)
DROP TABLE IF EXISTS bl_dm.dim_geography CASCADE;

CREATE TABLE bl_dm.dim_geography (
    geography_surr_id BIGINT       NOT NULL PRIMARY KEY,
    geography_src_id  VARCHAR(255) NOT NULL UNIQUE,
    city_name         VARCHAR(100) NOT NULL,
    state_name        VARCHAR(100) NOT NULL,
    country_name      VARCHAR(100) NOT NULL,
    region_name       VARCHAR(100) NOT NULL,
    market_name       VARCHAR(50)  NOT NULL,
    insert_dt         DATE         NOT NULL,
    update_dt         DATE         NOT NULL,
    source_system     VARCHAR(100) NOT NULL,
    source_entity     VARCHAR(100) NOT NULL
);


-- DIM_EMPLOYEES (SCD Type 1)
-- Employee Name = 'n.a.' when only sourced from International system.
DROP TABLE IF EXISTS bl_dm.dim_employees CASCADE;

CREATE TABLE bl_dm.dim_employees (
    employee_surr_id BIGINT       NOT NULL PRIMARY KEY,
    employee_src_id  VARCHAR(50)  NOT NULL UNIQUE,
    employee_name    VARCHAR(255) NOT NULL,
    insert_dt        DATE         NOT NULL,
    update_dt        DATE         NOT NULL,
    source_system    VARCHAR(100) NOT NULL,
    source_entity    VARCHAR(100) NOT NULL
);


-- DIM_ORDER_ATTRIBUTES (SCD Type 0 - junk dimension: Ship Mode x Order Priority)
-- International fact rows reference the default (-1) row, since Ship Mode
-- and Order Priority are not tracked in that source system.
DROP TABLE IF EXISTS bl_dm.dim_order_attributes CASCADE;

CREATE TABLE bl_dm.dim_order_attributes (
    order_attr_surr_id BIGINT       NOT NULL PRIMARY KEY,
    ship_mode          VARCHAR(50)  NOT NULL,
    order_priority     VARCHAR(25)  NOT NULL,
    insert_dt          DATE         NOT NULL,
    update_dt          DATE         NOT NULL,
    source_system      VARCHAR(100) NOT NULL,
    source_entity      VARCHAR(100) NOT NULL,
    UNIQUE (ship_mode, order_priority)
);


-- FCT_SALES_DD (transaction grain, daily)
-- ORDER_ID is a degenerate dimension, not a FK.
-- No FK to DIM_CUSTOMERS_SCD (SCD2 - see rule above).
-- KPI columns are the only nullable columns.
DROP TABLE IF EXISTS bl_dm.fct_sales_dd CASCADE;

CREATE TABLE bl_dm.fct_sales_dd (
    event_dt           DATE         NOT NULL,
    date_id            INTEGER      NOT NULL,
    product_surr_id    BIGINT       NOT NULL,
    customer_surr_id   BIGINT       NOT NULL,
    geography_surr_id  BIGINT       NOT NULL,
    employee_surr_id   BIGINT       NOT NULL,
    order_attr_surr_id BIGINT       NOT NULL,
    order_id           VARCHAR(50)  NOT NULL,
    sales_amt          NUMERIC(15,2),
    cost_amt           NUMERIC(15,2),
    profit_amt         NUMERIC(15,2),
    shipping_cost_amt  NUMERIC(15,2),
    quantity_cnt       NUMERIC(10,0),
    discount_amt       NUMERIC(5,4),
    profit_margin_amt  NUMERIC(10,4),
    insert_dt          DATE         NOT NULL,
    update_dt          DATE         NOT NULL,
    source_system      VARCHAR(100) NOT NULL,
    source_entity      VARCHAR(100) NOT NULL,
    CONSTRAINT fk_fct_sales_to_time
        FOREIGN KEY (date_id)
        REFERENCES bl_dm.dim_time_day(date_id),
    CONSTRAINT fk_fct_sales_to_product
        FOREIGN KEY (product_surr_id)
        REFERENCES bl_dm.dim_products(product_surr_id),
    CONSTRAINT fk_fct_sales_to_geography
        FOREIGN KEY (geography_surr_id)
        REFERENCES bl_dm.dim_geography(geography_surr_id),
    CONSTRAINT fk_fct_sales_to_employee
        FOREIGN KEY (employee_surr_id)
        REFERENCES bl_dm.dim_employees(employee_surr_id),
    CONSTRAINT fk_fct_sales_to_order_attr
        FOREIGN KEY (order_attr_surr_id)
        REFERENCES bl_dm.dim_order_attributes(order_attr_surr_id)
    -- No FK to DIM_CUSTOMERS_SCD: SCD2 table has composite PK,
    -- relationship enforced at ETL level only.
);

CREATE INDEX IF NOT EXISTS idx_fct_sales_dd_event_dt          ON bl_dm.fct_sales_dd(event_dt);
CREATE INDEX IF NOT EXISTS idx_fct_sales_dd_product_surr_id   ON bl_dm.fct_sales_dd(product_surr_id);
CREATE INDEX IF NOT EXISTS idx_fct_sales_dd_customer_surr_id  ON bl_dm.fct_sales_dd(customer_surr_id);
CREATE INDEX IF NOT EXISTS idx_fct_sales_dd_geography_surr_id ON bl_dm.fct_sales_dd(geography_surr_id);
CREATE INDEX IF NOT EXISTS idx_fct_sales_dd_employee_surr_id  ON bl_dm.fct_sales_dd(employee_surr_id);
CREATE INDEX IF NOT EXISTS idx_fct_sales_dd_order_id          ON bl_dm.fct_sales_dd(order_id);

COMMIT;


-- STEP 3: Populate DIM_TIME_DAY for full 2024-2025 range
-- Same generation logic as BL_3NF.CE_DATES, kept independent per design
-- decision (calendar dimension is populated by a standalone script, not
-- denormalized from 3NF).
-----------------------------------------------------------------------------

BEGIN;

INSERT INTO bl_dm.dim_time_day (
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
    EXTRACT(QUARTER FROM d)::TEXT AS quarter_value,
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
WHERE table_schema = 'bl_dm'
ORDER BY table_name;


-- STEP 5: Insert default (-1) rows into all dimension tables
-- Required in every dimension table except the fact table, so fact rows
-- with a missing/unresolvable dimension value never fail an FK constraint.
-- ON CONFLICT DO NOTHING makes this block safe to re-run.
-----------------------------------------------------------------------------

BEGIN;

-- DIM_TIME_DAY default row
INSERT INTO bl_dm.dim_time_day (
    date_id, date_dt, day_of_week_no, day_of_week_desc, weekend_flag,
    iso_week_no, day_of_month_no, month_value, month_desc,
    quarter_value, quarter_desc, year_value, insert_dt, update_dt
)
VALUES (
    -1, '1900-01-01', -1, 'n.a.', -1, -1, -1, 'n.a.', 'n.a.', 'n.a.', 'n.a.',
    'n.a.', '1900-01-01', '1900-01-01'
)
ON CONFLICT (date_id) DO NOTHING;

COMMIT;


BEGIN;

-- DIM_PRODUCTS default row
INSERT INTO bl_dm.dim_products (
    product_surr_id, product_src_id, product_name,
    product_category_id, product_category_name,
    product_subcategory_id, product_subcategory_name,
    insert_dt, update_dt, source_system, source_entity
)
VALUES (
    -1, 'n.a.', 'n.a.', -1, 'n.a.', -1, 'n.a.',
    '1900-01-01', '1900-01-01', 'MANUAL', 'MANUAL'
)
ON CONFLICT (product_surr_id) DO NOTHING;

COMMIT;


BEGIN;

-- DIM_CUSTOMERS_SCD default row (SCD Type 2)
INSERT INTO bl_dm.dim_customers_scd (
    customer_surr_id, customer_src_id, customer_name, customer_segment,
    start_dt, end_dt, is_active, insert_dt, update_dt,
    source_system, source_entity
)
VALUES (
    -1, 'n.a.', 'n.a.', 'n.a.',
    '1900-01-01', '9999-12-31', 'Y', '1900-01-01', '1900-01-01',
    'MANUAL', 'MANUAL'
)
ON CONFLICT (customer_surr_id, start_dt) DO NOTHING;

COMMIT;


BEGIN;

-- DIM_GEOGRAPHY default row
INSERT INTO bl_dm.dim_geography (
    geography_surr_id, geography_src_id, city_name, state_name,
    country_name, region_name, market_name,
    insert_dt, update_dt, source_system, source_entity
)
VALUES (
    -1, 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.',
    '1900-01-01', '1900-01-01', 'MANUAL', 'MANUAL'
)
ON CONFLICT (geography_surr_id) DO NOTHING;

COMMIT;


BEGIN;

-- DIM_EMPLOYEES default row
INSERT INTO bl_dm.dim_employees (
    employee_surr_id, employee_src_id, employee_name,
    insert_dt, update_dt, source_system, source_entity
)
VALUES (
    -1, 'n.a.', 'n.a.', '1900-01-01', '1900-01-01', 'MANUAL', 'MANUAL'
)
ON CONFLICT (employee_surr_id) DO NOTHING;

COMMIT;


BEGIN;

-- DIM_ORDER_ATTRIBUTES default row (junk dimension, SCD Type 0)
INSERT INTO bl_dm.dim_order_attributes (
    order_attr_surr_id, ship_mode, order_priority,
    insert_dt, update_dt, source_system, source_entity
)
VALUES (
    -1, 'n.a.', 'n.a.', '1900-01-01', '1900-01-01', 'MANUAL', 'MANUAL'
)
ON CONFLICT (order_attr_surr_id) DO NOTHING;

COMMIT;


-- STEP 6: Verify default rows are present in all dimension tables
-- Every table below must return count = 1
-----------------------------------------------------------------------------

SELECT 'DIM_TIME_DAY' AS table_name, COUNT(*) AS default_row_count
FROM bl_dm.dim_time_day
WHERE date_id = -1
UNION ALL
SELECT 'DIM_PRODUCTS', COUNT(*)
FROM bl_dm.dim_products
WHERE product_surr_id = -1
UNION ALL
SELECT 'DIM_CUSTOMERS_SCD', COUNT(*)
FROM bl_dm.dim_customers_scd
WHERE customer_surr_id = -1
UNION ALL
SELECT 'DIM_GEOGRAPHY', COUNT(*)
FROM bl_dm.dim_geography
WHERE geography_surr_id = -1
UNION ALL
SELECT 'DIM_EMPLOYEES', COUNT(*)
FROM bl_dm.dim_employees
WHERE employee_surr_id = -1
UNION ALL
SELECT 'DIM_ORDER_ATTRIBUTES', COUNT(*)
FROM bl_dm.dim_order_attributes
WHERE order_attr_surr_id = -1;