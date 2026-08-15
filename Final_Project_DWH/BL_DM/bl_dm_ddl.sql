-- BL_DM DDL: Dimensional Layer Tables, Sequences, Default Rows, DIM_TIME_DAY population
-- Global Retail Superstore Sales
-- Tamar Tutisani
--
-- DDL DESIGN NOTE:
-- Using CREATE TABLE IF NOT EXISTS instead of DROP TABLE IF EXISTS CASCADE.
-- This is safer for production: if the table already exists with data,
-- the script skips creation rather than destroying it.
-- To reset completely, run DROP SCHEMA bl_dm CASCADE and re-run this DDL.
-- Sequences use CREATE SEQUENCE IF NOT EXISTS for the same reason.
--
-- Convention for default rows:
--   Numeric IDs  : -1
--   Text fields  : COALESCE(NULL, 'n.a.') — explicit NULL handling per requirement
--   insert/update: '1900-01-01'
--   start_dt     : '1900-01-01'
--   end_dt       : '9999-12-31'
--   source_system: COALESCE(NULL, 'MANUAL')
--   source_entity: COALESCE(NULL, 'MANUAL')
-----------------------------------------------------------------------------


-- STEP 0: Create the BL_DM schema
-----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS bl_dm;


-- STEP 1: Create SEQUENCES for surrogate key generation
-- One sequence per dimension table. SEQUENCES only, never SERIAL type.
-- DIM_TIME_DAY uses YYYYMMDD integer keys, no sequence needed.
-----------------------------------------------------------------------------
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_product_surr_id START 100 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_customer_surr_id START 100 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_geography_surr_id START 100 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_employee_surr_id START 100 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_order_attr_surr_id START 100 INCREMENT 1;


-- STEP 2: Create DIM_ and FCT_ TABLES
-- All non-KPI columns are NOT NULL.
-- KPI/metric columns in the fact table are the ONLY nullable columns.
-----------------------------------------------------------------------------

-- DIM_TIME_DAY (SCD Type 0 - calendar dates never change)
-- Populated independently by a SQL date-generation script (STEP 3 below),
-- not sourced from the 3NF layer.
-- month_value/quarter_value are VARCHAR(5) to accommodate both real values
-- (2 chars) and the 'n.a.' default placeholder without truncation.
CREATE TABLE IF NOT EXISTS bl_dm.dim_time_day (
    date_id INTEGER NOT NULL PRIMARY KEY,
    date_dt DATE NOT NULL,
    day_of_week_no INTEGER NOT NULL CHECK (day_of_week_no BETWEEN -1 AND 7),
    day_of_week_desc VARCHAR(25) NOT NULL,
    weekend_flag INTEGER NOT NULL CHECK (weekend_flag IN (-1, 0, 1)),
    iso_week_no INTEGER NOT NULL,
    day_of_month_no INTEGER NOT NULL,
    month_value VARCHAR(5) NOT NULL,
    month_desc VARCHAR(25) NOT NULL,
    quarter_value VARCHAR(5) NOT NULL,
    quarter_desc VARCHAR(25) NOT NULL,
    year_value VARCHAR(4) NOT NULL,
    insert_dt DATE NOT NULL,
    update_dt DATE NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_time_day_date_dt
    ON bl_dm.dim_time_day(date_dt);


-- DIM_PRODUCTS (SCD Type 1 - hierarchy flattened: Category > Sub-Category > Product)
CREATE TABLE IF NOT EXISTS bl_dm.dim_products (
    product_surr_id BIGINT NOT NULL PRIMARY KEY,
    product_src_id VARCHAR(50) NOT NULL UNIQUE,
    product_name VARCHAR(255) NOT NULL,
    product_category_id BIGINT NOT NULL,
    product_category_name VARCHAR(100) NOT NULL,
    product_subcategory_id BIGINT NOT NULL,
    product_subcategory_name VARCHAR(100) NOT NULL,
    insert_dt DATE NOT NULL,
    update_dt DATE NOT NULL,
    source_system VARCHAR(100) NOT NULL,
    source_entity VARCHAR(100) NOT NULL
);


-- DIM_CUSTOMERS_SCD (SCD Type 2)
-- Tracks history of customer_name and customer_segment changes.
-- START_DT/END_DT/IS_ACTIVE reflect this dimension's own history,
-- calculated independently at each layer.
-- No FK from FCT_SALES_DD: FK constraints to SCD2 tables are prohibited
-- per naming conventions. Enforced at ETL level only.
CREATE TABLE IF NOT EXISTS bl_dm.dim_customers_scd (
    customer_surr_id BIGINT NOT NULL,
    customer_src_id VARCHAR(100) NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    customer_segment VARCHAR(50) NOT NULL,
    start_dt DATE NOT NULL,
    end_dt DATE NOT NULL,
    is_active VARCHAR(1) NOT NULL CHECK (is_active IN ('Y', 'N')),
    insert_dt DATE NOT NULL,
    update_dt DATE NOT NULL,
    source_system VARCHAR(100) NOT NULL,
    source_entity VARCHAR(100) NOT NULL,
    PRIMARY KEY (customer_surr_id, start_dt),
    CHECK (end_dt > start_dt)
);

CREATE INDEX IF NOT EXISTS idx_dim_customers_scd_src_id
    ON bl_dm.dim_customers_scd(customer_src_id);
CREATE INDEX IF NOT EXISTS idx_dim_customers_scd_active
    ON bl_dm.dim_customers_scd(is_active, end_dt);


-- DIM_GEOGRAPHY (SCD Type 1 - hierarchy flattened: Market > Region > Country > State > City)
CREATE TABLE IF NOT EXISTS bl_dm.dim_geography (
    geography_surr_id BIGINT NOT NULL PRIMARY KEY,
    geography_src_id VARCHAR(255) NOT NULL UNIQUE,
    city_name VARCHAR(100) NOT NULL,
    state_name VARCHAR(100) NOT NULL,
    country_name VARCHAR(100) NOT NULL,
    region_name VARCHAR(100) NOT NULL,
    market_name VARCHAR(50) NOT NULL,
    insert_dt DATE NOT NULL,
    update_dt DATE NOT NULL,
    source_system VARCHAR(100) NOT NULL,
    source_entity VARCHAR(100) NOT NULL
);


-- DIM_EMPLOYEES (SCD Type 1)
-- Employee Name = 'n.a.' when only sourced from International system.
CREATE TABLE IF NOT EXISTS bl_dm.dim_employees (
    employee_surr_id BIGINT NOT NULL PRIMARY KEY,
    employee_src_id VARCHAR(50) NOT NULL UNIQUE,
    employee_name VARCHAR(255) NOT NULL,
    insert_dt DATE NOT NULL,
    update_dt DATE NOT NULL,
    source_system VARCHAR(100) NOT NULL,
    source_entity VARCHAR(100) NOT NULL
);


-- DIM_ORDER_ATTRIBUTES (SCD Type 0 - junk dimension: Ship Mode x Order Priority)
-- International fact rows reference the default (-1) row.
CREATE TABLE IF NOT EXISTS bl_dm.dim_order_attributes (
    order_attr_surr_id BIGINT NOT NULL PRIMARY KEY,
    ship_mode VARCHAR(50) NOT NULL,
    order_priority VARCHAR(25) NOT NULL,
    insert_dt DATE NOT NULL,
    update_dt DATE NOT NULL,
    source_system VARCHAR(100) NOT NULL,
    source_entity VARCHAR(100) NOT NULL,
    UNIQUE (ship_mode, order_priority)
);


-- FCT_SALES_DD (transaction grain, daily, partitioned by event_dt month)
-- ORDER_ID is a degenerate dimension, not a FK.
-- No FK to DIM_CUSTOMERS_SCD (SCD2 - prohibited per naming conventions).
-- KPI columns are the ONLY nullable columns.
-- Partitioned by RANGE on event_dt, one partition per month.
-- Partitions are created and attached by the rolling-window load procedure.
--
-- POSTGRESQL PARTITIONING NOTE:
-- FK constraints declared on a partitioned parent table are NOT enforced
-- on child partitions by PostgreSQL (as of PG 14+). The constraints are
-- syntactically valid and serve as documentation of referential intent,
-- but violation checking does not propagate to individual partitions.
-- Referential integrity between FCT_SALES_DD and dimension tables is
-- therefore enforced at the ETL level (COALESCE to default -1 row, LEFT JOINs)
-- rather than at the database constraint level.
CREATE TABLE IF NOT EXISTS bl_dm.fct_sales_dd (
    event_dt DATE NOT NULL,
    date_id INTEGER NOT NULL,
    product_surr_id BIGINT NOT NULL,
    customer_surr_id BIGINT NOT NULL,
    geography_surr_id BIGINT NOT NULL,
    employee_surr_id BIGINT NOT NULL,
    order_attr_surr_id BIGINT NOT NULL,
    order_id VARCHAR(50) NOT NULL,
    sales_amt NUMERIC(15,2),
    cost_amt NUMERIC(15,2),
    profit_amt NUMERIC(15,2),
    shipping_cost_amt NUMERIC(15,2),
    quantity_cnt NUMERIC(10,0),
    discount_amt NUMERIC(5,4),
    profit_margin_amt NUMERIC(10,4),
    insert_dt DATE NOT NULL,
    update_dt DATE NOT NULL,
    source_system VARCHAR(100) NOT NULL,
    source_entity VARCHAR(100) NOT NULL,
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
)
PARTITION BY RANGE (event_dt);

CREATE INDEX IF NOT EXISTS idx_fct_sales_dd_event_dt ON bl_dm.fct_sales_dd(event_dt);
CREATE INDEX IF NOT EXISTS idx_fct_sales_dd_product_surr_id ON bl_dm.fct_sales_dd(product_surr_id);
CREATE INDEX IF NOT EXISTS idx_fct_sales_dd_customer_surr_id ON bl_dm.fct_sales_dd(customer_surr_id);
CREATE INDEX IF NOT EXISTS idx_fct_sales_dd_geography_surr_id ON bl_dm.fct_sales_dd(geography_surr_id);
CREATE INDEX IF NOT EXISTS idx_fct_sales_dd_employee_surr_id ON bl_dm.fct_sales_dd(employee_surr_id);
CREATE INDEX IF NOT EXISTS idx_fct_sales_dd_order_id ON bl_dm.fct_sales_dd(order_id);


-- STEP 3: Populate DIM_TIME_DAY for full 2024-2025 range
-- Same generation logic as BL_3NF.CE_DATES.
-- Populated independently — calendar dimension is never sourced from 3NF.
-- ON CONFLICT DO NOTHING = safe to re-run.
-----------------------------------------------------------------------------
INSERT INTO bl_dm.dim_time_day (
    date_id, date_dt, day_of_week_no, day_of_week_desc, weekend_flag,
    iso_week_no, day_of_month_no, month_value, month_desc,
    quarter_value, quarter_desc, year_value, insert_dt, update_dt
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER AS date_id,
    d AS date_dt,
    EXTRACT(ISODOW FROM d)::INTEGER AS day_of_week_no,
    TRIM(TO_CHAR(d, 'Day')) AS day_of_week_desc,
    CASE WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN 1 ELSE 0 END AS weekend_flag,
    EXTRACT(WEEK FROM d)::INTEGER AS iso_week_no,
    EXTRACT(DAY FROM d)::INTEGER AS day_of_month_no,
    TO_CHAR(d, 'MM') AS month_value,
    TRIM(TO_CHAR(d, 'Month')) AS month_desc,
    EXTRACT(QUARTER FROM d)::TEXT AS quarter_value,
    'Q' || EXTRACT(QUARTER FROM d)::INTEGER AS quarter_desc,
    TO_CHAR(d, 'YYYY') AS year_value,
    CURRENT_DATE AS insert_dt,
    CURRENT_DATE AS update_dt
FROM GENERATE_SERIES('2024-01-01'::DATE, '2025-12-31'::DATE, '1 day'::INTERVAL) AS d
ON CONFLICT (date_id) DO NOTHING;

COMMIT;


-- STEP 4: Insert default (-1) rows into all dimension tables
-- COALESCE(NULL, 'n.a.') used explicitly on all text fields to demonstrate
-- NULL handling per naming conventions and mentor requirement.
-- ON CONFLICT DO NOTHING = safe to re-run.
-----------------------------------------------------------------------------

-- DIM_TIME_DAY default row
INSERT INTO bl_dm.dim_time_day (
    date_id, date_dt, day_of_week_no, day_of_week_desc, weekend_flag,
    iso_week_no, day_of_month_no, month_value, month_desc,
    quarter_value, quarter_desc, year_value, insert_dt, update_dt
)
VALUES (
    -1,
    '1900-01-01'::DATE,
    -1,
    COALESCE(NULL, 'n.a.'),
    -1, -1, -1,
    COALESCE(NULL, 'n.a.'),
    COALESCE(NULL, 'n.a.'),
    COALESCE(NULL, 'n.a.'),
    COALESCE(NULL, 'n.a.'),
    COALESCE(NULL, 'n.a.'),
    '1900-01-01'::DATE,
    '1900-01-01'::DATE
)
ON CONFLICT (date_id) DO NOTHING;
COMMIT;

-- DIM_PRODUCTS default row
INSERT INTO bl_dm.dim_products (
    product_surr_id, product_src_id, product_name,
    product_category_id, product_category_name,
    product_subcategory_id, product_subcategory_name,
    insert_dt, update_dt, source_system, source_entity
)
VALUES (
    -1,
    COALESCE(NULL, 'n.a.'),
    COALESCE(NULL, 'n.a.'),
    -1,
    COALESCE(NULL, 'n.a.'),
    -1,
    COALESCE(NULL, 'n.a.'),
    '1900-01-01'::DATE,
    '1900-01-01'::DATE,
    COALESCE(NULL, 'MANUAL'),
    COALESCE(NULL, 'MANUAL')
)
ON CONFLICT (product_surr_id) DO NOTHING;
COMMIT;

-- DIM_CUSTOMERS_SCD default row (SCD Type 2)
INSERT INTO bl_dm.dim_customers_scd (
    customer_surr_id, customer_src_id, customer_name, customer_segment,
    start_dt, end_dt, is_active, insert_dt, update_dt,
    source_system, source_entity
)
VALUES (
    -1,
    COALESCE(NULL, 'n.a.'),
    COALESCE(NULL, 'n.a.'),
    COALESCE(NULL, 'n.a.'),
    '1900-01-01'::DATE,
    '9999-12-31'::DATE,
    'Y',
    '1900-01-01'::DATE,
    '1900-01-01'::DATE,
    COALESCE(NULL, 'MANUAL'),
    COALESCE(NULL, 'MANUAL')
)
ON CONFLICT (customer_surr_id, start_dt) DO NOTHING;
COMMIT;

-- DIM_GEOGRAPHY default row
INSERT INTO bl_dm.dim_geography (
    geography_surr_id, geography_src_id, city_name, state_name,
    country_name, region_name, market_name,
    insert_dt, update_dt, source_system, source_entity
)
VALUES (
    -1,
    COALESCE(NULL, 'n.a.'),
    COALESCE(NULL, 'n.a.'),
    COALESCE(NULL, 'n.a.'),
    COALESCE(NULL, 'n.a.'),
    COALESCE(NULL, 'n.a.'),
    COALESCE(NULL, 'n.a.'),
    '1900-01-01'::DATE,
    '1900-01-01'::DATE,
    COALESCE(NULL, 'MANUAL'),
    COALESCE(NULL, 'MANUAL')
)
ON CONFLICT (geography_surr_id) DO NOTHING;
COMMIT;

-- DIM_EMPLOYEES default row
INSERT INTO bl_dm.dim_employees (
    employee_surr_id, employee_src_id, employee_name,
    insert_dt, update_dt, source_system, source_entity
)
VALUES (
    -1,
    COALESCE(NULL, 'n.a.'),
    COALESCE(NULL, 'n.a.'),
    '1900-01-01'::DATE,
    '1900-01-01'::DATE,
    COALESCE(NULL, 'MANUAL'),
    COALESCE(NULL, 'MANUAL')
)
ON CONFLICT (employee_surr_id) DO NOTHING;
COMMIT;

-- DIM_ORDER_ATTRIBUTES default row (junk dimension, SCD Type 0)
INSERT INTO bl_dm.dim_order_attributes (
    order_attr_surr_id, ship_mode, order_priority,
    insert_dt, update_dt, source_system, source_entity
)
VALUES (
    -1,
    COALESCE(NULL, 'n.a.'),
    COALESCE(NULL, 'n.a.'),
    '1900-01-01'::DATE,
    '1900-01-01'::DATE,
    COALESCE(NULL, 'MANUAL'),
    COALESCE(NULL, 'MANUAL')
)
ON CONFLICT (order_attr_surr_id) DO NOTHING;
COMMIT;


-- STEP 5: Verify default rows and table structure
-----------------------------------------------------------------------------
SELECT 
    'DIM_TIME_DAY' AS table_name, 
    COUNT(*) AS default_row_count 
FROM bl_dm.dim_time_day 
WHERE date_id = -1
UNION ALL
SELECT 
    'DIM_PRODUCTS', 
    COUNT(*) 
FROM bl_dm.dim_products 
WHERE product_surr_id = -1
UNION ALL
SELECT 
    'DIM_CUSTOMERS_SCD', 
    COUNT(*) 
FROM bl_dm.dim_customers_scd 
WHERE customer_surr_id = -1
UNION ALL
SELECT 
    'DIM_GEOGRAPHY', 
    COUNT(*) 
FROM bl_dm.dim_geography 
WHERE geography_surr_id = -1
UNION ALL
SELECT 
    'DIM_EMPLOYEES', 
    COUNT(*) 
FROM bl_dm.dim_employees 
WHERE employee_surr_id = -1
UNION ALL
SELECT 
    'DIM_ORDER_ATTRIBUTES', 
    COUNT(*) 
FROM bl_dm.dim_order_attributes 
WHERE order_attr_surr_id = -1;