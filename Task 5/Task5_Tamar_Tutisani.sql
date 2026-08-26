-- TASK 5: STAGING LAYER
-- Global Retail Superstore Sales | Tamar Tutisani
-- Schemas: SA_DOMESTIC, SA_INTERNATIONAL
-- Purpose: Create external and source tables, deduplicate raw data
-----------------------------------------------------------------------------
-- EXECUTION STEPS:
-- 1. Create file_fdw extension and file server (SUPERUSER REQUIRED)
-- 2. Create schemas (SA_DOMESTIC, SA_INTERNATIONAL)
-- 3. Create external tables (EXT_*) pointing to CSV files
-- 4. Create source tables (SRC_*) as regular PostgreSQL tables
-- 5. Load deduplicated data from EXT_ into SRC_ tables
-- 6. Run verification queries to show row counts and samples
-----------------------------------------------------------------------------



-- STEP 0: Enable file_fdw extension and create file server
-----------------------------------------------------------------------------

CREATE EXTENSION IF NOT EXISTS file_fdw;

CREATE SERVER IF NOT EXISTS sa_file_server
    FOREIGN DATA WRAPPER file_fdw;


-- STEP 1: Create schemas (one per source system)
-- Naming convention: sa_<source_system_name>
-----------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS sa_domestic;
CREATE SCHEMA IF NOT EXISTS sa_international;


-- STEP 2: Create EXTERNAL tables (EXT_)
-- These are foreign tables that read directly from the CSV files on disk.
-- Named EXT_<FILE_NAME> per naming conventions.
-- Both CSVs have a header row, delimiter is comma, encoding UTF8.
--
-----------------------------------------------------------------------------
-- SA_DOMESTIC.EXT_DOMESTIC_SALES
-- Source: SRC_DOMESTIC_SALES.csv
-- Columns: Row ID, Order ID, Order Date, Ship Date, Ship Mode,
--          Customer ID, Customer Name, Segment,
--          Country, City, Region, Market,
--          Product ID, Product Name, Category, Sub-Category,
--          Sales, Quantity, Discount, Profit, Shipping Cost,
--          Employee ID, Employee Name, Order Priority
-- All columns loaded as TEXT (no type conversion in external table)
-----------------------------------------------------------------------------


DROP FOREIGN TABLE IF EXISTS sa_domestic.ext_domestic_sales;

CREATE FOREIGN TABLE sa_domestic.ext_domestic_sales (
    category        TEXT,
    city            TEXT,
    country         TEXT,
    customer_id     TEXT,
    customer_name   TEXT,
    discount        TEXT,
    market          TEXT,
    order_date      TEXT,
    order_id        TEXT,
    order_priority  TEXT,
    product_id      TEXT,
    product_name    TEXT,
    profit          TEXT,
    quantity        TEXT,
    region          TEXT,
    row_id          TEXT,
    sales           TEXT,
    segment         TEXT,
    ship_date       TEXT,
    ship_mode       TEXT,
    shipping_cost   TEXT,
    sub_category    TEXT,
    cost            TEXT,
    employee_id     TEXT,
    employee_name   TEXT
)
SERVER sa_file_server
OPTIONS (
    filename  'SRC_DOMESTIC_SALES.csv',
    format    'csv',
    header    'true',
    delimiter ',',
    encoding  'UTF8'
);


-----------------------------------------------------------------------------
-- SA_INTERNATIONAL.EXT_INTERNATIONAL_SALES
-- Source: SRC_INTERNATIONAL_SALES.csv
-- Columns: Row ID, Order ID, Order Date, Ship Date,
--          Customer ID, Customer Name, Segment,
--          Country, State, City, Region, Market,
--          Product ID, Product Name, Category, Sub-Category,
--          Sales, Quantity, Discount, Profit, Shipping Cost,
--          Employee ID
-- (no Ship Mode, no Order Priority, no Employee Name - per dataset differences)
-- All columns loaded as TEXT (no type conversion in external table)
-----------------------------------------------------------------------------

DROP FOREIGN TABLE IF EXISTS sa_international.ext_international_sales;

CREATE FOREIGN TABLE sa_international.ext_international_sales (
    category        TEXT,
    city            TEXT,
    country         TEXT,
    customer_id     TEXT,
    customer_name   TEXT,
    discount        TEXT,
    market          TEXT,
    order_date      TEXT,
    order_id        TEXT,
    product_id      TEXT,
    product_name    TEXT,
    profit          TEXT,
    quantity        TEXT,
    region          TEXT,
    row_id          TEXT,
    sales           TEXT,
    segment         TEXT,
    ship_date       TEXT,
    shipping_cost   TEXT,
    state           TEXT,
    sub_category    TEXT,
    cost            TEXT,
    employee_id     TEXT
)
SERVER sa_file_server
OPTIONS (
    filename  'SRC_INTERNATIONAL_SALES.csv',
    format    'csv',
    header    'true',
    delimiter ',',
    encoding  'UTF8'
);


-- STEP 3: Create SOURCE tables (SRC_)
-- These are regular PostgreSQL tables. They hold a deduplicated, typed copy
-- of the raw data. Named SRC_<FILE_NAME> per naming conventions.
-- 
-- Data types are more specific than EXT_ tables:
-- - Dates are DATE (not TEXT)
-- - Numbers are NUMERIC or INTEGER
-- - Text is VARCHAR with appropriate length
-- 
-- Deduplication will be applied when inserting data (see STEP 4).
-- Deduplication key: order_id + product_id + customer_id + order_date
-- Only the first occurrence of each key combination is kept.
-----------------------------------------------------------------------------

-----------------------------------------------------------------------------
-- SA_DOMESTIC.SRC_DOMESTIC_SALES
-- Regular table with proper data types
-- NOT NULL constraints on business key columns only
-- Metrics (sales, profit, quantity) are nullable
-----------------------------------------------------------------------------

DROP TABLE IF EXISTS sa_domestic.src_domestic_sales;

CREATE TABLE sa_domestic.src_domestic_sales (
    row_id          VARCHAR(50),
    order_id        VARCHAR(50)     NOT NULL,
    order_date      DATE            NOT NULL,
    ship_date       DATE,
    ship_mode       VARCHAR(50),
    customer_id     VARCHAR(50)     NOT NULL,
    customer_name   VARCHAR(255),
    segment         VARCHAR(50),
    country         VARCHAR(100),
    city            VARCHAR(100),
    region          VARCHAR(100),
    market          VARCHAR(50),
    product_id      VARCHAR(50)     NOT NULL,
    product_name    VARCHAR(255),
    category        VARCHAR(100),
    sub_category    VARCHAR(100),
    sales           NUMERIC(15,4),
    quantity        INTEGER,
    discount        NUMERIC(5,4),
    profit          NUMERIC(15,4),
    shipping_cost   NUMERIC(15,4),
    cost            NUMERIC(15,4),
    employee_id     VARCHAR(50),
    employee_name   VARCHAR(255),
    order_priority  VARCHAR(25)
);


-----------------------------------------------------------------------------
-- SA_INTERNATIONAL.SRC_INTERNATIONAL_SALES
-- Regular table with proper data types
-- Note: No ship_mode, no order_priority, no employee_name columns
-----------------------------------------------------------------------------

DROP TABLE IF EXISTS sa_international.src_international_sales;

CREATE TABLE sa_international.src_international_sales (
    row_id          VARCHAR(50),
    order_id        VARCHAR(50)     NOT NULL,
    order_date      DATE            NOT NULL,
    ship_date       DATE,
    customer_id     VARCHAR(50)     NOT NULL,
    customer_name   VARCHAR(255),
    segment         VARCHAR(50),
    country         VARCHAR(100),
    state           VARCHAR(100),
    city            VARCHAR(100),
    region          VARCHAR(100),
    market          VARCHAR(50),
    product_id      VARCHAR(50)     NOT NULL,
    product_name    VARCHAR(255),
    category        VARCHAR(100),
    sub_category    VARCHAR(100),
    sales           NUMERIC(15,4),
    quantity        INTEGER,
    discount        NUMERIC(5,4),
    profit          NUMERIC(15,4),
    shipping_cost   NUMERIC(15,4),
    cost            NUMERIC(15,4),
    employee_id     VARCHAR(50)
);


-- STEP 4: Load deduplicated data from EXT_ into SRC_ tables
--
-- Deduplication approach:
-- Using ROW_NUMBER() window function to identify duplicate rows.
-- Partition by: order_id, product_id, customer_id, order_date
-- Order by: row_id (keep first occurrence)
-- Filter: WHERE rn = 1 (only first occurrence inserted)
-- 
-- This removes any duplicate rows that may exist in the raw CSV exports
-- where the same order line was exported more than once.
--
-- Data transformations applied:
-- - TEXT to DATE conversion (order_date, ship_date)
-- - TEXT to INTEGER conversion (row_id, quantity)
-- - TEXT to NUMERIC conversion (sales, discount, profit, shipping_cost)
-- - NULLIF(TRIM(...), '') to handle empty strings and whitespace as NULL
-----------------------------------------------------------------------------

-----------------------------------------------------------------------------
-- Load SA_DOMESTIC.SRC_DOMESTIC_SALES (deduplicated)
-----------------------------------------------------------------------------

INSERT INTO sa_domestic.src_domestic_sales (
    row_id, order_id, order_date, ship_date, ship_mode,
    customer_id, customer_name, segment,
    country, city, region, market,
    product_id, product_name, category, sub_category,
    sales, quantity, discount, profit, shipping_cost, cost,
    employee_id, employee_name, order_priority
)
SELECT
    row_id,
    order_id,
    order_date::DATE,
    NULLIF(TRIM(ship_date), '')::DATE,
    NULLIF(TRIM(ship_mode), ''),
    customer_id,
    NULLIF(TRIM(customer_name), ''),
    NULLIF(TRIM(segment), ''),
    NULLIF(TRIM(country), ''),
    NULLIF(TRIM(city), ''),
    NULLIF(TRIM(region), ''),
    NULLIF(TRIM(market), ''),
    product_id,
    NULLIF(TRIM(product_name), ''),
    NULLIF(TRIM(category), ''),
    NULLIF(TRIM(sub_category), ''),
    NULLIF(TRIM(sales), '')::NUMERIC(15,4),
    NULLIF(TRIM(quantity), '')::INTEGER,
    NULLIF(TRIM(discount), '')::NUMERIC(5,4),
    NULLIF(TRIM(profit), '')::NUMERIC(15,4),
    NULLIF(TRIM(shipping_cost), '')::NUMERIC(15,4),
    NULLIF(TRIM(cost), '')::NUMERIC(15,4),
    NULLIF(TRIM(employee_id), ''),
    NULLIF(TRIM(employee_name), ''),
    NULLIF(TRIM(order_priority), '')
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, product_id, customer_id, order_date
            ORDER BY row_id
        ) AS rn
    FROM sa_domestic.ext_domestic_sales
    WHERE order_id IS NOT NULL
      AND product_id IS NOT NULL
      AND customer_id IS NOT NULL
      AND order_date IS NOT NULL
) deduped
WHERE rn = 1;


-----------------------------------------------------------------------------
-- Load SA_INTERNATIONAL.SRC_INTERNATIONAL_SALES (deduplicated)
-----------------------------------------------------------------------------

INSERT INTO sa_international.src_international_sales (
    row_id, order_id, order_date, ship_date,
    customer_id, customer_name, segment,
    country, state, city, region, market,
    product_id, product_name, category, sub_category,
    sales, quantity, discount, profit, shipping_cost, cost,
    employee_id
)
SELECT
    row_id,
    order_id,
    order_date::DATE,
    NULLIF(TRIM(ship_date), '')::DATE,
    customer_id,
    NULLIF(TRIM(customer_name), ''),
    NULLIF(TRIM(segment), ''),
    NULLIF(TRIM(country), ''),
    NULLIF(TRIM(state), ''),
    NULLIF(TRIM(city), ''),
    NULLIF(TRIM(region), ''),
    NULLIF(TRIM(market), ''),
    product_id,
    NULLIF(TRIM(product_name), ''),
    NULLIF(TRIM(category), ''),
    NULLIF(TRIM(sub_category), ''),
    NULLIF(TRIM(sales), '')::NUMERIC(15,4),
    NULLIF(TRIM(quantity), '')::INTEGER,
    NULLIF(TRIM(discount), '')::NUMERIC(5,4),
    NULLIF(TRIM(profit), '')::NUMERIC(15,4),
    NULLIF(TRIM(shipping_cost), '')::NUMERIC(15,4),
    NULLIF(TRIM(cost), '')::NUMERIC(15,4),
    NULLIF(TRIM(employee_id), '')
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, product_id, customer_id, order_date
            ORDER BY row_id
        ) AS rn
    FROM sa_international.ext_international_sales
    WHERE order_id IS NOT NULL
      AND product_id IS NOT NULL
      AND customer_id IS NOT NULL
      AND order_date IS NOT NULL
) deduped
WHERE rn = 1;


-- STEP 5: Verification queries
-- Run these after loading to confirm the data looks correct and deduplication
-- was successful. These queries will be used to generate screenshots.
-- 
-- Expected output:
-- 1. Row counts for all 4 tables (EXT_* should have more rows than SRC_*)
-- 2. Number of duplicates removed from each dataset
-- 3. 5 sample rows from each table to verify data quality
-----------------------------------------------------------------------------

-- Query 5.1: Row counts: shows how many rows in each table
-- SRC_ count should be less than EXT_ count due to deduplication
SELECT 'SA_DOMESTIC.EXT_DOMESTIC_SALES'    AS table_name, COUNT(*) AS row_count FROM sa_domestic.ext_domestic_sales
UNION ALL
SELECT 'SA_DOMESTIC.SRC_DOMESTIC_SALES'    AS table_name, COUNT(*) AS row_count FROM sa_domestic.src_domestic_sales
UNION ALL
SELECT 'SA_INTERNATIONAL.EXT_INTERNATIONAL_SALES' AS table_name, COUNT(*) AS row_count FROM sa_international.ext_international_sales
UNION ALL
SELECT 'SA_INTERNATIONAL.SRC_INTERNATIONAL_SALES' AS table_name, COUNT(*) AS row_count FROM sa_international.src_international_sales;

-- Query 5.2: Duplicates removed count
-- Shows how many rows were removed as duplicates
SELECT
    (SELECT COUNT(*) FROM sa_domestic.ext_domestic_sales) -
    (SELECT COUNT(*) FROM sa_domestic.src_domestic_sales)
    AS domestic_duplicates_removed,
    (SELECT COUNT(*) FROM sa_international.ext_international_sales) -
    (SELECT COUNT(*) FROM sa_international.src_international_sales)
    AS international_duplicates_removed;

-- Query 5.3: Sample SELECT from EXT_DOMESTIC (5 rows)
SELECT * FROM sa_domestic.ext_domestic_sales LIMIT 5;

-- Query 5.4: Sample SELECT from SRC_DOMESTIC (5 rows)
SELECT * FROM sa_domestic.src_domestic_sales LIMIT 5;

-- Query 5.5: Sample SELECT from EXT_INTERNATIONAL (5 rows)
SELECT * FROM sa_international.ext_international_sales LIMIT 5;

-- Query 5.6: Sample SELECT from SRC_INTERNATIONAL (5 rows)
SELECT * FROM sa_international.src_international_sales LIMIT 5;


-- OPTIONAL: Create indexes on deduplication key for performance
-----------------------------------------------------------------------------

-- CREATE UNIQUE INDEX IF NOT EXISTS idx_src_domestic_dedup_key 
-- ON sa_domestic.src_domestic_sales(order_id, product_id, customer_id, order_date);

-- CREATE UNIQUE INDEX IF NOT EXISTS idx_src_international_dedup_key 
-- ON sa_international.src_international_sales(order_id, product_id, customer_id, order_date);