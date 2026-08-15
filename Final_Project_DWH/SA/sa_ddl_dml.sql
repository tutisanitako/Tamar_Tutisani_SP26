-- STAGING LAYER: SA_DOMESTIC and SA_INTERNATIONAL
-- Global Retail Superstore Sales
-- Tamar Tutisani
-- Creates schemas, external tables (EXT_), source tables (SRC_),
-- and loads deduplicated data from EXT_ into SRC_.
-----------------------------------------------------------------------------
-- EXECUTION STEPS:
-- 1. Create file_fdw extension and file server (SUPERUSER REQUIRED)
-- 2. Create schemas (SA_DOMESTIC, SA_INTERNATIONAL)
-- 3. Create external tables (EXT_*) pointing to CSV files
-- 4. Create source tables (SRC_*) as regular PostgreSQL tables
-- 5. Load deduplicated data from EXT_ into SRC_ tables
-- 6. Run verification queries
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
-- All columns loaded as TEXT (no type conversion in external table).
-----------------------------------------------------------------------------

-- SA_DOMESTIC.EXT_DOMESTIC_SALES
DROP FOREIGN TABLE IF EXISTS sa_domestic.ext_domestic_sales;

CREATE FOREIGN TABLE sa_domestic.ext_domestic_sales (
    category TEXT,
    city TEXT,
    country TEXT,
    customer_id TEXT,
    customer_name TEXT,
    discount TEXT,
    market TEXT,
    order_date TEXT,
    order_id TEXT,
    order_priority TEXT,
    product_id TEXT,
    product_name TEXT,
    profit TEXT,
    quantity TEXT,
    region TEXT,
    row_id TEXT,
    sales TEXT,
    segment TEXT,
    ship_date TEXT,
    ship_mode TEXT,
    shipping_cost TEXT,
    sub_category TEXT,
    cost TEXT,
    employee_id TEXT,
    employee_name TEXT
)
SERVER sa_file_server
OPTIONS (
    filename 'SRC_DOMESTIC_SALES_5PCT.csv',
    format 'csv',
    header 'true',
    delimiter ',',
    encoding 'UTF8'
);

-- SA_INTERNATIONAL.EXT_INTERNATIONAL_SALES
-- Note: No ship_mode, no order_priority, no employee_name columns
DROP FOREIGN TABLE IF EXISTS sa_international.ext_international_sales;

CREATE FOREIGN TABLE sa_international.ext_international_sales (
    category TEXT,
    city TEXT,
    country TEXT,
    customer_id TEXT,
    customer_name TEXT,
    discount TEXT,
    market TEXT,
    order_date TEXT,
    order_id TEXT,
    product_id TEXT,
    product_name TEXT,
    profit TEXT,
    quantity TEXT,
    region TEXT,
    row_id TEXT,
    sales TEXT,
    segment TEXT,
    ship_date TEXT,
    shipping_cost TEXT,
    state TEXT,
    sub_category TEXT,
    cost TEXT,
    employee_id TEXT
)
SERVER sa_file_server
OPTIONS (
    filename 'SRC_INTERNATIONAL_SALES_5PCT.csv',
    format 'csv',
    header 'true',
    delimiter ',',
    encoding 'UTF8'
);

-- STEP 3: Create SOURCE tables (SRC_)
-- Regular PostgreSQL tables with proper data types.
-- Named SRC_<FILE_NAME> per naming conventions.
-- NOT NULL constraints on business key columns only.
-- Metric columns (sales, profit, quantity) are nullable.
-- Using CREATE TABLE IF NOT EXISTS — safe to re-run without data loss.
-- To reload from scratch: TRUNCATE the SRC_ tables, then re-run STEP 4.
-----------------------------------------------------------------------------

-- SA_DOMESTIC.SRC_DOMESTIC_SALES
CREATE TABLE IF NOT EXISTS sa_domestic.src_domestic_sales (
    row_id VARCHAR(50),
    order_id VARCHAR(50) NOT NULL,
    order_date DATE NOT NULL,
    ship_date DATE,
    ship_mode VARCHAR(50),
    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(255),
    segment VARCHAR(50),
    country VARCHAR(100),
    city VARCHAR(100),
    region VARCHAR(100),
    market VARCHAR(50),
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(255),
    category VARCHAR(100),
    sub_category VARCHAR(100),
    sales NUMERIC(15,4),
    quantity INTEGER,
    discount NUMERIC(5,4),
    profit NUMERIC(15,4),
    shipping_cost NUMERIC(15,4),
    cost NUMERIC(15,4),
    employee_id VARCHAR(50),
    employee_name VARCHAR(255),
    order_priority VARCHAR(25)
);

-- SA_INTERNATIONAL.SRC_INTERNATIONAL_SALES
CREATE TABLE IF NOT EXISTS sa_international.src_international_sales (
    row_id VARCHAR(50),
    order_id VARCHAR(50) NOT NULL,
    order_date DATE NOT NULL,
    ship_date DATE,
    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(255),
    segment VARCHAR(50),
    country VARCHAR(100),
    state VARCHAR(100),
    city VARCHAR(100),
    region VARCHAR(100),
    market VARCHAR(50),
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(255),
    category VARCHAR(100),
    sub_category VARCHAR(100),
    sales NUMERIC(15,4),
    quantity INTEGER,
    discount NUMERIC(5,4),
    profit NUMERIC(15,4),
    shipping_cost NUMERIC(15,4),
    cost NUMERIC(15,4),
    employee_id VARCHAR(50)
);

-- STEP 4: Incremental load procedures EXT_ -> SRC_
-- These procedures are called both for the initial load and for every
-- subsequent incremental load (i.e. when the CSV on disk is replaced with
-- a new file containing new rows).
--
-- HOW INCREMENTAL LOAD WORKS AT THE SA LAYER:
--   - EXT_ is a foreign table: it always reads whatever CSV is on disk right now.
--   - SRC_ is a regular PostgreSQL table that accumulates rows across all loads.
--   - The WHERE NOT EXISTS guard (keyed on order_id + product_id + customer_id
--     + order_date) ensures rows already in SRC_ are never inserted again.
--   - So when you swap the CSV to the 5% increment file and call these
--     procedures, only the genuinely new order lines (not already in SRC_)
--     are inserted. Previously loaded rows are skipped automatically.
--   - Logging and exception blocks satisfy the project requirements.
--
-- TO RUN AFTER AN INITIAL LOAD:
--   Replace the CSV file on disk with the new increment file, then call:
--   CALL bl_cl.prc_load_src_domestic();
--   CALL bl_cl.prc_load_src_international();
--   (or simply CALL bl_cl.prc_load_all_src() to run both at once)
-----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_src_domestic()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc  CONSTANT VARCHAR := 'bl_cl.prc_load_src_domestic';
    v_count INTEGER := 0;
BEGIN
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
    WHERE rn = 1
      AND NOT EXISTS (
          SELECT 1
          FROM sa_domestic.src_domestic_sales existing
          WHERE existing.order_id = deduped.order_id
            AND existing.product_id = deduped.product_id
            AND existing.customer_id = deduped.customer_id
            AND existing.order_date = deduped.order_date::DATE
      );

    GET DIAGNOSTICS v_count = ROW_COUNT;

    CALL bl_cl.prc_log(v_proc, v_count, 'SUCCESS',
        'Loaded ' || v_count || ' new rows into sa_domestic.src_domestic_sales');

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'SQLERRM: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


CREATE OR REPLACE PROCEDURE bl_cl.prc_load_src_international()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc  CONSTANT VARCHAR := 'bl_cl.prc_load_src_international';
    v_count INTEGER := 0;
BEGIN
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
    WHERE rn = 1
      AND NOT EXISTS (
          SELECT 1
          FROM sa_international.src_international_sales existing
          WHERE existing.order_id = deduped.order_id
            AND existing.product_id = deduped.product_id
            AND existing.customer_id = deduped.customer_id
            AND existing.order_date = deduped.order_date::DATE
      );

    GET DIAGNOSTICS v_count = ROW_COUNT;

    CALL bl_cl.prc_log(v_proc, v_count, 'SUCCESS',
        'Loaded ' || v_count || ' new rows into sa_international.src_international_sales');

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'SQLERRM: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


-- Master SA load procedure, calls both sources in sequence
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_all_src()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc CONSTANT VARCHAR := 'bl_cl.prc_load_all_src';
BEGIN
    RAISE NOTICE '% started at %', v_proc, NOW();

    CALL bl_cl.prc_load_src_domestic();
    CALL bl_cl.prc_load_src_international();

    CALL bl_cl.prc_log(v_proc, 0, 'SUCCESS',
        'All SA source tables loaded successfully');

    RAISE NOTICE '% finished at %', v_proc, NOW();

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'SA master load failed: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


-- STEP 4a: Initial load, call the procedures immediately for the first time
-----------------------------------------------------------------------------
CALL bl_cl.prc_load_all_src();
--10868
-- STEP 5: Verification queries
-----------------------------------------------------------------------------

-- Row counts: EXT_ should have >= rows than SRC_ due to deduplication
SELECT 
    'SA_DOMESTIC.EXT_DOMESTIC_SALES' AS table_name, 
    COUNT(*) AS row_count 
FROM sa_domestic.ext_domestic_sales
UNION ALL
SELECT 
    'SA_DOMESTIC.SRC_DOMESTIC_SALES' AS table_name, 
    COUNT(*) AS row_count 
FROM sa_domestic.src_domestic_sales
UNION ALL
SELECT 
    'SA_INTERNATIONAL.EXT_INTERNATIONAL_SALES' AS table_name, 
    COUNT(*) AS row_count 
FROM sa_international.ext_international_sales
UNION ALL
SELECT 
    'SA_INTERNATIONAL.SRC_INTERNATIONAL_SALES' AS table_name, 
    COUNT(*) AS row_count 
FROM sa_international.src_international_sales;

-- Duplicates removed
SELECT
    (SELECT COUNT(*) FROM sa_domestic.ext_domestic_sales) -
    (SELECT COUNT(*) FROM sa_domestic.src_domestic_sales)
    AS domestic_duplicates_removed,
    (SELECT COUNT(*) FROM sa_international.ext_international_sales) -
    (SELECT COUNT(*) FROM sa_international.src_international_sales)
    AS international_duplicates_removed;

-- Sample rows
SELECT * FROM sa_domestic.ext_domestic_sales LIMIT 5;
SELECT * FROM sa_domestic.src_domestic_sales LIMIT 5;
SELECT * FROM sa_international.ext_international_sales LIMIT 5;
SELECT * FROM sa_international.src_international_sales LIMIT 5;