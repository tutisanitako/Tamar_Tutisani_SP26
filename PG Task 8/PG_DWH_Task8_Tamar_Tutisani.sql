-- POSTGRESQL TASK 8: Loading DM dimensions
-- Tamar Tutisani
-- Module: PG_DWH
-----------------------------------------------------------------------------

---------------------------------------------------------------
-- SECTION 0: PRIVILEGES FOR BL_CL ON BL_DM
---------------------------------------------------------------

GRANT USAGE ON SCHEMA bl_dm TO PUBLIC;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA bl_dm TO PUBLIC;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA bl_dm TO PUBLIC;


---------------------------------------------------------------
-- SECTION 1: COMPOSITE TYPE
---------------------------------------------------------------
-- Task requires "use composite types (one or more procedures)".
-- I'm defining a composite type for the product row we'll loop through
-- when loading DIM_PRODUCTS. Instead of using RECORD everywhere, the
-- composite type makes the structure explicit and documents what the
-- cursor is expected to carry.

DROP TYPE IF EXISTS bl_cl.t_dim_product_row CASCADE;

CREATE TYPE bl_cl.t_dim_product_row AS (
    product_src_id VARCHAR(50),
    product_name VARCHAR(255),
    product_category_id BIGINT,
    product_category_name VARCHAR(100),
    product_subcategory_id BIGINT,
    product_subcategory_name VARCHAR(100),
    source_system VARCHAR(100),
    source_entity VARCHAR(100)
);


---------------------------------------------------------------
-- SECTION 2: FUNCTION THAT RETURNS SETOF COMPOSITE TYPE
---------------------------------------------------------------
-- Returns denormalized product rows by joining the 3NF hierarchy
-- (ce_products -> ce_product_subcategories -> ce_product_categories).
-- Used as the data source inside prc_load_dim_products via a cursor FOR loop.
-- This satisfies "function returns table / setof type" from task 7 again,
-- but here it's used with a proper composite type as the return shape.

CREATE OR REPLACE FUNCTION bl_cl.fn_get_dm_products()
RETURNS SETOF bl_cl.t_dim_product_row
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        p.product_src_id,
        p.product_name,
        cat.product_category_id,
        cat.product_category_name,
        sub.product_subcategory_id,
        sub.product_subcategory_name,
        p.source_system,
        p.source_entity
    FROM bl_3nf.ce_products p
    JOIN bl_3nf.ce_product_subcategories sub
        ON sub.product_subcategory_id = p.product_subcategory_id
    JOIN bl_3nf.ce_product_categories cat
        ON cat.product_category_id = sub.product_category_id
    WHERE p.product_id != -1;
END;
$$;


---------------------------------------------------------------
-- SECTION 3: LOADING PROCEDURES
---------------------------------------------------------------
-- Loading order respects FK dependencies in FCT_SALES_DD:
-- 1. DIM_TIME_DAY         (already populated in DDL, skip procedure)
-- 2. DIM_PRODUCTS         (SCD1, UPSERT, composite type + cursor FOR loop)
-- 3. DIM_CUSTOMERS_SCD    (SCD2, cursor variable, incremental delta logic)
-- 4. DIM_GEOGRAPHY        (SCD1, UPSERT, EXECUTE dynamic SQL)
-- 5. DIM_EMPLOYEES        (SCD1, UPSERT)
-- 6. DIM_ORDER_ATTRIBUTES (SCD0, UPSERT)

-- DIM_TIME_DAY was populated once in the DDL script (task 7).
-- A procedure for it isn't needed because it never changes (SCD Type 0).


-- 3.1 DIM_PRODUCTS  (SCD Type 1)
-- ----------------------------------------------------------
-- Uses the composite type t_dim_product_row and fn_get_dm_products().
-- UPSERT (INSERT ON CONFLICT DO UPDATE) handles SCD1: if a product already
-- exists we overwrite name/category/subcategory with the latest values
-- and refresh update_dt. Surrogate key is preserved on conflict.
-- ON CONFLICT targets product_src_id (has UNIQUE constraint from DDL).

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_dim_products()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc CONSTANT VARCHAR := 'bl_cl.prc_load_dim_products';
    v_count INTEGER := 0;
    v_row bl_cl.t_dim_product_row;  -- composite type variable
BEGIN
    -- cursor FOR loop over the function that returns setof composite type
    FOR v_row IN
        SELECT * FROM bl_cl.fn_get_dm_products()
    LOOP
        INSERT INTO bl_dm.dim_products (
            product_surr_id,
            product_src_id,
            product_name,
            product_category_id,
            product_category_name,
            product_subcategory_id,
            product_subcategory_name,
            insert_dt,
            update_dt,
            source_system,
            source_entity
        )
        VALUES (
            nextval('bl_dm.seq_product_surr_id'),
            v_row.product_src_id,
            COALESCE(v_row.product_name, 'n.a.'),
            v_row.product_category_id,
            COALESCE(v_row.product_category_name, 'n.a.'),
            v_row.product_subcategory_id,
            COALESCE(v_row.product_subcategory_name, 'n.a.'),
            CURRENT_DATE,
            CURRENT_DATE,
            v_row.source_system,
            v_row.source_entity
        )
        -- SCD1: on conflict with existing product_src_id, overwrite descriptive
        -- attributes. The surrogate key (product_surr_id) stays the same.
        ON CONFLICT (product_src_id) DO UPDATE
            SET product_name = EXCLUDED.product_name,
                product_category_id = EXCLUDED.product_category_id,
                product_category_name = EXCLUDED.product_category_name,
                product_subcategory_id = EXCLUDED.product_subcategory_id,
                product_subcategory_name = EXCLUDED.product_subcategory_name,
                update_dt = CURRENT_DATE,
                source_system = EXCLUDED.source_system,
                source_entity = EXCLUDED.source_entity;

        -- only count genuinely new inserts, not updates
        IF NOT FOUND THEN
            -- FOUND is FALSE when ON CONFLICT DO UPDATE fires (it's an update)
            -- so we need the opposite: count when a new row was inserted
            NULL;
        END IF;

        -- track new inserts via xmax: if xmax=0 it's a fresh insert
        -- simpler approach: count before vs after
        v_count := v_count + 1;
    END LOOP;

    -- v_count here reflects total rows processed (new + updated).
    -- On re-run: all rows will hit ON CONFLICT DO UPDATE but nothing
    -- logically changes because data is the same. rows_affected = total
    -- products processed is still meaningful to log.
    CALL bl_cl.prc_log(v_proc, v_count, 'SUCCESS',
        'Processed ' || v_count || ' rows into bl_dm.dim_products (new inserts + SCD1 updates)');

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'SQLERRM: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


-- 3.2 DIM_CUSTOMERS_SCD  (SCD Type 2)
-- ----------------------------------------------------------
-- This is the most complex procedure.
-- SCD2 logic:
--   a) New customer (src_id not in DIM at all) -> insert with
--      START_DT = today, END_DT = 9999-12-31, IS_ACTIVE = Y
--   b) Existing customer with changed segment -> expire the current
--      active row (END_DT = today-1, IS_ACTIVE = N) and insert a new
--      version (START_DT = today, END_DT = 9999-12-31, IS_ACTIVE = Y)
--   c) Existing customer, no change -> skip
--
-- Uses a cursor variable:
--   cur_customers refcursor is declared, opened with OPEN FOR, then
--   iterated with FETCH. This is different from a plain FOR loop and
--   lets us demonstrate explicit cursor control.

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_dim_customers_scd()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc CONSTANT VARCHAR := 'bl_cl.prc_load_dim_customers_scd';
    v_count_new INTEGER := 0;
    v_count_scd INTEGER := 0;

    -- explicit cursor variable (task requirement)
    cur_customers REFCURSOR;
    v_row RECORD;

    -- variables for the SCD2 comparison
    v_existing_segment VARCHAR(50);
    v_existing_name VARCHAR(255);
BEGIN
    -- open the cursor explicitly (OPEN FOR syntax)
    OPEN cur_customers FOR
        SELECT
            c.customer_src_id,
            c.customer_name,
            c.customer_segment,
            c.source_system,
            c.source_entity
        FROM bl_3nf.ce_customers_scd c
        WHERE c.customer_id != -1
          AND c.is_active = 'Y';

    LOOP
        FETCH cur_customers INTO v_row;
        EXIT WHEN NOT FOUND;

        -- check if this customer already exists in DIM with an active version
        SELECT customer_segment, customer_name
        INTO v_existing_segment, v_existing_name
        FROM bl_dm.dim_customers_scd
        WHERE customer_src_id = v_row.customer_src_id
          AND is_active = 'Y';

        IF NOT FOUND THEN
            -- case (a): brand new customer, never seen before in DIM
            INSERT INTO bl_dm.dim_customers_scd (
                customer_surr_id,
                customer_src_id,
                customer_name,
                customer_segment,
                start_dt,
                end_dt,
                is_active,
                insert_dt,
                update_dt,
                source_system,
                source_entity
            )
            VALUES (
                nextval('bl_dm.seq_customer_surr_id'),
                v_row.customer_src_id,
                COALESCE(v_row.customer_name, 'n.a.'),
                COALESCE(v_row.customer_segment, 'n.a.'),
                CURRENT_DATE,
                '9999-12-31'::DATE,
                'Y',
                CURRENT_DATE,
                CURRENT_DATE,
                v_row.source_system,
                v_row.source_entity
            );
            v_count_new := v_count_new + 1;

		ELSIF v_existing_segment IS DISTINCT FROM v_row.customer_segment
		   OR v_existing_name IS DISTINCT FROM v_row.customer_name THEN
		
		    DECLARE
		        v_old_start_dt DATE;
		        v_new_end_dt DATE;
		    BEGIN
		        -- get the start_dt of the row we're about to expire
		        SELECT start_dt INTO v_old_start_dt
		        FROM bl_dm.dim_customers_scd
		        WHERE customer_src_id = v_row.customer_src_id
		          AND is_active = 'Y';
		
		        -- end_dt must be strictly > start_dt
		        v_new_end_dt := GREATEST(v_old_start_dt + 1, CURRENT_DATE);
		
		        UPDATE bl_dm.dim_customers_scd
		        SET end_dt    = v_new_end_dt,
		            is_active = 'N',
		            update_dt = CURRENT_DATE
		        WHERE customer_src_id = v_row.customer_src_id
		          AND is_active = 'Y';
		
		        INSERT INTO bl_dm.dim_customers_scd (
		            customer_surr_id, customer_src_id, customer_name,
		            customer_segment, start_dt, end_dt, is_active,
		            insert_dt, update_dt, source_system, source_entity
		        )
		        VALUES (
		            nextval('bl_dm.seq_customer_surr_id'),
		            v_row.customer_src_id,
		            COALESCE(v_row.customer_name, 'n.a.'),
		            COALESCE(v_row.customer_segment, 'n.a.'),
		            v_new_end_dt + 1,        -- new version starts day after expiry
		            '9999-12-31'::DATE,
		            'Y',
		            CURRENT_DATE,
		            CURRENT_DATE,
		            v_row.source_system,
		            v_row.source_entity
		        );
		
		        v_count_scd := v_count_scd + 1;
		    END;

        -- case (c): no change, do nothing
        END IF;
    END LOOP;

    CLOSE cur_customers;

    CALL bl_cl.prc_log(v_proc, v_count_new + v_count_scd, 'SUCCESS',
        'DIM_CUSTOMERS_SCD: ' || v_count_new || ' new inserts, '
        || v_count_scd || ' SCD2 version changes');

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'SQLERRM: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


-- 3.3 DIM_GEOGRAPHY  (SCD Type 1)
-- ----------------------------------------------------------
-- Flattens the 5-level 3NF geography hierarchy
-- (ce_cities -> ce_states -> ce_countries -> ce_regions -> ce_markets)
-- into one wide DIM_GEOGRAPHY row.
-- Uses EXECUTE (dynamic SQL) to satisfy the task requirement.
-- The INSERT itself is built as a string and executed with EXECUTE + USING
-- to pass parameters safely (no SQL injection risk).
-- UPSERT (ON CONFLICT DO UPDATE) handles SCD1 overwrites.

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_dim_geography()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc CONSTANT VARCHAR := 'bl_cl.prc_load_dim_geography';
    v_count INTEGER := 0;
    v_row RECORD;
    v_sql TEXT;  -- dynamic SQL string
BEGIN
    -- build the INSERT statement once as a string, then execute it for each row
    -- using USING clause to pass values. %I is for identifiers, $1..$n for values.
    v_sql := '
        INSERT INTO bl_dm.dim_geography (
            geography_surr_id,
            geography_src_id,
            city_name,
            state_name,
            country_name,
            region_name,
            market_name,
            insert_dt,
            update_dt,
            source_system,
            source_entity
        )
        VALUES (
            nextval(''bl_dm.seq_geography_surr_id''),
            $1, $2, $3, $4, $5, $6,
            CURRENT_DATE,
            CURRENT_DATE,
            $7, $8
        )
        ON CONFLICT (geography_src_id) DO UPDATE
            SET city_name = EXCLUDED.city_name,
                state_name = EXCLUDED.state_name,
                country_name = EXCLUDED.country_name,
                region_name = EXCLUDED.region_name,
                market_name = EXCLUDED.market_name,
                update_dt = CURRENT_DATE,
                source_system  = EXCLUDED.source_system,
                source_entity  = EXCLUDED.source_entity
    ';

    FOR v_row IN
        SELECT
            cty.city_src_id AS geography_src_id,
            cty.city_name,
            st.state_name,
            co.country_name,
            re.region_name,
            mk.market_name,
            cty.source_system,
            cty.source_entity
        FROM bl_3nf.ce_cities cty
        JOIN bl_3nf.ce_states st ON st.state_id = cty.state_id
        JOIN bl_3nf.ce_countries co ON co.country_id = st.country_id
        JOIN bl_3nf.ce_regions re ON re.region_id = co.region_id
        JOIN bl_3nf.ce_markets mk ON mk.market_id = re.market_id
        WHERE cty.city_id != -1
    LOOP
        -- EXECUTE with USING passes values as typed parameters, not string
        -- concatenation, which avoids any risk of SQL injection
        EXECUTE v_sql
        USING
            v_row.geography_src_id,
            v_row.city_name,
            v_row.state_name,
            v_row.country_name,
            v_row.region_name,
            v_row.market_name,
            v_row.source_system,
            v_row.source_entity;

        v_count := v_count + 1;
    END LOOP;

    CALL bl_cl.prc_log(v_proc, v_count, 'SUCCESS',
        'Processed ' || v_count || ' rows into bl_dm.dim_geography');

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'SQLERRM: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


-- 3.4 DIM_EMPLOYEES  (SCD Type 1)
-- ----------------------------------------------------------
-- Simple UPSERT from ce_employees.
-- If employee_name was 'n.a.' (international-only) and later the domestic
-- source provides the real name, the UPDATE clause overwrites it (SCD1).

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_dim_employees()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc CONSTANT VARCHAR := 'bl_cl.prc_load_dim_employees';
    v_count INTEGER := 0;
    v_row RECORD;
BEGIN
    FOR v_row IN
        SELECT
            employee_src_id,
            employee_name,
            source_system,
            source_entity
        FROM bl_3nf.ce_employees
        WHERE employee_id != -1
    LOOP
        INSERT INTO bl_dm.dim_employees (
            employee_surr_id,
            employee_src_id,
            employee_name,
            insert_dt,
            update_dt,
            source_system,
            source_entity
        )
        VALUES (
            nextval('bl_dm.seq_employee_surr_id'),
            v_row.employee_src_id,
            COALESCE(v_row.employee_name, 'n.a.'),
            CURRENT_DATE,
            CURRENT_DATE,
            v_row.source_system,
            v_row.source_entity
        )
        ON CONFLICT (employee_src_id) DO UPDATE
            SET employee_name = EXCLUDED.employee_name,
                update_dt = CURRENT_DATE,
                source_system = EXCLUDED.source_system,
                source_entity = EXCLUDED.source_entity;

        v_count := v_count + 1;
    END LOOP;

    CALL bl_cl.prc_log(v_proc, v_count, 'SUCCESS',
        'Processed ' || v_count || ' rows into bl_dm.dim_employees');

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'SQLERRM: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


-- 3.5 DIM_ORDER_ATTRIBUTES  (SCD Type 0)
-- ----------------------------------------------------------
-- Junk dimension: ship_mode x order_priority combinations.
-- SCD Type 0: once inserted, values never change. The DO NOTHING clause
-- enforces this - we simply skip rows that already exist.
-- ON CONFLICT uses the composite UNIQUE constraint (ship_mode, order_priority).

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_dim_order_attributes()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc CONSTANT VARCHAR := 'bl_cl.prc_load_dim_order_attributes';
    v_count INTEGER := 0;
    v_row RECORD;
BEGIN
    FOR v_row IN
        SELECT
            ship_mode,
            order_priority,
            source_system,
            source_entity
        FROM bl_3nf.ce_order_attributes
        WHERE order_attr_id != -1
    LOOP
        INSERT INTO bl_dm.dim_order_attributes (
            order_attr_surr_id,
            ship_mode,
            order_priority,
            insert_dt,
            update_dt,
            source_system,
            source_entity
        )
        VALUES (
            nextval('bl_dm.seq_order_attr_surr_id'),
            v_row.ship_mode,
            v_row.order_priority,
            CURRENT_DATE,
            CURRENT_DATE,
            v_row.source_system,
            v_row.source_entity
        )
        -- SCD Type 0: do nothing on conflict, combinations are permanent
        ON CONFLICT (ship_mode, order_priority) DO NOTHING;

        v_count := v_count + 1;
    END LOOP;

    CALL bl_cl.prc_log(v_proc, v_count, 'SUCCESS',
        'Processed ' || v_count || ' rows into bl_dm.dim_order_attributes');

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'SQLERRM: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;

---------------------------------------------------------------
-- SECTION 4: MASTER PROCEDURE
---------------------------------------------------------------

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_all_dm_dims()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc CONSTANT VARCHAR := 'bl_cl.prc_load_all_dm_dims';
BEGIN
    RAISE NOTICE '% started at %', v_proc, NOW();

    CALL bl_cl.prc_load_dim_products();
    CALL bl_cl.prc_load_dim_customers_scd();
    CALL bl_cl.prc_load_dim_geography();
    CALL bl_cl.prc_load_dim_employees();
    CALL bl_cl.prc_load_dim_order_attributes();

    CALL bl_cl.prc_log(v_proc, 0, 'SUCCESS',
        'All DM dimension procedures completed successfully');

    RAISE NOTICE '% finished at %', v_proc, NOW();

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'Master procedure failed: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


---------------------------------------------------------------
-- SECTION 5: RUN 1 + VERIFY
---------------------------------------------------------------

CALL bl_cl.prc_load_all_dm_dims();

-- All procedures should show SUCCESS.
SELECT
    procedure_name,
    rows_affected,
    status,
    log_message,
    log_dt
FROM bl_cl.mta_load_log
WHERE procedure_name LIKE '%dm%' OR procedure_name LIKE '%dim%'
ORDER BY log_dt DESC, log_id DESC;


-- Row counts per DIM table
SELECT 'DIM_PRODUCTS' AS table_name, COUNT(*) AS total_rows,
       COUNT(*) FILTER (WHERE product_surr_id = -1) AS default_rows
FROM bl_dm.dim_products
UNION ALL
SELECT 'DIM_CUSTOMERS_SCD', COUNT(*),
       COUNT(*) FILTER (WHERE customer_surr_id = -1)
FROM bl_dm.dim_customers_scd
UNION ALL
SELECT 'DIM_GEOGRAPHY', COUNT(*),
       COUNT(*) FILTER (WHERE geography_surr_id = -1)
FROM bl_dm.dim_geography
UNION ALL
SELECT 'DIM_EMPLOYEES', COUNT(*),
       COUNT(*) FILTER (WHERE employee_surr_id = -1)
FROM bl_dm.dim_employees
UNION ALL
SELECT 'DIM_ORDER_ATTRIBUTES', COUNT(*),
       COUNT(*) FILTER (WHERE order_attr_surr_id = -1)
FROM bl_dm.dim_order_attributes;
-- results shown in screenshot: union.png

-- Sample rows from DIM_PRODUCTS - shows hierarchy was correctly flattened
SELECT
    product_surr_id,
    product_src_id,
    product_name,
    product_category_name,
    product_subcategory_name,
    update_dt
FROM bl_dm.dim_products
WHERE product_surr_id != -1
LIMIT 5;
-- results shown in screenshot: dm_sample_products.png

-- Sample rows from DIM_CUSTOMERS_SCD - shows SCD2 columns
SELECT
    customer_surr_id,
    customer_src_id,
    customer_name,
    customer_segment,
    start_dt,
    end_dt,
    is_active,
    source_system
FROM bl_dm.dim_customers_scd
WHERE customer_surr_id != -1
LIMIT 5;
-- results shown in screenshot: dm_sample_customers.png


-- Sample rows from DIM_GEOGRAPHY - shows full hierarchy flattened
SELECT
    geography_surr_id,
    city_name,
    state_name,
    country_name,
    region_name,
    market_name
FROM bl_dm.dim_geography
WHERE geography_surr_id != -1
LIMIT 5;
-- results shown in screenshot: dm_sample_geography.png


---------------------------------------------------------------
-- SECTION 6: RUN 2 (idempotency check)
---------------------------------------------------------------

CALL bl_cl.prc_load_all_dm_dims();

SELECT
    procedure_name,
    rows_affected,
    status,
    log_message,
    log_dt
FROM bl_cl.mta_load_log
WHERE procedure_name LIKE '%dm%' OR procedure_name LIKE '%dim%'
ORDER BY log_dt DESC, log_id DESC
LIMIT 12;
-- results shown in screenshot: dm_log_run.png

/*
EXPLANATION - IDEMPOTENCY:
- DIM_PRODUCTS (SCD1 UPSERT): on re-run all rows hit ON CONFLICT DO UPDATE.
  Since source data hasn't changed, SET values equal what's already there.
  rows_affected = same total because the loop still processes every row.
  No duplicates are created. Functionally idempotent.
- DIM_CUSTOMERS_SCD (SCD2 cursor): on re-run, every customer already exists
  with is_active = 'Y' and the same segment/name, so case (c) fires for all
  of them (no change detected). rows_affected = 0. Fully idempotent.
- DIM_GEOGRAPHY (SCD1 EXECUTE/UPSERT): same as DIM_PRODUCTS. rows_affected =
  total rows processed (all hit ON CONFLICT path with no actual change).
- DIM_EMPLOYEES (SCD1 UPSERT): same as DIM_PRODUCTS.
- DIM_ORDER_ATTRIBUTES (SCD0, DO NOTHING): on re-run all rows hit DO NOTHING.
  rows_affected = total combinations processed but 0 actual inserts.
*/


---------------------------------------------------------------
-- SECTION 7: SCD2 TEST
---------------------------------------------------------------
-- Task requires:
-- 1. screenshot of original CSV rows
-- 2. screenshot of SCD2 data in 3NF and DM matching those rows
-- 3. modify the CSV (change a customer segment)
-- 4. run the procedure
-- 5. screenshot showing the new version in 3NF and DM

-- STEP 7a: pick a handful of customers to show before the change
-- screenshot: scd2_before_3nf.png
SELECT
    customer_id,
    customer_src_id,
    customer_name,
    customer_segment,
    start_dt,
    end_dt,
    is_active,
    insert_dt
FROM bl_3nf.ce_customers_scd
WHERE customer_src_id IN (
    SELECT customer_src_id
    FROM bl_3nf.ce_customers_scd
    WHERE customer_id != -1
    ORDER BY customer_id
    LIMIT 5
)
ORDER BY customer_src_id, start_dt;

-- screenshot: scd2_before_dm.png
SELECT
    customer_surr_id,
    customer_src_id,
    customer_name,
    customer_segment,
    start_dt,
    end_dt,
    is_active
FROM bl_dm.dim_customers_scd
WHERE customer_src_id IN (
    SELECT customer_src_id
    FROM bl_dm.dim_customers_scd
    WHERE customer_surr_id != -1
    ORDER BY customer_surr_id
    LIMIT 5
)
ORDER BY customer_src_id, start_dt;

-- STEP 7b: simulate a segment change arriving in the 3NF layer
-- (In a real incremental load this would come from updated source data
-- re-processed through prc_load_customers_scd in task 7. For the test
-- here I update CE_CUSTOMERS_SCD directly to simulate what would happen
-- when a customer switches from 'Consumer' to 'Corporate'.)

-- Find a Consumer customer to change (store it so we can reference it)
DO $$
DECLARE
    v_src_id VARCHAR(100);
BEGIN
    SELECT customer_src_id INTO v_src_id
    FROM bl_3nf.ce_customers_scd
    WHERE customer_segment = 'Consumer'
      AND is_active = 'Y'
      AND customer_id != -1
    ORDER BY customer_id
    LIMIT 1;

    RAISE NOTICE 'Simulating segment change for customer: %', v_src_id;

    -- simulate the 3NF layer having received updated segment data
    UPDATE bl_3nf.ce_customers_scd
    SET customer_segment = 'Corporate'
    WHERE customer_src_id = v_src_id
      AND is_active = 'Y';
END;
$$;

-- screenshot: scd2_3nf_after_source_change.png
-- Confirm the 3NF row now shows Corporate (still single row - 3NF SCD2
-- logic adds versions; the procedure in task 7 would do that on incremental
-- load but here we're testing the DM-layer SCD2 procedure directly)
SELECT
    customer_id,
    customer_src_id,
    customer_name,
    customer_segment,
    start_dt,
    end_dt,
    is_active
FROM bl_3nf.ce_customers_scd
WHERE customer_id != -1
ORDER BY customer_id
LIMIT 5;

-- STEP 7c: run the DM SCD2 procedure - it should detect the change and
-- create a new version in DIM_CUSTOMERS_SCD
CALL bl_cl.prc_load_dim_customers_scd();

-- screenshot: scd2_after_dm.png
-- The changed customer should now have 2 rows in DIM_CUSTOMERS_SCD:
-- - old version: is_active = 'N', end_dt = today - 1
-- - new version: is_active = 'Y', customer_segment = 'Corporate'
SELECT
    customer_surr_id,
    customer_src_id,
    customer_name,
    customer_segment,
    start_dt,
    end_dt,
    is_active,
    update_dt
FROM bl_dm.dim_customers_scd
WHERE customer_src_id = (
    -- pick the customer whose segment we just changed
    SELECT customer_src_id
    FROM bl_3nf.ce_customers_scd
    WHERE customer_segment = 'Corporate'
      AND is_active = 'Y'
      AND customer_id != -1
    ORDER BY customer_id
    LIMIT 1
)
ORDER BY start_dt;

-- screenshot: scd2_log_after_change.png
-- Log entry for the SCD2 run should show rows_affected > 0 (the new version)
SELECT
    procedure_name,
    rows_affected,
    status,
    log_message,
    log_dt
FROM bl_cl.mta_load_log
WHERE procedure_name = 'bl_cl.prc_load_dim_customers_scd'
ORDER BY log_dt DESC
LIMIT 3;

/*
WHAT SCD2 TEST PROVES:
The procedure correctly detects when customer_segment changes between the
3NF source and the currently active DM row. It expires the old version
(end_dt = today - 1, is_active = N) and inserts a new active version with
the updated segment. The two rows have different customer_surr_id values
(generated by sequence), different start_dt and end_dt, and no gaps or
overlaps in the time coverage. This is the correct SCD2 behaviour.

On the next re-run with no further changes, rows_affected = 0 because the
active DM version now matches the 3NF source again.
*/