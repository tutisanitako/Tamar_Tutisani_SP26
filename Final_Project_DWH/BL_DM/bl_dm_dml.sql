-- BL_DM DML: Loading procedures for all DIM_ tables and FCT_SALES_DD
-- Global Retail Superstore Sales
-- Tamar Tutisani
--
-- Contains:
--   - t_dim_product_row composite type
--   - fn_get_dm_products() function returning SETOF composite type
--   - prc_load_dim_products()       (SCD1, UPSERT, composite type + FOR loop)
--   - prc_load_dim_customers_scd()  (SCD2, cursor variable, delta logic)
--   - prc_load_dim_geography()      (SCD1, UPSERT, EXECUTE dynamic SQL)
--   - prc_load_dim_employees()      (SCD1, UPSERT)
--   - prc_load_dim_order_attributes() (SCD0, UPSERT DO NOTHING)
--   - prc_load_all_dm_dims()        (master for dimensions)
--   - prc_load_ce_sales_incremental() (incremental 3NF fact load)
--   - prc_load_fct_sales_dd_rolling() (DM fact with partition ATTACH/DETACH)
--   - prc_load_all_facts()          (master for facts)
--
-- Grants: to bl_cl_role only, NOT to PUBLIC.
-----------------------------------------------------------------------------


-- SECTION 0: GRANTS TO bl_cl_role (not PUBLIC)
-----------------------------------------------------------------------------
GRANT USAGE ON SCHEMA bl_dm TO bl_cl_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA bl_dm TO bl_cl_role;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA bl_dm TO bl_cl_role;


-- SECTION 1: COMPOSITE TYPE + FUNCTION
-----------------------------------------------------------------------------

-- Composite type for product rows (used by prc_load_dim_products)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type t
                   JOIN pg_namespace n ON n.oid = t.typnamespace
                   WHERE t.typname = 't_dim_product_row' AND n.nspname = 'bl_cl') THEN
        CREATE TYPE bl_cl.t_dim_product_row AS (
            product_src_id           VARCHAR(50),
            product_name             VARCHAR(255),
            product_category_id      BIGINT,
            product_category_name    VARCHAR(100),
            product_subcategory_id   BIGINT,
            product_subcategory_name VARCHAR(100),
            source_system            VARCHAR(100),
            source_entity            VARCHAR(100)
        );
    END IF;
END;
$$;

-- Function returning SETOF composite type
-- Denormalizes the 3NF product hierarchy into flat rows for DIM_PRODUCTS.
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


-- SECTION 2: DIMENSION LOAD PROCEDURES
-----------------------------------------------------------------------------

-- 2.1 DIM_PRODUCTS (SCD Type 1)
-- Uses composite type t_dim_product_row and fn_get_dm_products().
-- UPSERT handles SCD1: on conflict, overwrites descriptive attributes
-- and refreshes update_dt. Surrogate key is preserved on conflict.
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_dim_products()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc  CONSTANT VARCHAR := 'bl_cl.prc_load_dim_products';
    v_count INTEGER := 0;
    v_row   bl_cl.t_dim_product_row;
BEGIN
    FOR v_row IN
        SELECT * FROM bl_cl.fn_get_dm_products()
    LOOP
        INSERT INTO bl_dm.dim_products (
            product_surr_id, product_src_id, product_name,
            product_category_id, product_category_name,
            product_subcategory_id, product_subcategory_name,
            insert_dt, update_dt, source_system, source_entity
        )
        VALUES (
            nextval('bl_dm.seq_product_surr_id'),
            v_row.product_src_id,
            COALESCE(v_row.product_name,             'n.a.'),
            v_row.product_category_id,
            COALESCE(v_row.product_category_name,    'n.a.'),
            v_row.product_subcategory_id,
            COALESCE(v_row.product_subcategory_name, 'n.a.'),
            CURRENT_DATE, CURRENT_DATE,
            v_row.source_system,
            v_row.source_entity
        )
        ON CONFLICT (product_src_id) DO UPDATE
            SET product_name             = EXCLUDED.product_name,
                product_category_id      = EXCLUDED.product_category_id,
                product_category_name    = EXCLUDED.product_category_name,
                product_subcategory_id   = EXCLUDED.product_subcategory_id,
                product_subcategory_name = EXCLUDED.product_subcategory_name,
                update_dt                = CURRENT_DATE,
                source_system            = EXCLUDED.source_system,
                source_entity            = EXCLUDED.source_entity;

        v_count := v_count + 1;
    END LOOP;

    CALL bl_cl.prc_log(v_proc, v_count, 'SUCCESS',
        'Processed ' || v_count || ' rows into bl_dm.dim_products');

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'SQLERRM: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


-- 2.2 DIM_CUSTOMERS_SCD (SCD Type 2)
-- Uses an explicit cursor variable (OPEN FOR / FETCH) per task requirement.
-- Three cases:
--   A) New customer  -> insert with START_DT = today, END_DT = 9999-12-31, IS_ACTIVE = Y
--   B) Changed attrs -> expire active row, insert new version
--   C) No change     -> skip
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_dim_customers_scd()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc          CONSTANT VARCHAR := 'bl_cl.prc_load_dim_customers_scd';
    v_count_new     INTEGER := 0;
    v_count_scd     INTEGER := 0;
    cur_customers   REFCURSOR;
    v_row           RECORD;
    v_existing_seg  VARCHAR(50);
    v_existing_name VARCHAR(255);
    v_old_start_dt  DATE;
    v_new_end_dt    DATE;
BEGIN
    -- Open explicit cursor (satisfies cursor variable requirement)
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

        SELECT customer_segment, customer_name
        INTO   v_existing_seg, v_existing_name
        FROM   bl_dm.dim_customers_scd
        WHERE  customer_src_id = v_row.customer_src_id
          AND  is_active = 'Y';

        IF NOT FOUND THEN
            -- CASE A: new customer
            INSERT INTO bl_dm.dim_customers_scd (
                customer_surr_id, customer_src_id, customer_name, customer_segment,
                start_dt, end_dt, is_active, insert_dt, update_dt,
                source_system, source_entity
            )
            VALUES (
                nextval('bl_dm.seq_customer_surr_id'),
                v_row.customer_src_id,
                COALESCE(v_row.customer_name,    'n.a.'),
                COALESCE(v_row.customer_segment, 'n.a.'),
                CURRENT_DATE, '9999-12-31'::DATE, 'Y',
                CURRENT_DATE, CURRENT_DATE,
                v_row.source_system, v_row.source_entity
            );
            v_count_new := v_count_new + 1;

        ELSIF v_existing_seg  IS DISTINCT FROM v_row.customer_segment
           OR v_existing_name IS DISTINCT FROM v_row.customer_name THEN
            -- CASE B: attribute changed — SCD2 versioning
            DECLARE
                v_inner_old_start DATE;
                v_inner_new_end   DATE;
            BEGIN
                SELECT start_dt INTO v_inner_old_start
                FROM   bl_dm.dim_customers_scd
                WHERE  customer_src_id = v_row.customer_src_id AND is_active = 'Y';

                v_inner_new_end := GREATEST(v_inner_old_start + 1, CURRENT_DATE);

                -- Expire current active row
                UPDATE bl_dm.dim_customers_scd
                SET    end_dt    = v_inner_new_end,
                       is_active = 'N',
                       update_dt = CURRENT_DATE
                WHERE  customer_src_id = v_row.customer_src_id
                  AND  is_active = 'Y';

                -- Insert new active version
                INSERT INTO bl_dm.dim_customers_scd (
                    customer_surr_id, customer_src_id, customer_name, customer_segment,
                    start_dt, end_dt, is_active, insert_dt, update_dt,
                    source_system, source_entity
                )
                VALUES (
                    nextval('bl_dm.seq_customer_surr_id'),
                    v_row.customer_src_id,
                    COALESCE(v_row.customer_name,    'n.a.'),
                    COALESCE(v_row.customer_segment, 'n.a.'),
                    v_inner_new_end + 1, '9999-12-31'::DATE, 'Y',
                    CURRENT_DATE, CURRENT_DATE,
                    v_row.source_system, v_row.source_entity
                );
                v_count_scd := v_count_scd + 1;
            END;

        -- CASE C: no change — do nothing
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


-- 2.3 DIM_GEOGRAPHY (SCD Type 1)
-- Flattens the 5-level 3NF geography hierarchy into one wide row.
-- Uses EXECUTE (dynamic SQL) per task requirement.
-- UPSERT handles SCD1 overwrites.
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_dim_geography()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc  CONSTANT VARCHAR := 'bl_cl.prc_load_dim_geography';
    v_count INTEGER := 0;
    v_row   RECORD;
    v_sql   TEXT;
BEGIN
    -- Build INSERT as dynamic SQL string (satisfies EXECUTE requirement)
    v_sql := '
        INSERT INTO bl_dm.dim_geography (
            geography_surr_id, geography_src_id,
            city_name, state_name, country_name, region_name, market_name,
            insert_dt, update_dt, source_system, source_entity
        )
        VALUES (
            nextval(''bl_dm.seq_geography_surr_id''),
            $1, $2, $3, $4, $5, $6,
            CURRENT_DATE, CURRENT_DATE, $7, $8
        )
        ON CONFLICT (geography_src_id) DO UPDATE
            SET city_name    = EXCLUDED.city_name,
                state_name   = EXCLUDED.state_name,
                country_name = EXCLUDED.country_name,
                region_name  = EXCLUDED.region_name,
                market_name  = EXCLUDED.market_name,
                update_dt    = CURRENT_DATE,
                source_system = EXCLUDED.source_system,
                source_entity = EXCLUDED.source_entity
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
        JOIN bl_3nf.ce_states   st ON st.state_id   = cty.state_id
        JOIN bl_3nf.ce_countries co ON co.country_id = st.country_id
        JOIN bl_3nf.ce_regions   re ON re.region_id  = co.region_id
        JOIN bl_3nf.ce_markets   mk ON mk.market_id  = re.market_id
        WHERE cty.city_id != -1
    LOOP
        -- EXECUTE with USING: safe parameterized values (no SQL injection risk)
        EXECUTE v_sql
        USING
            v_row.geography_src_id,
            COALESCE(v_row.city_name,    'n.a.'),
            COALESCE(v_row.state_name,   'n.a.'),
            COALESCE(v_row.country_name, 'n.a.'),
            COALESCE(v_row.region_name,  'n.a.'),
            COALESCE(v_row.market_name,  'n.a.'),
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


-- 2.4 DIM_EMPLOYEES (SCD Type 1)
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_dim_employees()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc  CONSTANT VARCHAR := 'bl_cl.prc_load_dim_employees';
    v_count INTEGER := 0;
    v_row   RECORD;
BEGIN
    FOR v_row IN
        SELECT employee_src_id, employee_name, source_system, source_entity
        FROM bl_3nf.ce_employees
        WHERE employee_id != -1
    LOOP
        INSERT INTO bl_dm.dim_employees (
            employee_surr_id, employee_src_id, employee_name,
            insert_dt, update_dt, source_system, source_entity
        )
        VALUES (
            nextval('bl_dm.seq_employee_surr_id'),
            v_row.employee_src_id,
            COALESCE(v_row.employee_name, 'n.a.'),
            CURRENT_DATE, CURRENT_DATE,
            v_row.source_system, v_row.source_entity
        )
        ON CONFLICT (employee_src_id) DO UPDATE
            SET employee_name = EXCLUDED.employee_name,
                update_dt     = CURRENT_DATE,
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


-- 2.5 DIM_ORDER_ATTRIBUTES (SCD Type 0 - junk dimension)
-- DO NOTHING enforces SCD0: once loaded, combinations never change.
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_dim_order_attributes()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc  CONSTANT VARCHAR := 'bl_cl.prc_load_dim_order_attributes';
    v_count INTEGER := 0;
    v_row   RECORD;
BEGIN
    FOR v_row IN
        SELECT ship_mode, order_priority, source_system, source_entity
        FROM bl_3nf.ce_order_attributes
        WHERE order_attr_id != -1
    LOOP
        INSERT INTO bl_dm.dim_order_attributes (
            order_attr_surr_id, ship_mode, order_priority,
            insert_dt, update_dt, source_system, source_entity
        )
        VALUES (
            nextval('bl_dm.seq_order_attr_surr_id'),
            COALESCE(v_row.ship_mode,      'n.a.'),
            COALESCE(v_row.order_priority, 'n.a.'),
            CURRENT_DATE, CURRENT_DATE,
            v_row.source_system, v_row.source_entity
        )
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


-- SECTION 3: MASTER PROCEDURE FOR DM DIMENSIONS
-----------------------------------------------------------------------------
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
        'Master DM dims procedure failed: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


-- SECTION 4: FACT TABLE LOAD PROCEDURES
-----------------------------------------------------------------------------

-- 4.1 CE_SALES incremental load (3NF fact)
-- Uses MAX(event_dt) as high-water mark.
-- On first run: loads all rows (v_last_dt IS NULL).
-- On subsequent runs: loads only rows with event_dt > last loaded date.
-- Idempotency: WHERE NOT EXISTS on (order_id, event_dt, source_system).
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_sales_incremental()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc          CONSTANT VARCHAR := 'bl_cl.prc_load_ce_sales_incremental';
    v_count         INTEGER := 0;
    v_last_dt       DATE;
    v_row           RECORD;
    v_date_id       INTEGER;
    v_product_id    BIGINT;
    v_customer_id   BIGINT;
    v_city_id       BIGINT;
    v_employee_id   BIGINT;
    v_order_attr_id BIGINT;
BEGIN
    -- High-water mark: max event_dt already in CE_SALES
    SELECT MAX(event_dt) INTO v_last_dt FROM bl_3nf.ce_sales;

    RAISE NOTICE '% incremental from: %', v_proc, COALESCE(v_last_dt::TEXT, 'full load');

    FOR v_row IN
        SELECT
            s.order_date, s.order_id, s.product_id, s.customer_id,
            s.city, 'N/A'::VARCHAR(100) AS state, s.country, s.region,
            s.employee_id, s.ship_mode, s.order_priority,
            s.sales AS sales_amt, s.cost AS cost_amt, s.profit AS profit_amt,
            s.shipping_cost AS shipping_cost_amt, s.quantity AS quantity_cnt,
            s.discount AS discount_amt,
            'SA_DOMESTIC'      AS source_system,
            'SRC_DOMESTIC_SALES' AS source_entity
        FROM sa_domestic.src_domestic_sales s
        WHERE s.order_date IS NOT NULL
          AND s.order_id IS NOT NULL
          AND s.product_id IS NOT NULL
          AND (v_last_dt IS NULL OR s.order_date > v_last_dt)

        UNION ALL

        SELECT
            s.order_date, s.order_id, s.product_id, s.customer_id,
            s.city, COALESCE(NULLIF(TRIM(s.state), ''), 'N/A') AS state,
            s.country, s.region,
            s.employee_id, NULL::VARCHAR(50), NULL::VARCHAR(25),
            s.sales, s.cost, s.profit, s.shipping_cost, s.quantity, s.discount,
            'SA_INTERNATIONAL',
            'SRC_INTERNATIONAL_SALES'
        FROM sa_international.src_international_sales s
        WHERE s.order_date IS NOT NULL
          AND s.order_id IS NOT NULL
          AND s.product_id IS NOT NULL
          AND (v_last_dt IS NULL OR s.order_date > v_last_dt)
    LOOP
        -- Idempotency guard
        IF EXISTS (
            SELECT 1 FROM bl_3nf.ce_sales e
            WHERE e.order_id      = v_row.order_id
              AND e.event_dt      = v_row.order_date
              AND e.source_system = v_row.source_system
        ) THEN CONTINUE;
        END IF;

        -- Resolve surrogate keys, COALESCE to -1 on miss
        SELECT COALESCE(date_id, -1)        INTO v_date_id       FROM bl_3nf.ce_dates           WHERE date_dt = v_row.order_date;
        SELECT COALESCE(product_id, -1)     INTO v_product_id    FROM bl_3nf.ce_products         WHERE product_src_id = v_row.product_id;
        SELECT COALESCE(customer_id, -1)    INTO v_customer_id   FROM bl_3nf.ce_customers_scd    WHERE customer_src_id = v_row.customer_id AND is_active = 'Y';
        SELECT COALESCE(city_id, -1)        INTO v_city_id       FROM bl_3nf.ce_cities           WHERE city_src_id = (v_row.city || '_' || v_row.country || '_' || v_row.region);
        SELECT COALESCE(employee_id, -1)    INTO v_employee_id   FROM bl_3nf.ce_employees        WHERE employee_src_id = v_row.employee_id;
        SELECT COALESCE(order_attr_id, -1)  INTO v_order_attr_id FROM bl_3nf.ce_order_attributes WHERE ship_mode = COALESCE(NULLIF(v_row.ship_mode, ''), 'n.a.') AND order_priority = COALESCE(NULLIF(v_row.order_priority, ''), 'n.a.');

        INSERT INTO bl_3nf.ce_sales (
            event_dt, date_id, product_id, customer_id, city_id,
            employee_id, order_attr_id, order_id,
            sales_amt, cost_amt, profit_amt, shipping_cost_amt,
            quantity_cnt, discount_amt,
            insert_dt, update_dt, source_system, source_entity
        )
        VALUES (
            v_row.order_date,
            COALESCE(v_date_id, -1),
            COALESCE(v_product_id, -1),
            COALESCE(v_customer_id, -1),
            COALESCE(v_city_id, -1),
            COALESCE(v_employee_id, -1),
            COALESCE(v_order_attr_id, -1),
            COALESCE(v_row.order_id, 'n.a.'),
            v_row.sales_amt, v_row.cost_amt, v_row.profit_amt,
            v_row.shipping_cost_amt, v_row.quantity_cnt, v_row.discount_amt,
            CURRENT_DATE, CURRENT_DATE,
            v_row.source_system, v_row.source_entity
        );

        v_count := v_count + 1;
    END LOOP;

    CALL bl_cl.prc_log(v_proc, v_count, 'SUCCESS',
        'Incremental load from ' || COALESCE(v_last_dt::TEXT, 'beginning')
        || ': ' || v_count || ' new rows into bl_3nf.ce_sales');

    COMMIT;

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'SQLERRM: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


-- 4.2 FCT_SALES_DD rolling-window partition load (DM fact)
-- Rolling window: the 2 most recent months in CE_SALES are always refreshed.
-- For each month:
--   A) Create a staging table (plain heap, same structure as fct_sales_dd)
--   B) Populate staging from CE_SALES + DIM_ surrogate key lookups
--   C) DETACH + DROP the existing partition for that month (if exists)
--   D) RENAME staging to partition name, then ATTACH it
--   E) Create index on event_dt on the newly attached partition
-- This approach keeps FCT_SALES_DD available to readers throughout the load.
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_fct_sales_dd_rolling()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc           CONSTANT VARCHAR := 'bl_cl.prc_load_fct_sales_dd_rolling';
    v_count          INTEGER := 0;
    v_month_count    INTEGER := 0;
    v_month_rec      RECORD;
    v_staging_name   TEXT;
    v_partition_name TEXT;
    v_month_start    DATE;
    v_month_end      DATE;
    v_sql            TEXT;
    v_row_count      INTEGER;
BEGIN
    -- Find the 2 most recent months present in CE_SALES
    FOR v_month_rec IN
        SELECT DISTINCT DATE_TRUNC('month', event_dt)::DATE AS month_start
        FROM bl_3nf.ce_sales
        ORDER BY month_start DESC
        LIMIT 2
    LOOP
        v_month_start    := v_month_rec.month_start;
        v_month_end      := v_month_start + INTERVAL '1 month';
        v_staging_name   := 'fct_sales_dd_stg_' || TO_CHAR(v_month_start, 'YYYYMM');
        v_partition_name := 'fct_sales_dd_'     || TO_CHAR(v_month_start, 'YYYYMM');

        RAISE NOTICE '% processing month: %', v_proc, v_month_start;

        -- STEP A: drop staging table from any previous failed run
        EXECUTE 'DROP TABLE IF EXISTS bl_dm.' || v_staging_name || ' CASCADE';

        -- STEP B: create staging table (plain heap, same columns)
        EXECUTE 'CREATE TABLE bl_dm.' || v_staging_name || ' (LIKE bl_dm.fct_sales_dd)';

        -- STEP C: populate staging from CE_SALES joined to DM dimension tables
        EXECUTE '
            INSERT INTO bl_dm.' || v_staging_name || ' (
                event_dt, date_id,
                product_surr_id, customer_surr_id, geography_surr_id,
                employee_surr_id, order_attr_surr_id, order_id,
                sales_amt, cost_amt, profit_amt, shipping_cost_amt,
                quantity_cnt, discount_amt, profit_margin_amt,
                insert_dt, update_dt, source_system, source_entity
            )
            SELECT
                s.event_dt,
                s.date_id,
                COALESCE(p.product_surr_id,    -1),
                COALESCE(c.customer_surr_id,   -1),
                COALESCE(g.geography_surr_id,  -1),
                COALESCE(e.employee_surr_id,   -1),
                COALESCE(oa.order_attr_surr_id,-1),
                s.order_id,
                s.sales_amt, s.cost_amt, s.profit_amt,
                s.shipping_cost_amt, s.quantity_cnt, s.discount_amt,
                CASE
                    WHEN s.sales_amt IS NOT NULL AND s.sales_amt != 0
                    THEN ROUND(s.profit_amt / s.sales_amt, 4)
                    ELSE NULL
                END AS profit_margin_amt,
                CURRENT_DATE, CURRENT_DATE,
                s.source_system, s.source_entity
            FROM bl_3nf.ce_sales s
            LEFT JOIN bl_3nf.ce_products cp
                ON cp.product_id = s.product_id
            LEFT JOIN bl_dm.dim_products p
                ON p.product_src_id = cp.product_src_id
            LEFT JOIN bl_3nf.ce_customers_scd cc
                ON cc.customer_id = s.customer_id AND cc.is_active = ''Y''
            LEFT JOIN bl_dm.dim_customers_scd c
                ON c.customer_src_id = cc.customer_src_id AND c.is_active = ''Y''
            LEFT JOIN bl_3nf.ce_cities cty
                ON cty.city_id = s.city_id
            LEFT JOIN bl_dm.dim_geography g
                ON g.geography_src_id = cty.city_src_id
            LEFT JOIN bl_3nf.ce_employees ce
                ON ce.employee_id = s.employee_id
            LEFT JOIN bl_dm.dim_employees e
                ON e.employee_src_id = ce.employee_src_id
            LEFT JOIN bl_3nf.ce_order_attributes coa
                ON coa.order_attr_id = s.order_attr_id
            LEFT JOIN bl_dm.dim_order_attributes oa
                ON oa.ship_mode      = coa.ship_mode
               AND oa.order_priority = coa.order_priority
            WHERE s.event_dt >= $1
              AND s.event_dt <  $2
        ' USING v_month_start, v_month_end;

        GET DIAGNOSTICS v_row_count = ROW_COUNT;
        RAISE NOTICE '  staged % rows for month %', v_row_count, v_month_start;

        -- STEP D: detach and drop the existing partition if it exists
        IF EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'bl_dm' AND c.relname = v_partition_name
        ) THEN
            EXECUTE 'ALTER TABLE bl_dm.fct_sales_dd DETACH PARTITION bl_dm.' || v_partition_name;
            EXECUTE 'DROP TABLE bl_dm.' || v_partition_name;
            RAISE NOTICE '  dropped old partition: %', v_partition_name;
        END IF;

        -- STEP E: rename staging -> partition name, then ATTACH
        EXECUTE 'ALTER TABLE bl_dm.' || v_staging_name || ' RENAME TO ' || v_partition_name;

        EXECUTE '
            ALTER TABLE bl_dm.fct_sales_dd
            ATTACH PARTITION bl_dm.' || v_partition_name || '
            FOR VALUES FROM (''' || v_month_start || ''')
                        TO  (''' || v_month_end   || ''')
        ';

        -- STEP F: index on the newly attached partition
        EXECUTE '
            CREATE INDEX IF NOT EXISTS idx_' || v_partition_name || '_event_dt
            ON bl_dm.' || v_partition_name || ' (event_dt)
        ';

        v_count       := v_count + v_row_count;
        v_month_count := v_month_count + 1;
    END LOOP;

    CALL bl_cl.prc_log(v_proc, v_count, 'SUCCESS',
        'Rolling window: refreshed ' || v_month_count
        || ' month partitions, ' || v_count || ' total rows in bl_dm.fct_sales_dd');

    COMMIT;

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'SQLERRM: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


-- SECTION 5: MASTER PROCEDURE FOR ALL FACTS
-----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_all_facts()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc CONSTANT VARCHAR := 'bl_cl.prc_load_all_facts';
BEGIN
    RAISE NOTICE '% started at %', v_proc, NOW();

    CALL bl_cl.prc_load_ce_sales_incremental();
    CALL bl_cl.prc_load_fct_sales_dd_rolling();

    CALL bl_cl.prc_log(v_proc, 0, 'SUCCESS',
        'All fact load procedures completed successfully');

    RAISE NOTICE '% finished at %', v_proc, NOW();

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'Master facts procedure failed: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


-- SECTION 6: RUN 1 + VERIFY
-----------------------------------------------------------------------------
CALL bl_cl.prc_load_all_dm_dims();
CALL bl_cl.prc_load_all_facts();

SELECT procedure_name, rows_affected, status, log_message, log_dt
FROM bl_cl.mta_load_log
ORDER BY log_dt DESC, log_id DESC
LIMIT 15;

SELECT 'DIM_PRODUCTS'         AS table_name, COUNT(*) AS total_rows, COUNT(*) FILTER (WHERE product_surr_id = -1)    AS default_rows FROM bl_dm.dim_products
UNION ALL
SELECT 'DIM_CUSTOMERS_SCD',   COUNT(*), COUNT(*) FILTER (WHERE customer_surr_id = -1)   FROM bl_dm.dim_customers_scd
UNION ALL
SELECT 'DIM_GEOGRAPHY',       COUNT(*), COUNT(*) FILTER (WHERE geography_surr_id = -1)  FROM bl_dm.dim_geography
UNION ALL
SELECT 'DIM_EMPLOYEES',       COUNT(*), COUNT(*) FILTER (WHERE employee_surr_id = -1)   FROM bl_dm.dim_employees
UNION ALL
SELECT 'DIM_ORDER_ATTRIBUTES',COUNT(*), COUNT(*) FILTER (WHERE order_attr_surr_id = -1) FROM bl_dm.dim_order_attributes
UNION ALL
SELECT 'FCT_SALES_DD',        COUNT(*), 0                                                FROM bl_dm.fct_sales_dd;


-- SECTION 7: COMPLETE SCD2 TEST (5 steps, satisfies mentor requirement)
-- This test demonstrates the full SCD2 flow:
-- source data changes -> 3NF procedure detects it -> DM procedure versions it.
-----------------------------------------------------------------------------

-- STEP 7.1: Record the BEFORE state of a chosen customer
-- Run this query, note the customer_src_id returned, replace below.
SELECT customer_src_id, customer_name, customer_segment, is_active
FROM bl_3nf.ce_customers_scd
WHERE customer_segment = 'Consumer'
  AND is_active = 'Y'
  AND customer_id != -1
ORDER BY customer_id
LIMIT 1;
-- screenshot: scd2_step1_before_3nf.png

-- Also show matching DM row
SELECT customer_surr_id, customer_src_id, customer_name,
       customer_segment, start_dt, end_dt, is_active
FROM bl_dm.dim_customers_scd
WHERE customer_src_id = (
    SELECT customer_src_id FROM bl_3nf.ce_customers_scd
    WHERE customer_segment = 'Consumer' AND is_active = 'Y' AND customer_id != -1
    ORDER BY customer_id LIMIT 1
)
ORDER BY start_dt;
-- screenshot: scd2_step1_before_dm.png


-- STEP 7.2: Simulate source data change — segment updated in SRC_ table
-- (In a real incremental load this arrives via the updated CSV.
--  Here we update SRC_ directly to simulate the source system sending new data.)
UPDATE sa_domestic.src_domestic_sales
SET segment = 'Corporate'
WHERE customer_id = (
    SELECT customer_src_id FROM bl_3nf.ce_customers_scd
    WHERE customer_segment = 'Consumer' AND is_active = 'Y' AND customer_id != -1
    ORDER BY customer_id LIMIT 1
);
-- screenshot: scd2_step2_source_updated.png


-- STEP 7.3: Run the 3NF customer procedure — detects change, creates new version
CALL bl_cl.prc_load_customers_scd();

SELECT customer_id, customer_src_id, customer_name,
       customer_segment, start_dt, end_dt, is_active
FROM bl_3nf.ce_customers_scd
WHERE customer_src_id = (
    SELECT customer_src_id FROM bl_3nf.ce_customers_scd
    WHERE customer_segment = 'Corporate' AND is_active = 'Y' AND customer_id != -1
    ORDER BY customer_id LIMIT 1
)
ORDER BY start_dt;
-- screenshot: scd2_step3_after_3nf.png
-- EXPECTED: 2 rows — original (IS_ACTIVE=N) + new version (IS_ACTIVE=Y, Corporate)


-- STEP 7.4: Run the DM customer SCD2 procedure — mirrors the versioning in DM
CALL bl_cl.prc_load_dim_customers_scd();

SELECT customer_surr_id, customer_src_id, customer_name,
       customer_segment, start_dt, end_dt, is_active, update_dt
FROM bl_dm.dim_customers_scd
WHERE customer_src_id = (
    SELECT customer_src_id FROM bl_3nf.ce_customers_scd
    WHERE customer_segment = 'Corporate' AND is_active = 'Y' AND customer_id != -1
    ORDER BY customer_id LIMIT 1
)
ORDER BY start_dt;
-- screenshot: scd2_step4_after_dm.png
-- EXPECTED: 2 rows — expired version (IS_ACTIVE=N) + new version (IS_ACTIVE=Y, Corporate)


-- STEP 7.5: Log confirmation — rows_affected > 0 for both procedures
SELECT procedure_name, rows_affected, status, log_message, log_dt
FROM bl_cl.mta_load_log
WHERE procedure_name IN (
    'bl_cl.prc_load_customers_scd',
    'bl_cl.prc_load_dim_customers_scd'
)
ORDER BY log_dt DESC
LIMIT 6;
-- screenshot: scd2_step5_log.png


-- SECTION 8: IDEMPOTENCY CHECK (Run 2)
-----------------------------------------------------------------------------
CALL bl_cl.prc_load_all_dm_dims();
CALL bl_cl.prc_load_all_facts();

SELECT procedure_name, rows_affected, status, log_message, log_dt
FROM bl_cl.mta_load_log
ORDER BY log_dt DESC, log_id DESC
LIMIT 15;
-- EXPECTED:
-- prc_load_ce_sales_incremental: rows_affected = 0 (high-water mark unchanged)
-- prc_load_fct_sales_dd_rolling: rows_affected = same as run 1 (rolling window re-loads 2 months by design)
-- All dim procedures: rows_affected = 0 or total processed with no actual data change