-- BL_3NF DML: Loading procedures for all CE_ tables
-- Global Retail Superstore Sales
-- Tamar Tutisani
--
-- Contains:
--   - fn_get_source_categories() : function returning SETOF composite type
--   - prc_load_product_categories()
--   - prc_load_product_subcategories()
--   - prc_load_products()
--   - prc_load_markets()
--   - prc_load_regions()
--   - prc_load_countries()
--   - prc_load_states()
--   - prc_load_cities()
--   - prc_load_customers_scd()  <- FIXED: includes full SCD2 versioning logic
--   - prc_load_employees()
--   - prc_load_order_attributes()
--   - prc_load_all_3nf()  (master procedure)
--
-- Every procedure is idempotent (WHERE NOT EXISTS / ON CONFLICT guards).
-- Every procedure has a EXCEPTION block and calls prc_log on success/error.
-- Loading order respects FK dependencies.
-----------------------------------------------------------------------------


-- SECTION 1: COMPOSITE TYPE + FUNCTION RETURNING SETOF TYPE
-----------------------------------------------------------------------------

-- Composite type for category rows (used by prc_load_product_categories)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type t
                   INNER JOIN pg_namespace n ON n.oid = t.typnamespace
                   WHERE t.typname = 't_category_row' AND n.nspname = 'bl_cl') THEN
        CREATE TYPE bl_cl.t_category_row AS (
            category_name VARCHAR(100)
        );
    END IF;
END;
$$;

-- Function returning SETOF composite type
-- Returns distinct non-null category names from both source systems.
CREATE OR REPLACE FUNCTION bl_cl.fn_get_source_categories()
RETURNS SETOF bl_cl.t_category_row
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT TRIM(category)::VARCHAR(100)
    FROM sa_domestic.src_domestic_sales
    WHERE category IS NOT NULL AND TRIM(category) != ''
    UNION
    SELECT DISTINCT TRIM(category)::VARCHAR(100)
    FROM sa_international.src_international_sales
    WHERE category IS NOT NULL AND TRIM(category) != '';
END;
$$;


-- SECTION 2: DIMENSION LOAD PROCEDURES
-----------------------------------------------------------------------------

-- 2.1 CE_PRODUCT_CATEGORIES
-- Uses fn_get_source_categories() in a FOR LOOP (satisfies "FOR loop over
-- function returning table" requirement).
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_product_categories()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc CONSTANT VARCHAR := 'bl_cl.prc_load_product_categories';
    v_count INTEGER := 0;
    v_row bl_cl.t_category_row;
BEGIN
    FOR v_row IN
        SELECT * FROM bl_cl.fn_get_source_categories()
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM bl_3nf.ce_product_categories
            WHERE product_category_src_id = v_row.category_name
        ) THEN
            INSERT INTO bl_3nf.ce_product_categories (
                product_category_id, product_category_src_id, product_category_name,
                insert_dt, update_dt, source_system, source_entity
            )
            VALUES (
                nextval('bl_3nf.seq_product_category_id'),
                COALESCE(v_row.category_name, 'n.a.'),
                COALESCE(v_row.category_name, 'n.a.'),
                CURRENT_DATE, CURRENT_DATE,
                'SA_COMBINED',
                'SRC_DOMESTIC_SALES,SRC_INTERNATIONAL_SALES'
            );
            v_count := v_count + 1;
        END IF;
    END LOOP;

    CALL bl_cl.prc_log(v_proc, v_count, 'SUCCESS',
        'Loaded ' || v_count || ' new rows into bl_3nf.ce_product_categories');

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'SQLERRM: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


-- 2.2 CE_PRODUCT_SUBCATEGORIES
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_product_subcategories()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc CONSTANT VARCHAR := 'bl_cl.prc_load_product_subcategories';
    v_count INTEGER := 0;
    v_row RECORD;
    v_cat_id BIGINT;
BEGIN
    FOR v_row IN
        SELECT DISTINCT
            TRIM(sub_category)::VARCHAR(100) AS sub_category_name,
            TRIM(category)::VARCHAR(100) AS category_name
        FROM sa_domestic.src_domestic_sales
        WHERE sub_category IS NOT NULL AND TRIM(sub_category) != ''
        UNION
        SELECT DISTINCT
            TRIM(sub_category)::VARCHAR(100),
            TRIM(category)::VARCHAR(100)
        FROM sa_international.src_international_sales
        WHERE sub_category IS NOT NULL AND TRIM(sub_category) != ''
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM bl_3nf.ce_product_subcategories
            WHERE product_subcategory_src_id = v_row.sub_category_name
        ) THEN
            SELECT COALESCE(product_category_id, -1)
            INTO v_cat_id
            FROM bl_3nf.ce_product_categories
            WHERE product_category_src_id = v_row.category_name;

            INSERT INTO bl_3nf.ce_product_subcategories (
                product_subcategory_id, product_subcategory_src_id, product_subcategory_name,
                product_category_id, insert_dt, update_dt, source_system, source_entity
            )
            VALUES (
                nextval('bl_3nf.seq_product_subcategory_id'),
                COALESCE(v_row.sub_category_name, 'n.a.'),
                COALESCE(v_row.sub_category_name, 'n.a.'),
                COALESCE(v_cat_id, -1),
                CURRENT_DATE, CURRENT_DATE,
                'SA_COMBINED',
                'SRC_DOMESTIC_SALES,SRC_INTERNATIONAL_SALES'
            );
            v_count := v_count + 1;
        END IF;
    END LOOP;

    CALL bl_cl.prc_log(v_proc, v_count, 'SUCCESS',
        'Loaded ' || v_count || ' new rows into bl_3nf.ce_product_subcategories');

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'SQLERRM: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


-- 2.3 CE_PRODUCTS
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_products()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc   CONSTANT VARCHAR := 'bl_cl.prc_load_products';
    v_count  INTEGER := 0;
    v_row    RECORD;
    v_sub_id BIGINT;
BEGIN
    FOR v_row IN
        SELECT DISTINCT ON (product_src_id)
            product_src_id, product_name, sub_category
        FROM (
            SELECT DISTINCT
                TRIM(product_id)::VARCHAR(50)   AS product_src_id,
                TRIM(product_name)::VARCHAR(255) AS product_name,
                TRIM(sub_category)::VARCHAR(100) AS sub_category
            FROM sa_domestic.src_domestic_sales
            WHERE product_id IS NOT NULL AND TRIM(product_id) != ''
            UNION ALL
            SELECT DISTINCT
                TRIM(product_id)::VARCHAR(50),
                TRIM(product_name)::VARCHAR(255),
                TRIM(sub_category)::VARCHAR(100)
            FROM sa_international.src_international_sales
            WHERE product_id IS NOT NULL AND TRIM(product_id) != ''
        ) all_products
        ORDER BY product_src_id
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM bl_3nf.ce_products
            WHERE product_src_id = v_row.product_src_id
        ) THEN
            SELECT COALESCE(product_subcategory_id, -1)
            INTO v_sub_id
            FROM bl_3nf.ce_product_subcategories
            WHERE product_subcategory_src_id = v_row.sub_category;

            INSERT INTO bl_3nf.ce_products (
                product_id, product_src_id, product_name, product_subcategory_id,
                insert_dt, update_dt, source_system, source_entity
            )
            VALUES (
                nextval('bl_3nf.seq_product_id'),
                COALESCE(v_row.product_src_id, 'n.a.'),
                COALESCE(v_row.product_name, 'n.a.'),
                COALESCE(v_sub_id, -1),
                CURRENT_DATE, CURRENT_DATE,
                'SA_COMBINED',
                'SRC_DOMESTIC_SALES,SRC_INTERNATIONAL_SALES'
            );
            v_count := v_count + 1;
        END IF;
    END LOOP;

    CALL bl_cl.prc_log(v_proc, v_count, 'SUCCESS',
        'Loaded ' || v_count || ' new rows into bl_3nf.ce_products');

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'SQLERRM: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


-- 2.4 CE_MARKETS
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_markets()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc  CONSTANT VARCHAR := 'bl_cl.prc_load_markets';
    v_count INTEGER := 0;
    v_row   RECORD;
BEGIN
    FOR v_row IN
        SELECT DISTINCT TRIM(market)::VARCHAR(50) AS market_name
        FROM sa_domestic.src_domestic_sales
        WHERE market IS NOT NULL AND TRIM(market) != ''
        UNION
        SELECT DISTINCT TRIM(market)::VARCHAR(50)
        FROM sa_international.src_international_sales
        WHERE market IS NOT NULL AND TRIM(market) != ''
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM bl_3nf.ce_markets WHERE market_src_id = v_row.market_name
        ) THEN
            INSERT INTO bl_3nf.ce_markets (
                market_id, market_src_id, market_name,
                insert_dt, update_dt, source_system, source_entity
            )
            VALUES (
                nextval('bl_3nf.seq_market_id'),
                COALESCE(v_row.market_name, 'n.a.'),
                COALESCE(v_row.market_name, 'n.a.'),
                CURRENT_DATE, CURRENT_DATE,
                'SA_COMBINED',
                'SRC_DOMESTIC_SALES,SRC_INTERNATIONAL_SALES'
            );
            v_count := v_count + 1;
        END IF;
    END LOOP;

    CALL bl_cl.prc_log(v_proc, v_count, 'SUCCESS',
        'Loaded ' || v_count || ' new rows into bl_3nf.ce_markets');

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'SQLERRM: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


-- 2.5 CE_REGIONS
-- region_src_id = region_name || '_' || market_name (globally unique composite key)
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_regions()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc   CONSTANT VARCHAR := 'bl_cl.prc_load_regions';
    v_count  INTEGER := 0;
    v_row    RECORD;
    v_mkt_id BIGINT;
    v_src_id VARCHAR(255);
BEGIN
    FOR v_row IN
        SELECT DISTINCT
            TRIM(region)::VARCHAR(100) AS region_name,
            TRIM(market)::VARCHAR(50)  AS market_name
        FROM sa_domestic.src_domestic_sales
        WHERE region IS NOT NULL AND TRIM(region) != ''
        UNION
        SELECT DISTINCT
            TRIM(region)::VARCHAR(100),
            TRIM(market)::VARCHAR(50)
        FROM sa_international.src_international_sales
        WHERE region IS NOT NULL AND TRIM(region) != ''
    LOOP
        v_src_id := v_row.region_name || '_' || v_row.market_name;

        IF NOT EXISTS (
            SELECT 1 FROM bl_3nf.ce_regions WHERE region_src_id = v_src_id
        ) THEN
            SELECT COALESCE(market_id, -1)
            INTO v_mkt_id
            FROM bl_3nf.ce_markets
            WHERE market_src_id = v_row.market_name;

            INSERT INTO bl_3nf.ce_regions (
                region_id, region_src_id, region_name, market_id,
                insert_dt, update_dt, source_system, source_entity
            )
            VALUES (
                nextval('bl_3nf.seq_region_id'),
                v_src_id,
                COALESCE(v_row.region_name, 'n.a.'),
                COALESCE(v_mkt_id, -1),
                CURRENT_DATE, CURRENT_DATE,
                'SA_COMBINED',
                'SRC_DOMESTIC_SALES,SRC_INTERNATIONAL_SALES'
            );
            v_count := v_count + 1;
        END IF;
    END LOOP;

    CALL bl_cl.prc_log(v_proc, v_count, 'SUCCESS',
        'Loaded ' || v_count || ' new rows into bl_3nf.ce_regions');

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'SQLERRM: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


-- 2.6 CE_COUNTRIES
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_countries()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc   CONSTANT VARCHAR := 'bl_cl.prc_load_countries';
    v_count  INTEGER := 0;
    v_row    RECORD;
    v_reg_id BIGINT;
BEGIN
    FOR v_row IN
        SELECT DISTINCT ON (country_name)
            country_name, region_name, market_name
        FROM (
            SELECT DISTINCT
                TRIM(country)::VARCHAR(100) AS country_name,
                TRIM(region)::VARCHAR(100)  AS region_name,
                TRIM(market)::VARCHAR(50)   AS market_name
            FROM sa_domestic.src_domestic_sales
            WHERE country IS NOT NULL AND TRIM(country) != ''
            UNION
            SELECT DISTINCT
                TRIM(country)::VARCHAR(100),
                TRIM(region)::VARCHAR(100),
                TRIM(market)::VARCHAR(50)
            FROM sa_international.src_international_sales
            WHERE country IS NOT NULL AND TRIM(country) != ''
        ) all_countries
        ORDER BY country_name
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM bl_3nf.ce_countries WHERE country_src_id = v_row.country_name
        ) THEN
            SELECT COALESCE(region_id, -1)
            INTO v_reg_id
            FROM bl_3nf.ce_regions
            WHERE region_src_id = (v_row.region_name || '_' || v_row.market_name);

            INSERT INTO bl_3nf.ce_countries (
                country_id, country_src_id, country_name, region_id,
                insert_dt, update_dt, source_system, source_entity
            )
            VALUES (
                nextval('bl_3nf.seq_country_id'),
                COALESCE(v_row.country_name, 'n.a.'),
                COALESCE(v_row.country_name, 'n.a.'),
                COALESCE(v_reg_id, -1),
                CURRENT_DATE, CURRENT_DATE,
                'SA_COMBINED',
                'SRC_DOMESTIC_SALES,SRC_INTERNATIONAL_SALES'
            );
            v_count := v_count + 1;
        END IF;
    END LOOP;

    CALL bl_cl.prc_log(v_proc, v_count, 'SUCCESS',
        'Loaded ' || v_count || ' new rows into bl_3nf.ce_countries');

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'SQLERRM: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


-- 2.7 CE_STATES
-- Domestic rows produce state_name = 'N/A' (no state column in domestic source).
-- state_src_id = state_name || '_' || country_name (globally unique composite key).
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_states()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc CONSTANT VARCHAR := 'bl_cl.prc_load_states';
    v_count INTEGER := 0;
    v_row RECORD;
    v_cty_id BIGINT;
    v_src_id VARCHAR(255);
BEGIN
    FOR v_row IN
        SELECT DISTINCT
            'N/A'::VARCHAR(100) AS state_name,
            TRIM(country)::VARCHAR(100) AS country_name
        FROM sa_domestic.src_domestic_sales
        WHERE country IS NOT NULL AND TRIM(country) != ''
        UNION
        SELECT DISTINCT
            TRIM(state)::VARCHAR(100) AS state_name,
            TRIM(country)::VARCHAR(100) AS country_name
        FROM sa_international.src_international_sales
        WHERE state IS NOT NULL AND TRIM(state) != ''
    LOOP
        v_src_id := v_row.state_name || '_' || v_row.country_name;

        IF NOT EXISTS (
            SELECT 1 FROM bl_3nf.ce_states WHERE state_src_id = v_src_id
        ) THEN
            SELECT COALESCE(country_id, -1)
            INTO v_cty_id
            FROM bl_3nf.ce_countries
            WHERE country_src_id = v_row.country_name;

            INSERT INTO bl_3nf.ce_states (
                state_id, state_src_id, state_name, country_id,
                insert_dt, update_dt, source_system, source_entity
            )
            VALUES (
                nextval('bl_3nf.seq_state_id'),
                v_src_id,
                COALESCE(v_row.state_name, 'n.a.'),
                COALESCE(v_cty_id, -1),
                CURRENT_DATE, CURRENT_DATE,
                'SA_COMBINED',
                'SRC_DOMESTIC_SALES,SRC_INTERNATIONAL_SALES'
            );
            v_count := v_count + 1;
        END IF;
    END LOOP;

    CALL bl_cl.prc_log(v_proc, v_count, 'SUCCESS',
        'Loaded ' || v_count || ' new rows into bl_3nf.ce_states');

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'SQLERRM: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


-- 2.8 CE_CITIES
-- city_src_id = city || '_' || country || '_' || region (composite natural key)
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_cities()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc  CONSTANT VARCHAR := 'bl_cl.prc_load_cities';
    v_count INTEGER := 0;
    v_row   RECORD;
    v_st_id BIGINT;
BEGIN
    FOR v_row IN
        SELECT DISTINCT ON (city_key)
            city_key, city_name, state_name, country_name
        FROM (
            SELECT DISTINCT
                TRIM(city)::VARCHAR(100) || '_' ||
                TRIM(country)::VARCHAR(100) || '_' ||
                TRIM(region)::VARCHAR(100)  AS city_key,
                TRIM(city)::VARCHAR(100)    AS city_name,
                'N/A'::VARCHAR(100)         AS state_name,
                TRIM(country)::VARCHAR(100) AS country_name
            FROM sa_domestic.src_domestic_sales
            WHERE city IS NOT NULL AND TRIM(city) != ''
            UNION ALL
            SELECT DISTINCT
                TRIM(city)::VARCHAR(100) || '_' ||
                TRIM(country)::VARCHAR(100) || '_' ||
                TRIM(region)::VARCHAR(100)  AS city_key,
                TRIM(city)::VARCHAR(100)    AS city_name,
                COALESCE(NULLIF(TRIM(state), ''), 'N/A')::VARCHAR(100) AS state_name,
                TRIM(country)::VARCHAR(100) AS country_name
            FROM sa_international.src_international_sales
            WHERE city IS NOT NULL AND TRIM(city) != ''
        ) all_cities
        ORDER BY city_key
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM bl_3nf.ce_cities WHERE city_src_id = v_row.city_key
        ) THEN
            SELECT COALESCE(state_id, -1)
            INTO v_st_id
            FROM bl_3nf.ce_states
            WHERE state_src_id = (v_row.state_name || '_' || v_row.country_name);

            INSERT INTO bl_3nf.ce_cities (
                city_id, city_src_id, city_name, state_id,
                insert_dt, update_dt, source_system, source_entity
            )
            VALUES (
                nextval('bl_3nf.seq_city_id'),
                COALESCE(v_row.city_key, 'n.a.'),
                COALESCE(v_row.city_name, 'n.a.'),
                COALESCE(v_st_id, -1),
                CURRENT_DATE, CURRENT_DATE,
                'SA_COMBINED',
                'SRC_DOMESTIC_SALES,SRC_INTERNATIONAL_SALES'
            );
            v_count := v_count + 1;
        END IF;
    END LOOP;

    CALL bl_cl.prc_log(v_proc, v_count, 'SUCCESS',
        'Loaded ' || v_count || ' new rows into bl_3nf.ce_cities');

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'SQLERRM: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


-- 2.9 CE_CUSTOMERS_SCD (SCD Type 2)
-- Three cases handled:
--   A) New customer (never seen before)   -> insert first active version
--   B) Existing customer, attribute changed -> expire old row, insert new version
--   C) Existing customer, no change       -> skip
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_customers_scd()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc CONSTANT VARCHAR := 'bl_cl.prc_load_customers_scd';
    v_count_new INTEGER := 0;
    v_count_scd INTEGER := 0;
    v_row RECORD;
    v_existing_seg VARCHAR(50);
    v_existing_name VARCHAR(255);
    v_old_start_dt DATE;
    v_new_end_dt DATE;
BEGIN
    FOR v_row IN
        -- Final DISTINCT ON ensures each customer_src_id enters the loop exactly once,
        -- even if the same ID appears in both source systems.
        -- SA_DOMESTIC sorts before SA_INTERNATIONAL so the domestic row (with name)
        -- is kept when the same customer exists in both sources.
        -- Note: DISTINCT ON subqueries with ORDER BY must be wrapped in their own
        -- subselect before being combined with UNION ALL — PostgreSQL syntax rule.
        SELECT DISTINCT ON (customer_src_id)
            customer_src_id, customer_name, customer_segment,
            source_system, source_entity
        FROM (
            SELECT * FROM (
                SELECT DISTINCT ON (customer_id)
                    customer_id AS customer_src_id,
                    customer_name,
                    segment AS customer_segment,
                    'SA_DOMESTIC' AS source_system,
                    'SRC_DOMESTIC_SALES' AS source_entity
                FROM sa_domestic.src_domestic_sales
                WHERE customer_id IS NOT NULL AND TRIM(customer_id) != ''
                ORDER BY customer_id, customer_name
            ) dom

            UNION ALL

            SELECT * FROM (
                SELECT DISTINCT ON (customer_id)
                    customer_id AS customer_src_id,
                    customer_name,
                    segment  AS customer_segment,
                    'SA_INTERNATIONAL' AS source_system,
                    'SRC_INTERNATIONAL_SALES' AS source_entity
                FROM sa_international.src_international_sales
                WHERE customer_id IS NOT NULL AND TRIM(customer_id) != ''
                ORDER BY customer_id, customer_name
            ) intl
        ) combined
        ORDER BY customer_src_id, source_system
    LOOP
        -- Check if this customer already has an active row in CE_CUSTOMERS_SCD
        SELECT customer_segment, customer_name
        INTO v_existing_seg, v_existing_name
        FROM bl_3nf.ce_customers_scd
        WHERE customer_src_id = v_row.customer_src_id
          AND is_active = 'Y';

        IF NOT FOUND THEN
            -- CASE A: brand new customer - insert first version
            INSERT INTO bl_3nf.ce_customers_scd (
                customer_id, customer_src_id, customer_name, customer_segment,
                start_dt, end_dt, is_active, insert_dt, update_dt, source_system, source_entity
            )
            VALUES (
                nextval('bl_3nf.seq_customer_id'),
                v_row.customer_src_id,
                COALESCE(v_row.customer_name,    'n.a.'),
                COALESCE(v_row.customer_segment, 'n.a.'),
                '1990-01-01'::DATE,
                '9999-12-31'::DATE,
                'Y',
                CURRENT_DATE,
                CURRENT_DATE,
                v_row.source_system,
                v_row.source_entity
            );
            v_count_new := v_count_new + 1;

        ELSIF v_existing_seg IS DISTINCT FROM v_row.customer_segment
           OR v_existing_name IS DISTINCT FROM v_row.customer_name THEN
            -- CASE B: attribute changed - SCD2 versioning

            -- Get start_dt of the row about to be expired
            SELECT start_dt INTO v_old_start_dt
            FROM bl_3nf.ce_customers_scd
            WHERE customer_src_id = v_row.customer_src_id AND is_active = 'Y';

            -- end_dt must be strictly > start_dt
            v_new_end_dt := GREATEST(v_old_start_dt + 1, CURRENT_DATE);

            -- Expire the current active row
            UPDATE bl_3nf.ce_customers_scd
            SET    end_dt = v_new_end_dt,
                   is_active = 'N',
                   update_dt = CURRENT_DATE
            WHERE  customer_src_id = v_row.customer_src_id
              AND  is_active = 'Y';

            -- Insert the new active version
            INSERT INTO bl_3nf.ce_customers_scd (
                customer_id, customer_src_id, customer_name, customer_segment,
                start_dt, end_dt, is_active, insert_dt, update_dt, source_system, source_entity
            )
            VALUES (
                nextval('bl_3nf.seq_customer_id'),
                v_row.customer_src_id,
                COALESCE(v_row.customer_name,    'n.a.'),
                COALESCE(v_row.customer_segment, 'n.a.'),
                v_new_end_dt + 1,
                '9999-12-31'::DATE,
                'Y',
                CURRENT_DATE,
                CURRENT_DATE,
                v_row.source_system,
                v_row.source_entity
            );
            v_count_scd := v_count_scd + 1;

        -- CASE C: no change — do nothing
        END IF;
    END LOOP;

    CALL bl_cl.prc_log(v_proc, v_count_new + v_count_scd, 'SUCCESS',
        'CE_CUSTOMERS_SCD: ' || v_count_new || ' new inserts, '
        || v_count_scd || ' SCD2 version changes');

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'SQLERRM: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


-- 2.10 CE_EMPLOYEES
-- Domestic gives employee_id + employee_name.
-- International gives employee_id only (name = 'n.a.').
-- DISTINCT ON ordered by employee_name NULLS LAST keeps the domestic row
-- (with name) when the same employee appears in both sources.
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_employees()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc  CONSTANT VARCHAR := 'bl_cl.prc_load_employees';
    v_count INTEGER := 0;
    v_row   RECORD;
BEGIN
    FOR v_row IN
        SELECT DISTINCT ON (employee_src_id)
            employee_src_id, employee_name
        FROM (
            SELECT DISTINCT
                TRIM(employee_id)::VARCHAR(50)   AS employee_src_id,
                TRIM(employee_name)::VARCHAR(255) AS employee_name
            FROM sa_domestic.src_domestic_sales
            WHERE employee_id IS NOT NULL AND TRIM(employee_id) != ''
            UNION ALL
            SELECT DISTINCT
                TRIM(employee_id)::VARCHAR(50),
                NULL::VARCHAR(255)
            FROM sa_international.src_international_sales
            WHERE employee_id IS NOT NULL AND TRIM(employee_id) != ''
        ) all_employees
        ORDER BY employee_src_id, employee_name NULLS LAST
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM bl_3nf.ce_employees WHERE employee_src_id = v_row.employee_src_id
        ) THEN
            INSERT INTO bl_3nf.ce_employees (
                employee_id, employee_src_id, employee_name,
                insert_dt, update_dt, source_system, source_entity
            )
            VALUES (
                nextval('bl_3nf.seq_employee_id'),
                COALESCE(v_row.employee_src_id, 'n.a.'),
                COALESCE(v_row.employee_name,   'n.a.'),
                CURRENT_DATE, CURRENT_DATE,
                'SA_COMBINED',
                'SRC_DOMESTIC_SALES,SRC_INTERNATIONAL_SALES'
            );
            v_count := v_count + 1;
        END IF;
    END LOOP;

    CALL bl_cl.prc_log(v_proc, v_count, 'SUCCESS',
        'Loaded ' || v_count || ' new rows into bl_3nf.ce_employees');

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'SQLERRM: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


-- 2.11 CE_ORDER_ATTRIBUTES (junk dimension)
-- Only from Domestic source. International rows reference the -1 default row.
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_order_attributes()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc CONSTANT VARCHAR := 'bl_cl.prc_load_order_attributes';
    v_count INTEGER := 0;
    v_row RECORD;
BEGIN
    FOR v_row IN
        SELECT DISTINCT
            COALESCE(NULLIF(TRIM(ship_mode),      ''), 'n.a.')::VARCHAR(50) AS ship_mode,
            COALESCE(NULLIF(TRIM(order_priority), ''), 'n.a.')::VARCHAR(25) AS order_priority
        FROM sa_domestic.src_domestic_sales
        WHERE ship_mode IS NOT NULL AND order_priority IS NOT NULL
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM bl_3nf.ce_order_attributes
            WHERE ship_mode = v_row.ship_mode AND order_priority = v_row.order_priority
        ) THEN
            INSERT INTO bl_3nf.ce_order_attributes (
                order_attr_id, ship_mode, order_priority,
                insert_dt, update_dt, source_system, source_entity
            )
            VALUES (
                nextval('bl_3nf.seq_order_attr_id'),
                v_row.ship_mode,
                v_row.order_priority,
                CURRENT_DATE, CURRENT_DATE,
                'SA_DOMESTIC',
                'SRC_DOMESTIC_SALES'
            );
            v_count := v_count + 1;
        END IF;
    END LOOP;

    CALL bl_cl.prc_log(v_proc, v_count, 'SUCCESS',
        'Loaded ' || v_count || ' new rows into bl_3nf.ce_order_attributes');

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'SQLERRM: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


-- SECTION 3: MASTER PROCEDURE FOR ALL 3NF DIMENSIONS
-- Calls all dimension procedures in FK dependency order.
-- If any procedure fails, the EXCEPTION block logs the error and re-raises,
-- stopping the master procedure immediately.
-----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_all_3nf()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc CONSTANT VARCHAR := 'bl_cl.prc_load_all_3nf';
BEGIN
    RAISE NOTICE '% started at %', v_proc, NOW();

    CALL bl_cl.prc_load_product_categories();
    CALL bl_cl.prc_load_product_subcategories();
    CALL bl_cl.prc_load_products();
    CALL bl_cl.prc_load_markets();
    CALL bl_cl.prc_load_regions();
    CALL bl_cl.prc_load_countries();
    CALL bl_cl.prc_load_states();
    CALL bl_cl.prc_load_cities();
    CALL bl_cl.prc_load_customers_scd();
    CALL bl_cl.prc_load_employees();
    CALL bl_cl.prc_load_order_attributes();

    CALL bl_cl.prc_log(v_proc, 0, 'SUCCESS',
        'All 3NF dimension procedures completed successfully');

    RAISE NOTICE '% finished at %', v_proc, NOW();

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'Master 3NF procedure failed: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


-- SECTION 4: PERFORMANCE INDEXES (run before fact load)
-----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_src_dom_product_id ON sa_domestic.src_domestic_sales(product_id);
CREATE INDEX IF NOT EXISTS idx_src_dom_customer_id ON sa_domestic.src_domestic_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_src_dom_employee_id ON sa_domestic.src_domestic_sales(employee_id);
CREATE INDEX IF NOT EXISTS idx_src_intl_product_id ON sa_international.src_international_sales(product_id);
CREATE INDEX IF NOT EXISTS idx_src_intl_customer_id ON sa_international.src_international_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_src_intl_employee_id ON sa_international.src_international_sales(employee_id);

ANALYZE sa_domestic.src_domestic_sales;
ANALYZE sa_international.src_international_sales;
ANALYZE bl_3nf.ce_dates;
ANALYZE bl_3nf.ce_products;
ANALYZE bl_3nf.ce_customers_scd;
ANALYZE bl_3nf.ce_cities;
ANALYZE bl_3nf.ce_employees;
ANALYZE bl_3nf.ce_order_attributes;


-- SECTION 5: RUN + VERIFY
-----------------------------------------------------------------------------
CALL bl_cl.prc_load_all_3nf();

SELECT procedure_name, rows_affected, status, log_message, log_dt
FROM bl_cl.mta_load_log
ORDER BY log_dt DESC, log_id DESC
LIMIT 15;

SELECT 
    'CE_PRODUCT_CATEGORIES' AS table_name, 
    COUNT(*) AS default_rows 
FROM bl_3nf.ce_product_categories 
WHERE product_category_id = -1
UNION ALL
SELECT 
    'CE_PRODUCT_SUBCATEGORIES', 
    COUNT(*) 
FROM bl_3nf.ce_product_subcategories 
WHERE product_subcategory_id = -1
UNION ALL
SELECT 
    'CE_PRODUCTS', 
    COUNT(*) 
FROM bl_3nf.ce_products 
WHERE product_id = -1
UNION ALL
SELECT 
    'CE_MARKETS', 
    COUNT(*) 
FROM bl_3nf.ce_markets 
WHERE market_id = -1
UNION ALL
SELECT 
    'CE_REGIONS', 
    COUNT(*) 
FROM bl_3nf.ce_regions 
WHERE region_id = -1
UNION ALL
SELECT 
    'CE_COUNTRIES', 
    COUNT(*) 
FROM bl_3nf.ce_countries 
WHERE country_id = -1
UNION ALL
SELECT 
    'CE_STATES', 
    COUNT(*) 
FROM bl_3nf.ce_states 
WHERE state_id = -1
UNION ALL
SELECT 
    'CE_CITIES', 
    COUNT(*) 
FROM bl_3nf.ce_cities 
WHERE city_id = -1
UNION ALL
SELECT 
    'CE_CUSTOMERS_SCD', 
    COUNT(*) 
FROM bl_3nf.ce_customers_scd 
WHERE customer_id = -1
UNION ALL
SELECT 
    'CE_EMPLOYEES', 
    COUNT(*) 
FROM bl_3nf.ce_employees 
WHERE employee_id = -1
UNION ALL
SELECT 
    'CE_ORDER_ATTRIBUTES', 
    COUNT(*) 
FROM bl_3nf.ce_order_attributes 
WHERE order_attr_id = -1;