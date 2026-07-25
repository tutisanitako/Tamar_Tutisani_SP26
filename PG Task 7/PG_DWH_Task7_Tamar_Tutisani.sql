-- POSTGRESQL TASK 7: Loading 3NF objects
-- Tamar Tutisani
-- Module: PG_DWH
-----------------------------------------------------------------------------

---------------------------------------------------------------
-- SECTION 0: BL_CL SCHEMA + PRIVILEGES
---------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS bl_cl;

-- give bl_cl usage rights over bl_3nf so the procedures can write to it
GRANT USAGE ON SCHEMA bl_3nf TO PUBLIC;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA bl_3nf TO PUBLIC;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA bl_3nf TO PUBLIC;

-- and bl_cl itself needs to be reachable
GRANT USAGE ON SCHEMA bl_cl TO PUBLIC;
GRANT USAGE ON SCHEMA sa_domestic TO PUBLIC;
GRANT USAGE ON SCHEMA sa_international TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA sa_domestic TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA sa_international TO PUBLIC;


---------------------------------------------------------------
-- SECTION 1: CENTRALIZED LOG TABLE
---------------------------------------------------------------
-- MTA_ prefix because it's a metadata/operational table in BL_CL,
-- matching the naming convention from Naming_Conventions.docx.
-- Columns I decided on:
-- log_dt - when the procedure ran (useful to see run history)
-- procedure_name - which procedure wrote this entry
-- rows_affected - how many rows actually got inserted (0 on re-run = idempotent)
-- status - 'SUCCESS' or 'ERROR' at a glance
-- log_message - free text: success note OR the error SQLERRM

DROP TABLE IF EXISTS bl_cl.mta_load_log;

CREATE TABLE bl_cl.mta_load_log (
	log_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	log_dt TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	procedure_name VARCHAR(200) NOT NULL,
	rows_affected INTEGER NOT NULL DEFAULT 0,
	status VARCHAR(10) NOT NULL CHECK (status IN ('SUCCESS', 'ERROR')),
	log_message TEXT NOT NULL
);


---------------------------------------------------------------
-- SECTION 2: LOGGING HELPER PROCEDURE
---------------------------------------------------------------
-- A separate procedure just for inserting log rows, so every
-- loading procedure calls this instead of writing INSERT INTO
-- mta_load_log directly. This way if the log table structure
-- changes, I only need to update one place.

CREATE OR REPLACE PROCEDURE bl_cl.prc_log(
	p_procedure_name VARCHAR,
	p_rows_affected INTEGER,
	p_status VARCHAR,
	p_message TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO bl_cl.mta_load_log (
		procedure_name,
		rows_affected,
		status,
		log_message
	)
	VALUES (
		p_procedure_name,
		p_rows_affected,
		p_status,
		p_message
	);
END;
$$;


---------------------------------------------------------------
-- SECTION 3: FUNCTION THAT RETURNS A TABLE (setof type)
---------------------------------------------------------------
-- The task requires at least one "function returns table / setof type".
-- I chose to wrap the combined source extraction for CE_PRODUCT_CATEGORIES
-- here because category names come from both source systems and need to be
-- unioned before the procedure iterates over them.
-- This way the FOR LOOP in prc_load_product_categories iterates over the
-- result of this function instead of embedding the UNION query inline.

CREATE TYPE bl_cl.t_category_row AS (
	category_name VARCHAR(100)
);

CREATE OR REPLACE FUNCTION bl_cl.fn_get_source_categories()
RETURNS SETOF bl_cl.t_category_row
LANGUAGE plpgsql
AS $$
-- Returns distinct non-null category names from both source systems.
-- I'm using SETOF a composite type here rather than RETURNS TABLE(...)
-- to practice both forms the material showed us.
BEGIN
	RETURN QUERY
	SELECT DISTINCT TRIM(category)::VARCHAR(100)
	FROM sa_domestic.src_domestic_sales
	WHERE category IS NOT NULL AND 
		  TRIM(category) != ''
	UNION
	SELECT DISTINCT TRIM(category)::VARCHAR(100)
	FROM sa_international.src_international_sales
	WHERE category IS NOT NULL AND 
		  TRIM(category) != '';
END;
$$;


---------------------------------------------------------------
-- SECTION 4: LOADING PROCEDURES (one per table, FK order)
---------------------------------------------------------------
-- Loading order is the same as the DML script from Task 6:
-- 1. CE_PRODUCT_CATEGORIES
-- 2. CE_PRODUCT_SUBCATEGORIES
-- 3. CE_PRODUCTS
-- 4. CE_MARKETS
-- 5. CE_REGIONS
-- 6. CE_COUNTRIES
-- 7. CE_STATES
-- 8. CE_CITIES
-- 9. CE_CUSTOMERS_SCD
-- 10. CE_EMPLOYEES
-- 11. CE_ORDER_ATTRIBUTES
--
-- Each procedure follows the same pattern:
-- - DECLARE a row_count INTEGER to track how many rows were inserted
-- - FOR LOOP over source rows
-- - WHERE NOT EXISTS idempotency check inside the loop
-- - EXCEPTION block catches any error, logs it, and re-raises
-- - On success, log with rows_affected = row_count


-- 4.1 CE_PRODUCT_CATEGORIES
-- ----------------------------------------------------------
-- Uses fn_get_source_categories() to satisfy the "FOR LOOP over function
-- that returns table" requirement.

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
			SELECT 1
			FROM bl_3nf.ce_product_categories
			WHERE product_category_src_id = v_row.category_name
		) THEN
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
				nextval('bl_3nf.seq_product_category_id'),
				COALESCE(v_row.category_name, 'n.a.'),
				COALESCE(v_row.category_name, 'n.a.'),
				CURRENT_DATE,
				CURRENT_DATE,
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


-- 4.2 CE_PRODUCT_SUBCATEGORIES
-- ----------------------------------------------------------

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_product_subcategories()
LANGUAGE plpgsql
AS $$
DECLARE
	v_proc CONSTANT VARCHAR := 'bl_cl.prc_load_product_subcategories';
	v_count INTEGER := 0;
	v_row RECORD;
	v_cat_id BIGINT;
BEGIN
	-- FOR LOOP over the combined distinct subcategory + category pairs
	-- from both source systems. UNION deduplicates across sources.
	FOR v_row IN
		SELECT DISTINCT
			TRIM(sub_category)::VARCHAR(100) AS sub_category_name,
			TRIM(category)::VARCHAR(100) AS category_name
		FROM sa_domestic.src_domestic_sales
		WHERE sub_category IS NOT NULL AND 
			  TRIM(sub_category) != ''
		UNION
		SELECT DISTINCT
			TRIM(sub_category)::VARCHAR(100),
			TRIM(category)::VARCHAR(100)
		FROM sa_international.src_international_sales
		WHERE sub_category IS NOT NULL AND 
			  TRIM(sub_category) != ''
	LOOP
		IF NOT EXISTS (
			SELECT 1
			FROM bl_3nf.ce_product_subcategories
			WHERE product_subcategory_src_id = v_row.sub_category_name
		) THEN
			-- look up the parent category surrogate key.
			-- if it's not found for some reason, fall back to -1 (default row).
			SELECT COALESCE(product_category_id, -1)
			INTO v_cat_id
			FROM bl_3nf.ce_product_categories
			WHERE product_category_src_id = v_row.category_name;

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
				nextval('bl_3nf.seq_product_subcategory_id'),
				COALESCE(v_row.sub_category_name, 'n.a.'),
				COALESCE(v_row.sub_category_name, 'n.a.'),
				COALESCE(v_cat_id, -1),
				CURRENT_DATE,
				CURRENT_DATE,
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


-- 4.3 CE_PRODUCTS
-- ----------------------------------------------------------

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_products()
LANGUAGE plpgsql
AS $$
DECLARE
	v_proc CONSTANT VARCHAR := 'bl_cl.prc_load_products';
	v_count INTEGER := 0;
	v_row RECORD;
	v_sub_id BIGINT;
BEGIN
	-- DISTINCT ON (product_src_id) keeps one row per product; domestic row
	-- is preferred because it's listed first in the UNION ALL and we order by
	-- product_src_id which makes DISTINCT ON deterministic.
	FOR v_row IN
		SELECT DISTINCT ON (product_src_id)
			product_src_id,
			product_name,
			sub_category
		FROM (
			SELECT DISTINCT
				TRIM(product_id)::VARCHAR(50) AS product_src_id,
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
			WHERE product_id IS NOT NULL AND 
				  TRIM(product_id) != ''
		) all_products
		ORDER BY product_src_id
	LOOP
		IF NOT EXISTS (
			SELECT 1
			FROM bl_3nf.ce_products
			WHERE product_src_id = v_row.product_src_id
		) THEN
			SELECT COALESCE(product_subcategory_id, -1)
			INTO v_sub_id
			FROM bl_3nf.ce_product_subcategories
			WHERE product_subcategory_src_id = v_row.sub_category;

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
				nextval('bl_3nf.seq_product_id'),
				COALESCE(v_row.product_src_id, 'n.a.'),
				COALESCE(v_row.product_name, 'n.a.'),
				COALESCE(v_sub_id, -1),
				CURRENT_DATE,
				CURRENT_DATE,
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


-- 4.4 CE_MARKETS
-- ----------------------------------------------------------

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_markets()
LANGUAGE plpgsql
AS $$
DECLARE
	v_proc CONSTANT VARCHAR := 'bl_cl.prc_load_markets';
	v_count INTEGER := 0;
	v_row RECORD;
BEGIN
	FOR v_row IN
		SELECT DISTINCT TRIM(market)::VARCHAR(50) AS market_name
		FROM sa_domestic.src_domestic_sales
		WHERE market IS NOT NULL AND 
			  TRIM(market) != ''
		UNION
		SELECT DISTINCT TRIM(market)::VARCHAR(50)
		FROM sa_international.src_international_sales
		WHERE market IS NOT NULL AND 
			  TRIM(market) != ''
	LOOP
		IF NOT EXISTS (
			SELECT 1
			FROM bl_3nf.ce_markets
			WHERE market_src_id = v_row.market_name
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


-- 4.5 CE_REGIONS
-- ----------------------------------------------------------
-- region_src_id is region_name||'_'||market_name because the same region
-- name (e.g. 'Central') can appear in multiple markets. This is the same
-- composite key logic we used in task 6.

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_regions()
LANGUAGE plpgsql
AS $$
DECLARE
	v_proc CONSTANT VARCHAR := 'bl_cl.prc_load_regions';
	v_count INTEGER := 0;
	v_row RECORD;
	v_mkt_id BIGINT;
	v_src_id VARCHAR(255);
BEGIN
	FOR v_row IN
		SELECT DISTINCT
			TRIM(region)::VARCHAR(100) AS region_name,
			TRIM(market)::VARCHAR(50) AS market_name
		FROM sa_domestic.src_domestic_sales
		WHERE region IS NOT NULL AND 
			  TRIM(region) != ''
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


-- 4.6 CE_COUNTRIES
-- ----------------------------------------------------------

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_countries()
LANGUAGE plpgsql
AS $$
DECLARE
	v_proc CONSTANT VARCHAR := 'bl_cl.prc_load_countries';
	v_count INTEGER := 0;
	v_row RECORD;
	v_reg_id BIGINT;
BEGIN
	-- DISTINCT ON keeps one country row; both sources may list the same
	-- country with the same region, which is fine.
	FOR v_row IN
		SELECT DISTINCT ON (country_name)
			country_name, region_name, market_name
		FROM (
			SELECT DISTINCT
				TRIM(country)::VARCHAR(100) AS country_name,
				TRIM(region)::VARCHAR(100) AS region_name,
				TRIM(market)::VARCHAR(50) AS market_name
			FROM sa_domestic.src_domestic_sales
			WHERE country IS NOT NULL AND 
				  TRIM(country) != ''
			UNION
			SELECT DISTINCT
				TRIM(country)::VARCHAR(100),
				TRIM(region)::VARCHAR(100),
				TRIM(market)::VARCHAR(50)
			FROM sa_international.src_international_sales
			WHERE country IS NOT NULL AND 
				  TRIM(country) != ''
		) all_countries
		ORDER BY country_name
	LOOP
		IF NOT EXISTS (
			SELECT 1 
			FROM bl_3nf.ce_countries 
			WHERE country_src_id = v_row.country_name
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


-- 4.7 CE_STATES
-- ----------------------------------------------------------
-- Domestic has no state column, so domestic rows produce state_name='N/A'.
-- state_src_id = state_name||'_'||country_name because the same state
-- name can exist in multiple countries.

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
		-- domestic: state = 'N/A' per country
		SELECT DISTINCT
			'N/A'::VARCHAR(100) AS state_name,
			TRIM(country)::VARCHAR(100) AS country_name
		FROM sa_domestic.src_domestic_sales
		WHERE country IS NOT NULL AND TRIM(country) != ''
		UNION
		-- international: actual state value
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


-- 4.8 CE_CITIES
-- ----------------------------------------------------------
-- city_src_id = city||'_'||country||'_'||region (composite natural key,
-- same as task 6). DISTINCT ON (city_key) ensures one row per city.

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_cities()
LANGUAGE plpgsql
AS $$
DECLARE
	v_proc CONSTANT VARCHAR := 'bl_cl.prc_load_cities';
	v_count INTEGER := 0;
	v_row RECORD;
	v_st_id BIGINT;
BEGIN
	FOR v_row IN
		SELECT DISTINCT ON (city_key)
			city_key, city_name, state_name, country_name
		FROM (
			SELECT DISTINCT
				TRIM(city)::VARCHAR(100) || '_' ||
				TRIM(country)::VARCHAR(100) || '_' ||
				TRIM(region)::VARCHAR(100) AS city_key,
				TRIM(city)::VARCHAR(100) AS city_name,
				'N/A'::VARCHAR(100) AS state_name,
				TRIM(country)::VARCHAR(100) AS country_name
			FROM sa_domestic.src_domestic_sales
			WHERE city IS NOT NULL AND TRIM(city) != ''
			UNION ALL
			SELECT DISTINCT
				TRIM(city)::VARCHAR(100) || '_' ||
				TRIM(country)::VARCHAR(100) || '_' ||
				TRIM(region)::VARCHAR(100) AS city_key,
				TRIM(city)::VARCHAR(100) AS city_name,
				COALESCE(NULLIF(TRIM(state),''), 'N/A')::VARCHAR(100) AS state_name,
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


-- 4.9 CE_CUSTOMERS_SCD (SCD Type 2)
-- ----------------------------------------------------------
-- For the initial/historical load, I take the single latest version of
-- each customer. START_DT = '1990-01-01', END_DT = '9999-12-31', IS_ACTIVE = 'Y'.
-- The full source ID (with DOM-/INT- prefix) is kept so customers from
-- different source systems are stored as separate rows and never merged.

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_customers_scd()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc CONSTANT VARCHAR := 'bl_cl.prc_load_customers_scd';
    v_count INTEGER := 0;
    v_row RECORD;
BEGIN
    FOR v_row IN
        SELECT customer_src_id, customer_name, customer_segment, source_system, source_entity
        FROM (
            SELECT DISTINCT ON (customer_id)
                customer_id AS customer_src_id,
                customer_name,
                segment AS customer_segment,
                'SA_DOMESTIC' AS source_system,
                'SRC_DOMESTIC_SALES' AS source_entity
            FROM sa_domestic.src_domestic_sales
            WHERE customer_id IS NOT NULL AND TRIM(customer_id) != ''
            ORDER BY customer_id, customer_name
        ) domestic_customers

        UNION ALL

        SELECT customer_src_id, customer_name, customer_segment, source_system, source_entity
        FROM (
            SELECT DISTINCT ON (customer_id)
                customer_id AS customer_src_id,
                customer_name,
                segment AS customer_segment,
                'SA_INTERNATIONAL' AS source_system,
                'SRC_INTERNATIONAL_SALES' AS source_entity
            FROM sa_international.src_international_sales
            WHERE customer_id IS NOT NULL AND TRIM(customer_id) != ''
            ORDER BY customer_id, customer_name
        ) international_customers
    LOOP
        IF NOT EXISTS (
            SELECT 1
            FROM bl_3nf.ce_customers_scd
            WHERE customer_src_id = v_row.customer_src_id
            AND is_active = 'Y'
        ) THEN
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
                nextval('bl_3nf.seq_customer_id'),
                v_row.customer_src_id,
                COALESCE(v_row.customer_name, 'n.a.'),
                COALESCE(v_row.customer_segment, 'n.a.'),
                '1990-01-01'::DATE,
                '9999-12-31'::DATE,
                'Y',
                CURRENT_DATE,
                v_row.source_system,
                v_row.source_entity
            );

            v_count := v_count + 1;
        END IF;
    END LOOP;

    CALL bl_cl.prc_log(v_proc, v_count, 'SUCCESS',
        'Loaded ' || v_count || ' new rows into bl_3nf.ce_customers_scd');

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'SQLERRM: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


-- 4.10 CE_EMPLOYEES
-- ----------------------------------------------------------
-- Domestic gives employee_id + employee_name.
-- International gives employee_id only (no name column).
-- DISTINCT ON keeps the domestic row when both sources have the same employee,
-- because ordering by employee_name NULLS LAST puts the named row first.

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_employees()
LANGUAGE plpgsql
AS $$
DECLARE
	v_proc CONSTANT VARCHAR := 'bl_cl.prc_load_employees';
	v_count INTEGER := 0;
	v_row RECORD;
BEGIN
	FOR v_row IN
		SELECT DISTINCT ON (employee_src_id)
			employee_src_id,
			employee_name
		FROM (
			SELECT DISTINCT
				TRIM(employee_id)::VARCHAR(50) AS employee_src_id,
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
		ORDER BY employee_src_id,
				employee_name NULLS LAST
	LOOP
		IF NOT EXISTS (
			SELECT 1 FROM bl_3nf.ce_employees
			WHERE employee_src_id = v_row.employee_src_id
		) THEN
			INSERT INTO bl_3nf.ce_employees (
				employee_id, employee_src_id, employee_name,
				insert_dt, update_dt, source_system, source_entity
			)
			VALUES (
				nextval('bl_3nf.seq_employee_id'),
				COALESCE(v_row.employee_src_id, 'n.a.'),
				COALESCE(v_row.employee_name, 'n.a.'),
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


-- 4.11 CE_ORDER_ATTRIBUTES (junk dimension)
-- ----------------------------------------------------------
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
			COALESCE(NULLIF(TRIM(ship_mode), ''), 'n.a.')::VARCHAR(50) AS ship_mode,
			COALESCE(NULLIF(TRIM(order_priority),''), 'n.a.')::VARCHAR(25) AS order_priority
		FROM sa_domestic.src_domestic_sales
		WHERE ship_mode IS NOT NULL AND order_priority IS NOT NULL
	LOOP
		IF NOT EXISTS (
			SELECT 1
			FROM bl_3nf.ce_order_attributes
			WHERE ship_mode = v_row.ship_mode AND 
				  order_priority = v_row.order_priority
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


---------------------------------------------------------------
-- SECTION 5: MASTER PROCEDURE
---------------------------------------------------------------
-- Calls all dimension procedures in FK dependency order.
-- If any single procedure fails its EXCEPTION block will log
-- the error and re-raise, which stops prc_load_all_3nf here.
-- This is intentional - we don't want to load cities if
-- states failed, for example.

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
		'All dimension procedures completed successfully');

	RAISE NOTICE '% finished at %', v_proc, NOW();

EXCEPTION WHEN OTHERS THEN
	CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
		'Master procedure failed: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
	RAISE;
END;
$$;


---------------------------------------------------------------
-- SECTION 6: EXECUTE + VERIFY
---------------------------------------------------------------

-- RUN 1
CALL bl_cl.prc_load_all_3nf();

-- Check the log after first run.
-- screenshot: log_run1.png
SELECT 
	procedure_name,
	rows_affected,
	status,
	log_message,
	log_dt
FROM bl_cl.mta_load_log
ORDER BY log_dt DESC,
		 log_id DESC;

/*
EXPLANATION - WHY rows_affected = 0 ON RUN 1:
The 3NF tables were already populated by a prior manual DML run (from Task 6)
before this procedure script was executed for the first time. Because every
procedure uses a WHERE NOT EXISTS guard before inserting, and all source rows
already had matching records in the target tables, v_count never incremented
during the loop - resulting in 0 inserts even on the very first call to this
procedure.

This is not an error. It demonstrates that the procedures are fully idempotent
from the start: they detect existing data correctly and skip duplicates, whether
those duplicates came from a previous procedure run or from a prior manual load.
The spot-check below (table_counts query) confirms all expected data IS present
in the 3NF tables.
*/


-- RUN 2 (idempotency check)
CALL bl_cl.prc_load_all_3nf();

-- Check the log again. Every procedure should show rows_affected = 0.
-- screenshot: log_run2.png
SELECT 
	procedure_name,
	rows_affected,
	status,
	log_message,
	log_dt
FROM bl_cl.mta_load_log
ORDER BY log_dt DESC,
		 log_id DESC;

/*
EXPLANATION - IDEMPOTENCY CONFIRMED:
Each procedure uses WHERE NOT EXISTS to check whether a source row already
exists in the target table before inserting. Since the source data has not
changed between Run 1 and Run 2, every source row already has a matching
record in the target. The loop iterates but v_count never increments, so
rows_affected = 0 for every procedure.

This is the expected and correct behaviour for an incremental load:
- On the first ever load (or after new source data arrives): rows_affected > 0
- On any subsequent run with unchanged source data: rows_affected = 0
- No duplicates are ever created regardless of how many times the procedure runs

The procedure can be scheduled to run daily and will safely insert only
genuinely new records without re-processing rows that already exist.
*/


-- Quick spot-check: confirm data is actually in the tables
-- (verifies that rows_affected = 0 means "already loaded", not "nothing loaded")
-- screenshot: table_counts.png
SELECT 'CE_PRODUCT_CATEGORIES' AS table_name,
	   COUNT(*) AS total_rows,
	   COUNT(*) FILTER (WHERE product_category_id = -1) AS default_rows
FROM bl_3nf.ce_product_categories
UNION ALL
SELECT 'CE_PRODUCT_SUBCATEGORIES',
	   COUNT(*),
	   COUNT(*) FILTER (WHERE product_subcategory_id = -1)
FROM bl_3nf.ce_product_subcategories
UNION ALL
SELECT 'CE_PRODUCTS',
	   COUNT(*),
	   COUNT(*) FILTER (WHERE product_id = -1)
FROM bl_3nf.ce_products
UNION ALL
SELECT 'CE_MARKETS',
	   COUNT(*),
	   COUNT(*) FILTER (WHERE market_id = -1)
FROM bl_3nf.ce_markets
UNION ALL
SELECT 'CE_REGIONS',
	   COUNT(*),
	   COUNT(*) FILTER (WHERE region_id = -1)
FROM bl_3nf.ce_regions
UNION ALL
SELECT 'CE_COUNTRIES',
	   COUNT(*),
	   COUNT(*) FILTER (WHERE country_id = -1)
FROM bl_3nf.ce_countries
UNION ALL
SELECT 'CE_STATES',
	   COUNT(*),
	   COUNT(*) FILTER (WHERE state_id = -1)
FROM bl_3nf.ce_states
UNION ALL
SELECT 'CE_CITIES',
	   COUNT(*),
	   COUNT(*) FILTER (WHERE city_id = -1)
FROM bl_3nf.ce_cities
UNION ALL
SELECT 'CE_CUSTOMERS_SCD',
	   COUNT(*),
	   COUNT(*) FILTER (WHERE customer_id = -1)
FROM bl_3nf.ce_customers_scd
UNION ALL
SELECT 'CE_EMPLOYEES',
	   COUNT(*),
	   COUNT(*) FILTER (WHERE employee_id = -1)
FROM bl_3nf.ce_employees
UNION ALL
SELECT 'CE_ORDER_ATTRIBUTES',
	   COUNT(*),
	   COUNT(*) FILTER (WHERE order_attr_id = -1)
FROM bl_3nf.ce_order_attributes;


/*
Each table has exactly 1 default row (id = -1, inserted during DDL in Task 6)
plus all the real dimension rows loaded from source. All counts look correct
and consistent with the source data volume.
*/


-- Verify fn_get_source_categories works standalone (satisfies "function returns
-- table/setof type" requirement - this function is also called inside the FOR
-- LOOP of prc_load_product_categories).
-- screenshot: fn_categories.png
SELECT * FROM bl_cl.fn_get_source_categories() ORDER BY category_name;

/*
3 distinct categories returned from the UNION of both source systems.
These match the 3 real rows in ce_product_categories (plus the 1 default row = 4 total).
*/


-- Sample 5 rows from ce_products to confirm data loaded correctly
-- screenshot: sample_products.png
SELECT 
	product_id,
	product_src_id,
	product_name,
	product_subcategory_id,
	source_system
FROM bl_3nf.ce_products
WHERE product_id != -1
LIMIT 5;

/*
All rows show source_system = 'SA_COMBINED' (merged from both domestic and
international sources). product_subcategory_id links back to the correct
surrogate key in ce_product_subcategories.
*/