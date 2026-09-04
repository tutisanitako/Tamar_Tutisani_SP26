-- TASK 6: DML SCRIPT - LOAD DATA INTO 3NF LAYER
-- Global Retail Superstore Sales
-- Tamar Tutisani
-- Schema: BL_3NF
-- Purpose: Load data from SA_DOMESTIC and SA_INTERNATIONAL into BL_3NF
-- All INSERTs use WHERE NOT EXISTS -> fully idempotent (safe to re-run)
--
-- Loading order (respects FK dependencies):
--  1. CE_PRODUCT_CATEGORIES     (no FK deps)
--  2. CE_PRODUCT_SUBCATEGORIES  (FK -> CE_PRODUCT_CATEGORIES)
--  3. CE_PRODUCTS               (FK -> CE_PRODUCT_SUBCATEGORIES)
--  4. CE_MARKETS                (no FK deps)
--  5. CE_REGIONS                (FK -> CE_MARKETS)
--  6. CE_COUNTRIES              (FK -> CE_REGIONS)
--  7. CE_STATES                 (FK -> CE_COUNTRIES)
--  8. CE_CITIES                 (FK -> CE_STATES)
--  9. CE_CUSTOMERS_SCD          (no FK deps)
-- 10. CE_EMPLOYEES              (no FK deps)
-- 11. CE_ORDER_ATTRIBUTES       (no FK deps)
-- 12. CE_SALES                  (FK -> all dimensions above)
-----------------------------------------------------------------------------
 
 
-- STEP 1: Load CE_PRODUCT_CATEGORIES
-----------------------------------------------------------------------------
-- Distinct categories from both sources combined via UNION (auto-deduplicates)
 
INSERT INTO bl_3nf.ce_product_categories (
    product_category_id,
    product_category_src_id,
    product_category_name,
    insert_dt,
    update_dt,
    source_system,
    source_entity
)
SELECT
    nextval('bl_3nf.seq_product_category_id'),
    COALESCE(src.category_name, 'n.a.'),
    COALESCE(src.category_name, 'n.a.'),
    CURRENT_DATE,
    CURRENT_DATE,
    'SA_COMBINED',
    'SRC_DOMESTIC_SALES,SRC_INTERNATIONAL_SALES'
FROM (
    SELECT DISTINCT category AS category_name
    FROM sa_domestic.src_domestic_sales
    WHERE category IS NOT NULL AND 
    	  TRIM(category) != ''
    UNION
    SELECT DISTINCT category
    FROM sa_international.src_international_sales
    WHERE category IS NOT NULL AND 
    	  TRIM(category) != ''
) src
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_product_categories pcat
    WHERE pcat.product_category_src_id = src.category_name
);
 
COMMIT;
 
 
-- STEP 2: Load CE_PRODUCT_SUBCATEGORIES
-----------------------------------------------------------------------------
-- sub_category names are unique across both sources (no same name in different category)
-- UNION deduplicates; LEFT JOIN resolves parent category FK
 
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
SELECT
    nextval('bl_3nf.seq_product_subcategory_id'),
    COALESCE(subcat.sub_category_name, 'n.a.'),
    COALESCE(subcat.sub_category_name, 'n.a.'),
    COALESCE(pcat.product_category_id, -1),
    CURRENT_DATE,
    CURRENT_DATE,
    'SA_COMBINED',
    'SRC_DOMESTIC_SALES,SRC_INTERNATIONAL_SALES'
FROM (
    SELECT DISTINCT sub_category AS sub_category_name,
                    category AS category_name
    FROM sa_domestic.src_domestic_sales
    WHERE sub_category IS NOT NULL AND 
    	  TRIM(sub_category) != ''
    UNION
    SELECT DISTINCT sub_category,
                    category
    FROM sa_international.src_international_sales
    WHERE sub_category IS NOT NULL AND
    	  TRIM(sub_category) != ''
) subcat
LEFT JOIN bl_3nf.ce_product_categories pcat
    ON pcat.product_category_src_id = subcat.category_name
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_product_subcategories pscat
    WHERE pscat.product_subcategory_src_id = subcat.sub_category_name
);
 
COMMIT;
 
 
-- STEP 3: Load CE_PRODUCTS
-----------------------------------------------------------------------------
-- Product IDs are conformed across both sources (same product_id = same product).
-- DISTINCT ON (product_id) ensures one row per product_id; domestic row preferred
-- when both sources carry the same product (domestic listed first in UNION ALL).
 
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
SELECT
    nextval('bl_3nf.seq_product_id'),
    COALESCE(prod.product_src_id, 'n.a.'),
    COALESCE(prod.product_name, 'n.a.'),
    COALESCE(pscat.product_subcategory_id, -1),
    CURRENT_DATE,
    CURRENT_DATE,
    'SA_COMBINED',
    'SRC_DOMESTIC_SALES,SRC_INTERNATIONAL_SALES'
FROM (
    SELECT DISTINCT ON (product_src_id)
           product_src_id,
           product_name,
           sub_category
    FROM (
        SELECT DISTINCT product_id AS product_src_id,
                        product_name,
                        sub_category
        FROM sa_domestic.src_domestic_sales
        WHERE product_id IS NOT NULL AND
        	  TRIM(product_id) != ''
        UNION ALL
        SELECT DISTINCT product_id,
                        product_name,
                        sub_category
        FROM sa_international.src_international_sales
        WHERE product_id IS NOT NULL AND
        	  TRIM(product_id) != ''
    ) all_products
    ORDER BY product_src_id
) prod
LEFT JOIN bl_3nf.ce_product_subcategories pscat
    ON pscat.product_subcategory_src_id = prod.sub_category
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_products p
    WHERE p.product_src_id = prod.product_src_id
);
 
COMMIT;
 
 
-- STEP 4: Load CE_MARKETS
-----------------------------------------------------------------------------
 
INSERT INTO bl_3nf.ce_markets (
    market_id,
    market_src_id,
    market_name,
    insert_dt,
    update_dt,
    source_system,
    source_entity
)
SELECT
    nextval('bl_3nf.seq_market_id'),
    COALESCE(src.market_name, 'n.a.'),
    COALESCE(src.market_name, 'n.a.'),
    CURRENT_DATE,
    CURRENT_DATE,
    'SA_COMBINED',
    'SRC_DOMESTIC_SALES,SRC_INTERNATIONAL_SALES'
FROM (
    SELECT DISTINCT market AS market_name
    FROM sa_domestic.src_domestic_sales
    WHERE market IS NOT NULL AND
    	  TRIM(market) != ''
    UNION
    SELECT DISTINCT market
    FROM sa_international.src_international_sales
    WHERE market IS NOT NULL AND
    	  TRIM(market) != ''
) src
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_markets m
    WHERE m.market_src_id = src.market_name
);
 
COMMIT;
 
 
-- STEP 5: Load CE_REGIONS
-----------------------------------------------------------------------------
-- region_src_id = region_name || '_' || market_name
-- Required because the same region name (e.g. 'Central') exists in multiple markets.
-- The LEFT JOIN to CE_REGIONS in downstream steps uses this composite key.
 
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
SELECT
    nextval('bl_3nf.seq_region_id'),
    COALESCE(src.region_name || '_' || src.market_name, 'n.a.'),
    COALESCE(src.region_name, 'n.a.'),
    COALESCE(m.market_id, -1),
    CURRENT_DATE,
    CURRENT_DATE,
    'SA_COMBINED',
    'SRC_DOMESTIC_SALES,SRC_INTERNATIONAL_SALES'
FROM (
    SELECT DISTINCT region AS region_name,
                    market AS market_name
    FROM sa_domestic.src_domestic_sales
    WHERE region IS NOT NULL AND
    	  TRIM(region) != ''
    UNION
    SELECT DISTINCT region,
                    market
    FROM sa_international.src_international_sales
    WHERE region IS NOT NULL AND
    	  TRIM(region) != ''
) src
LEFT JOIN bl_3nf.ce_markets m
    ON m.market_src_id = src.market_name
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_regions r
    WHERE r.region_src_id = (src.region_name || '_' || src.market_name)
);
 
COMMIT;
 
 
-- STEP 6: Load CE_COUNTRIES
-----------------------------------------------------------------------------
-- A country belongs to one region. DISTINCT ON (country_name) keeps one row
-- per country in case the same country appears with slight region variation.
 
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
SELECT
    nextval('bl_3nf.seq_country_id'),
    COALESCE(src.country_name, 'n.a.'),
    COALESCE(src.country_name, 'n.a.'),
    COALESCE(r.region_id, -1),
    CURRENT_DATE,
    CURRENT_DATE,
    'SA_COMBINED',
    'SRC_DOMESTIC_SALES,SRC_INTERNATIONAL_SALES'
FROM (
    SELECT DISTINCT ON (country_name)
           country_name,
           region_name,
           market_name
    FROM (
        SELECT DISTINCT country AS country_name,
                        region AS region_name,
                        market AS market_name
        FROM sa_domestic.src_domestic_sales
        WHERE country IS NOT NULL AND 
        	  TRIM(country) != ''
        UNION
        SELECT DISTINCT country,
                        region,
                        market
        FROM sa_international.src_international_sales
        WHERE country IS NOT NULL AND
        	  TRIM(country) != ''
    ) all_countries
    ORDER BY country_name
) src
LEFT JOIN bl_3nf.ce_regions r
    ON r.region_src_id = (src.region_name || '_' || src.market_name)
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_countries cc
    WHERE cc.country_src_id = src.country_name
);
 
COMMIT;
 
 
-- STEP 7: Load CE_STATES
-----------------------------------------------------------------------------
-- state_src_id = state_name || '_' || country_name
-- Required because the same state name can exist in multiple countries.
-- Domestic rows use 'N/A' as state_name.
 
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
SELECT
    nextval('bl_3nf.seq_state_id'),
    COALESCE(src.state_name || '_' || src.country_name, 'n.a.'),
    COALESCE(src.state_name, 'n.a.'),
    COALESCE(c.country_id, -1),
    CURRENT_DATE,
    CURRENT_DATE,
    'SA_COMBINED',
    'SRC_DOMESTIC_SALES,SRC_INTERNATIONAL_SALES'
FROM (
    -- Domestic: no state column -> use 'N/A' per country
    SELECT DISTINCT 'N/A' AS state_name,
                    country AS country_name
    FROM sa_domestic.src_domestic_sales
    WHERE country IS NOT NULL AND
    	  TRIM(country) != ''
    UNION
    -- International: actual state value
    SELECT DISTINCT state AS state_name,
                    country AS country_name
    FROM sa_international.src_international_sales
    WHERE state IS NOT NULL AND
    	  TRIM(state) != ''
) src
LEFT JOIN bl_3nf.ce_countries c
    ON c.country_src_id = src.country_name
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_states ss
    WHERE ss.state_src_id = (src.state_name || '_' || src.country_name)
);
 
COMMIT;
 
 
-- STEP 8: Load CE_CITIES
-----------------------------------------------------------------------------
-- city_src_id = city || '_' || country || '_' || region (composite natural key)
-- DISTINCT ON (city_key) prevents duplicates when same city appears in both sources.
-- LEFT JOIN to CE_STATES uses the composite state_src_id = state_name||'_'||country_name.
 
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
SELECT
    nextval('bl_3nf.seq_city_id'),
    COALESCE(src.city_key, 'n.a.'),
    COALESCE(src.city_name, 'n.a.'),
    COALESCE(st.state_id, -1),
    CURRENT_DATE,
    CURRENT_DATE,
    'SA_COMBINED',
    'SRC_DOMESTIC_SALES,SRC_INTERNATIONAL_SALES'
FROM (
    SELECT DISTINCT ON (city_key)
           city_key,
           city_name,
           state_name,
           country_name
    FROM (
        -- Domestic: state = 'N/A'
        SELECT DISTINCT
            city || '_' || country || '_' || region AS city_key,
            city AS city_name,
            'N/A' AS state_name,
            country AS country_name
        FROM sa_domestic.src_domestic_sales
        WHERE city IS NOT NULL AND
        	  TRIM(city) != ''
        UNION ALL
        -- International: use actual state
        SELECT DISTINCT
            city || '_' || country || '_' || region AS city_key,
            city AS city_name,
            COALESCE(NULLIF(TRIM(state), ''), 'N/A') AS state_name,
            country AS country_name
        FROM sa_international.src_international_sales
        WHERE city IS NOT NULL AND
        	  TRIM(city) != ''
    ) all_cities
    ORDER BY city_key
) src
LEFT JOIN bl_3nf.ce_states st
    ON st.state_src_id = (src.state_name || '_' || src.country_name)
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_cities cc
    WHERE cc.city_src_id = src.city_key
);
 
COMMIT;
 
 
-- STEP 9: Load CE_CUSTOMERS_SCD (SCD Type 2)
-----------------------------------------------------------------------------
-- for the initial/first load, picked the single latest version of each customer. 
-- No historical reconstruction from order dates. 
-- START_DT = '1990-01-01', END_DT = '9999-12-31', IS_ACTIVE = 'Y'.
--
-- customer_src_id stores the FULL source ID including prefix (DOM- / INT-)
-- so customers from domestic and international are separate entities as required
-- They are not merged. Each source system's customers are loaded as distinct rows.
--
-- DISTINCT ON (customer_src_id) deduplicates within each source (same customer
-- can appear on many order rows). GROUP BY + MAX resolves name/segment if
-- the same customer_id has minor variation across rows.
 
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
SELECT
    nextval('bl_3nf.seq_customer_id'),
    src.customer_src_id,
    src.customer_name,
    src.customer_segment,
    '1990-01-01'::DATE,
    '9999-12-31'::DATE,
    'Y',
    CURRENT_DATE,
    src.source_system,
    src.source_entity
FROM (
    (
    -- Domestic customers: keep full ID including DOM- prefix
    SELECT DISTINCT ON (customer_id)
           customer_id AS customer_src_id,
           customer_name,
           segment AS customer_segment,
           'SA_DOMESTIC' AS source_system,
           'SRC_DOMESTIC_SALES' AS source_entity
    FROM sa_domestic.src_domestic_sales
    WHERE customer_id IS NOT NULL AND 
    	  TRIM(customer_id) != ''
    ORDER BY customer_id, customer_name
    )
 
    UNION ALL
 
    (
    -- International customers: keep full ID including INT- prefix
    SELECT DISTINCT ON (customer_id)
           customer_id AS customer_src_id,
           customer_name,
           segment AS customer_segment,
           'SA_INTERNATIONAL' AS source_system,
           'SRC_INTERNATIONAL_SALES' AS source_entity
    FROM sa_international.src_international_sales
    WHERE customer_id IS NOT NULL AND
    	  TRIM(customer_id) != ''
    ORDER BY customer_id, customer_name
    )
) src
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_customers_scd cc
    WHERE cc.customer_src_id = src.customer_src_id AND 
    	  cc.is_active = 'Y'
);
 
COMMIT;
 
 
-- STEP 10: Load CE_EMPLOYEES
-----------------------------------------------------------------------------
-- Domestic provides employee_id + employee_name.
-- International provides employee_id only (no name column).
-- DISTINCT ON (employee_id) ordered by employee_name NULLS LAST keeps the
-- domestic row (with name) when the same employee appears in both sources.
 
INSERT INTO bl_3nf.ce_employees (
    employee_id,
    employee_src_id,
    employee_name,
    insert_dt,
    update_dt,
    source_system,
    source_entity
)
SELECT
    nextval('bl_3nf.seq_employee_id'),
    COALESCE(src.employee_src_id, 'n.a.'),
    COALESCE(src.employee_name,   'n.a.'),
    CURRENT_DATE,
    CURRENT_DATE,
    'SA_COMBINED',
    'SRC_DOMESTIC_SALES,SRC_INTERNATIONAL_SALES'
FROM (
    SELECT DISTINCT ON (employee_src_id)
           employee_src_id,
           employee_name
    FROM (
        -- Domestic: full name available
        SELECT DISTINCT employee_id AS employee_src_id,
                        employee_name
        FROM sa_domestic.src_domestic_sales
        WHERE employee_id IS NOT NULL AND
        	  TRIM(employee_id) != ''
 
        UNION ALL
 
        -- International: name not available -> NULL
        SELECT DISTINCT employee_id AS employee_src_id,
                        NULL::VARCHAR(255) AS employee_name
        FROM sa_international.src_international_sales
        WHERE employee_id IS NOT NULL AND
        	  TRIM(employee_id) != ''
    ) all_employees
    ORDER BY employee_src_id,
             employee_name NULLS LAST
) src
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_employees e
    WHERE e.employee_src_id = src.employee_src_id
);
 
COMMIT;
 
 
-- STEP 11: Load CE_ORDER_ATTRIBUTES (junk dimension)
-----------------------------------------------------------------------------
-- Only from Domestic source (International has no ship_mode / order_priority).
-- International fact rows will reference the default -1 row.
 
INSERT INTO bl_3nf.ce_order_attributes (
    order_attr_id,
    ship_mode,
    order_priority,
    insert_dt,
    update_dt,
    source_system,
    source_entity
)
SELECT
    nextval('bl_3nf.seq_order_attr_id'),
    COALESCE(src.ship_mode, 'n.a.'),
    COALESCE(src.order_priority, 'n.a.'),
    CURRENT_DATE,
    CURRENT_DATE,
    'SA_DOMESTIC',
    'SRC_DOMESTIC_SALES'
FROM (
    SELECT DISTINCT
        COALESCE(NULLIF(TRIM(ship_mode), ''), 'n.a.') AS ship_mode,
        COALESCE(NULLIF(TRIM(order_priority), ''), 'n.a.') AS order_priority
    FROM sa_domestic.src_domestic_sales
    WHERE ship_mode IS NOT NULL AND 
    	  order_priority IS NOT NULL
) src
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_order_attributes oa
    WHERE oa.ship_mode = src.ship_mode AND 
    	  oa.order_priority = src.order_priority
);
 
COMMIT;
 
-- STEP 11.5: Indexes to make Step 12 (fact load) performant
-- Without these, the LEFT JOINs to lookup tables and the correlated
-- NOT EXISTS subquery against the growing ce_sales table force sequential
-- scans / nested loops over ~1M rows each, causing the severe slowdown
-- observed after Step 12 starts.
-----------------------------------------------------------------------------

-- Speeds up LEFT JOIN lookups from staging to 3NF dimension tables
CREATE INDEX IF NOT EXISTS idx_src_dom_product_id   ON sa_domestic.src_domestic_sales(product_id);
CREATE INDEX IF NOT EXISTS idx_src_dom_customer_id  ON sa_domestic.src_domestic_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_src_dom_employee_id  ON sa_domestic.src_domestic_sales(employee_id);

CREATE INDEX IF NOT EXISTS idx_src_intl_product_id  ON sa_international.src_international_sales(product_id);
CREATE INDEX IF NOT EXISTS idx_src_intl_customer_id ON sa_international.src_international_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_src_intl_employee_id ON sa_international.src_international_sales(employee_id);

-- Speeds up the WHERE NOT EXISTS idempotency check in Step 12, which is run
-- once per source row against ce_sales on (order_id, product_id, customer_id,
-- event_dt, source_system).
CREATE INDEX IF NOT EXISTS idx_ce_sales_dedupe_key
    ON bl_3nf.ce_sales (order_id, product_id, customer_id, event_dt, source_system);

ANALYZE sa_domestic.src_domestic_sales;
ANALYZE sa_international.src_international_sales;
ANALYZE bl_3nf.ce_dates;
ANALYZE bl_3nf.ce_products;
ANALYZE bl_3nf.ce_customers_scd;
ANALYZE bl_3nf.ce_cities;
ANALYZE bl_3nf.ce_employees;
ANALYZE bl_3nf.ce_order_attributes;

-- STEP 12: Load CE_SALES (fact table - transaction grain)
-----------------------------------------------------------------------------
-- One row per product line item on a customer order.
-- Loaded from both SA_DOMESTIC and SA_INTERNATIONAL.
-- Domestic table has no 'state' column -> use literal 'N/A'.
-- LEFT JOINs resolve all surrogate keys; COALESCE to -1 if lookup fails.
-- city_src_id join uses composite key: city||'_'||country||'_'||region.
-- state_src_id join uses composite key: state_name||'_'||country (via city->state).
-- customer_src_id is the FULL source ID (with DOM-/INT- prefix) matching Step 9.
-- Idempotency: WHERE NOT EXISTS on (order_id, product_id, customer_id,
--              event_dt, source_system).
--
-- date_id previously fell back to a computed TO_CHAR(...)::INTEGER value
-- whenever the LEFT JOIN to ce_dates found no match, but that computed value
-- was never inserted into ce_dates, so it broke fk_sales_to_date. Since
-- CE_DATES now covers the full 2024-2025 range and has a default -1 row,
-- the fallback is COALESCE'd to -1 instead of a fabricated date_id.
 
INSERT INTO bl_3nf.ce_sales (
    event_dt,
    date_id,
    product_id,
    customer_id,
    city_id,
    employee_id,
    order_attr_id,
    order_id,
    sales_amt,
    cost_amt,
    profit_amt,
    shipping_cost_amt,
    quantity_cnt,
    discount_amt,
    insert_dt,
    update_dt,
    source_system,
    source_entity
)
SELECT
    s.order_date AS event_dt,
    COALESCE(d.date_id, -1) AS date_id,
    COALESCE(p.product_id, -1) AS product_id,
    COALESCE(c.customer_id, -1) AS customer_id,
    COALESCE(cty.city_id, -1) AS city_id,
    COALESCE(e.employee_id, -1) AS employee_id,
    COALESCE(oa.order_attr_id, -1) AS order_attr_id,
    COALESCE(s.order_id, 'n.a.') AS order_id,
    s.sales_amt,
    s.cost_amt,
    s.profit_amt,
    s.shipping_cost_amt,
    s.quantity_cnt,
    s.discount_amt,
    CURRENT_DATE AS insert_dt,
    CURRENT_DATE AS update_dt,
    s.source_system,
    s.source_entity
FROM (
    -- Domestic sales rows
    -- Domestic table has no 'state' column -> supply 'N/A' literal
    SELECT
        order_date,
        order_id,
        product_id,
        customer_id,
        city,
        'N/A' AS state,
        country,
        region,
        employee_id,
        ship_mode,
        order_priority,
        sales AS sales_amt,
        cost AS cost_amt,
        profit AS profit_amt,
        shipping_cost AS shipping_cost_amt,
        quantity AS quantity_cnt,
        discount AS discount_amt,
        'SA_DOMESTIC' AS source_system,
        'SRC_DOMESTIC_SALES' AS source_entity
    FROM sa_domestic.src_domestic_sales
    WHERE order_date IS NOT NULL AND 
    	  order_id IS NOT NULL AND
    	  product_id IS NOT NULL
 
    UNION ALL
 
    -- International sales rows
    SELECT
        order_date,
        order_id,
        product_id,
        customer_id,
        city,
        COALESCE(NULLIF(TRIM(state), ''), 'N/A') AS state,
        country,
        region,
        employee_id,
        NULL::VARCHAR(50) AS ship_mode,      -- not tracked in international
        NULL::VARCHAR(25) AS order_priority, -- not tracked in international
        sales AS sales_amt,
        cost AS cost_amt,
        profit AS profit_amt,
        shipping_cost AS shipping_cost_amt,
        quantity AS quantity_cnt,
        discount AS discount_amt,
        'SA_INTERNATIONAL' AS source_system,
        'SRC_INTERNATIONAL_SALES' AS source_entity
    FROM sa_international.src_international_sales
    WHERE order_date IS NOT NULL AND 
    	  order_id IS NOT NULL AND 
    	  product_id IS NOT NULL
) s
-- Resolve date surrogate key
LEFT JOIN bl_3nf.ce_dates d
	ON d.date_dt = s.order_date
-- Resolve product surrogate key
LEFT JOIN bl_3nf.ce_products p
	ON p.product_src_id = s.product_id
-- Resolve customer surrogate key (full source ID with prefix, matches Step 9)
LEFT JOIN bl_3nf.ce_customers_scd c
	ON c.customer_src_id = s.customer_id AND 
	   c.is_active = 'Y'
-- Resolve city surrogate key (composite city_src_id)
LEFT JOIN bl_3nf.ce_cities cty
	ON cty.city_src_id = (s.city || '_' || s.country || '_' || s.region)
-- Resolve employee surrogate key
LEFT JOIN bl_3nf.ce_employees e
	ON e.employee_src_id = s.employee_id
-- Resolve order attributes surrogate key
-- International rows: ship_mode and order_priority are NULL -> COALESCE to 'n.a.'
-- which matches the default row inserted in the default rows script
LEFT JOIN bl_3nf.ce_order_attributes oa
	ON oa.ship_mode = COALESCE(NULLIF(s.ship_mode, ''),      'n.a.') AND 
	   oa.order_priority = COALESCE(NULLIF(s.order_priority, ''), 'n.a.')
-- Idempotency: do not insert if this exact sales row already exists
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_sales existing
    WHERE existing.order_id = s.order_id AND
    	  existing.product_id = COALESCE(p.product_id, -1) AND 
    	  existing.customer_id = COALESCE(c.customer_id, -1) AND 
    	  existing.event_dt = s.order_date AND 
    	  existing.source_system = s.source_system
);

COMMIT;
 
 
-- VERIFICATION QUERIES
-----------------------------------------------------------------------------
 
SELECT
    'CE_PRODUCT_CATEGORIES' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE product_category_id = -1) AS default_rows
FROM bl_3nf.ce_product_categories
UNION ALL
SELECT 
	'CE_PRODUCT_SUBCATEGORIES',
    COUNT(*), 
    COUNT(*) FILTER (WHERE product_subcategory_id = -1)
FROM bl_3nf.ce_product_subcategories
UNION ALL
SELECT 'CE_PRODUCTS',
    COUNT(*), 
    COUNT(*) FILTER (WHERE product_id = -1)
FROM bl_3nf.ce_products
UNION ALL
SELECT 
	'CE_MARKETS',
    COUNT(*), 
    COUNT(*) FILTER (WHERE market_id = -1)
FROM bl_3nf.ce_markets
UNION ALL
SELECT 
	'CE_REGIONS',
    COUNT(*), 
    COUNT(*) FILTER (WHERE region_id = -1)
FROM bl_3nf.ce_regions
UNION ALL
SELECT 
	'CE_COUNTRIES',
    COUNT(*), 
    COUNT(*) FILTER (WHERE country_id = -1)
FROM bl_3nf.ce_countries
UNION ALL
SELECT 
	'CE_STATES',
    COUNT(*), 
    COUNT(*) FILTER (WHERE state_id = -1)
FROM bl_3nf.ce_states
UNION ALL
SELECT 
	'CE_CITIES',
    COUNT(*), 
    COUNT(*) FILTER (WHERE city_id = -1)
FROM bl_3nf.ce_cities
UNION ALL
SELECT 
	'CE_CUSTOMERS_SCD',
    COUNT(*), 
    COUNT(*) FILTER (WHERE customer_id = -1)
FROM bl_3nf.ce_customers_scd
UNION ALL
SELECT 
	'CE_EMPLOYEES',
    COUNT(*), 
    COUNT(*) FILTER (WHERE employee_id = -1)
FROM bl_3nf.ce_employees
UNION ALL
SELECT 
	'CE_ORDER_ATTRIBUTES',
    COUNT(*), 
    COUNT(*) FILTER (WHERE order_attr_id = -1)
FROM bl_3nf.ce_order_attributes
UNION ALL
SELECT 
	'CE_SALES (fact - no default row)', 
	COUNT(*), 
	0
FROM bl_3nf.ce_sales;