-- TESTS: Data Quality Checks
-- Global Retail Superstore Sales
-- Tamar Tutisani
--
-- Test Group 1: No duplicates in target tables
-- Test Group 2: All business keys from SA layer are in BL_3NF and BL_DM
--
-- Expected result for every test: 0 rows returned.
-- If any test returns rows, those rows are the problem records.
-----------------------------------------------------------------------------


-- ============================================================
-- TEST GROUP 1: NO DUPLICATES IN TARGET TABLES
-- ============================================================

-- TEST 1.1: No duplicate fact rows in CE_SALES (3NF fact)
-- Business key: order_id + product_id + customer_id + event_dt + source_system
-- If this returns rows, the same order line was loaded more than once.
SELECT
    order_id,
    product_id,
    customer_id,
    event_dt,
    source_system,
    COUNT(*) AS occurrences
FROM bl_3nf.ce_sales
GROUP BY order_id, product_id, customer_id, event_dt, source_system
HAVING COUNT(*) > 1
LIMIT 10;
-- EXPECTED: 0 rows


-- TEST 1.2: No duplicate fact rows in FCT_SALES_DD (DM fact)
-- Business key: order_id + product_surr_id + customer_surr_id + event_dt + source_system
SELECT
    order_id,
    product_surr_id,
    customer_surr_id,
    event_dt,
    source_system,
    COUNT(*) AS occurrences
FROM bl_dm.fct_sales_dd
GROUP BY order_id, product_surr_id, customer_surr_id, event_dt, source_system
HAVING COUNT(*) > 1
LIMIT 10;
-- EXPECTED: 0 rows


-- TEST 1.3: No duplicate active customers in CE_CUSTOMERS_SCD (3NF)
-- Each customer_src_id should have exactly one IS_ACTIVE = 'Y' row.
SELECT
    customer_src_id,
    COUNT(*) AS active_versions
FROM bl_3nf.ce_customers_scd
WHERE is_active = 'Y'
  AND customer_id != -1
GROUP BY customer_src_id
HAVING COUNT(*) > 1
LIMIT 10;
-- EXPECTED: 0 rows


-- TEST 1.4: No duplicate active customers in DIM_CUSTOMERS_SCD (DM)
SELECT
    customer_src_id,
    COUNT(*) AS active_versions
FROM bl_dm.dim_customers_scd
WHERE is_active = 'Y'
  AND customer_surr_id != -1
GROUP BY customer_src_id
HAVING COUNT(*) > 1
LIMIT 10;
-- EXPECTED: 0 rows


-- TEST 1.5: No duplicate products in CE_PRODUCTS (3NF)
SELECT
    product_src_id,
    COUNT(*) AS occurrences
FROM bl_3nf.ce_products
WHERE product_id != -1
GROUP BY product_src_id
HAVING COUNT(*) > 1
LIMIT 10;
-- EXPECTED: 0 rows


-- TEST 1.6: No duplicate products in DIM_PRODUCTS (DM)
SELECT
    product_src_id,
    COUNT(*) AS occurrences
FROM bl_dm.dim_products
WHERE product_surr_id != -1
GROUP BY product_src_id
HAVING COUNT(*) > 1
LIMIT 10;
-- EXPECTED: 0 rows


-- TEST 1.7: No duplicate geographies in DIM_GEOGRAPHY (DM)
SELECT
    geography_src_id,
    COUNT(*) AS occurrences
FROM bl_dm.dim_geography
WHERE geography_surr_id != -1
GROUP BY geography_src_id
HAVING COUNT(*) > 1
LIMIT 10;
-- EXPECTED: 0 rows


-- TEST 1.8: No duplicate employees in DIM_EMPLOYEES (DM)
SELECT
    employee_src_id,
    COUNT(*) AS occurrences
FROM bl_dm.dim_employees
WHERE employee_surr_id != -1
GROUP BY employee_src_id
HAVING COUNT(*) > 1
LIMIT 10;
-- EXPECTED: 0 rows


-- ============================================================
-- TEST GROUP 2: ALL BUSINESS KEYS FROM SA ARE IN BL_3NF AND BL_DM
-- ============================================================

-- TEST 2.1: Every distinct order_id from SA_DOMESTIC appears in CE_SALES
SELECT COUNT(*) AS domestic_orders_missing_from_3nf
FROM (
    SELECT DISTINCT order_id
    FROM sa_domestic.src_domestic_sales
    WHERE order_id IS NOT NULL
) sa
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_sales ce
    WHERE ce.order_id     = sa.order_id
      AND ce.source_system = 'SA_DOMESTIC'
);
-- EXPECTED: 0


-- TEST 2.2: Every distinct order_id from SA_INTERNATIONAL appears in CE_SALES
SELECT COUNT(*) AS international_orders_missing_from_3nf
FROM (
    SELECT DISTINCT order_id
    FROM sa_international.src_international_sales
    WHERE order_id IS NOT NULL
) sa
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_sales ce
    WHERE ce.order_id     = sa.order_id
      AND ce.source_system = 'SA_INTERNATIONAL'
);
-- EXPECTED: 0


-- TEST 2.3: Every distinct product_id from SA layer appears in CE_PRODUCTS
SELECT COUNT(*) AS products_missing_from_3nf
FROM (
    SELECT DISTINCT product_id FROM sa_domestic.src_domestic_sales      WHERE product_id IS NOT NULL
    UNION
    SELECT DISTINCT product_id FROM sa_international.src_international_sales WHERE product_id IS NOT NULL
) sa
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_products p
    WHERE p.product_src_id = sa.product_id
);
-- EXPECTED: 0


-- TEST 2.4: Every distinct customer_id from SA layer appears in CE_CUSTOMERS_SCD
SELECT COUNT(*) AS customers_missing_from_3nf
FROM (
    SELECT DISTINCT customer_id FROM sa_domestic.src_domestic_sales      WHERE customer_id IS NOT NULL
    UNION
    SELECT DISTINCT customer_id FROM sa_international.src_international_sales WHERE customer_id IS NOT NULL
) sa
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_customers_scd c
    WHERE c.customer_src_id = sa.customer_id
);
-- EXPECTED: 0


-- TEST 2.5: Every distinct product_id from CE_PRODUCTS appears in DIM_PRODUCTS
SELECT COUNT(*) AS products_missing_from_dm
FROM bl_3nf.ce_products src
WHERE src.product_id != -1
  AND NOT EXISTS (
      SELECT 1 FROM bl_dm.dim_products dm
      WHERE dm.product_src_id = src.product_src_id
  );
-- EXPECTED: 0


-- TEST 2.6: Every distinct employee_id from SA layer appears in CE_EMPLOYEES
SELECT COUNT(*) AS employees_missing_from_3nf
FROM (
    SELECT DISTINCT employee_id FROM sa_domestic.src_domestic_sales      WHERE employee_id IS NOT NULL
    UNION
    SELECT DISTINCT employee_id FROM sa_international.src_international_sales WHERE employee_id IS NOT NULL
) sa
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_employees e
    WHERE e.employee_src_id = sa.employee_id
);
-- EXPECTED: 0


-- TEST 2.7: Every CE_SALES row in the rolling window appears in FCT_SALES_DD
-- (only the 2 most recent months are in FCT_SALES_DD by design)
WITH rolling_window AS (
    SELECT MIN(month_start) AS from_dt,
           MAX(month_start) + INTERVAL '1 month' AS to_dt
    FROM (
        SELECT DISTINCT DATE_TRUNC('month', event_dt)::DATE AS month_start
        FROM bl_3nf.ce_sales
        ORDER BY month_start DESC
        LIMIT 2
    ) m
)
SELECT COUNT(*) AS ce_sales_rows_missing_from_dm
FROM bl_3nf.ce_sales ce
JOIN rolling_window rw
    ON ce.event_dt >= rw.from_dt AND ce.event_dt < rw.to_dt
WHERE NOT EXISTS (
    SELECT 1 FROM bl_dm.fct_sales_dd fct
    WHERE fct.order_id      = ce.order_id
      AND fct.source_system = ce.source_system
      AND fct.event_dt      = ce.event_dt
);
-- EXPECTED: 0


-- ============================================================
-- SUMMARY: ROW COUNTS ACROSS ALL LAYERS
-- ============================================================
SELECT 'SA_DOMESTIC.SRC_DOMESTIC_SALES'      AS layer_table, COUNT(*) AS row_count FROM sa_domestic.src_domestic_sales
UNION ALL
SELECT 'SA_INTERNATIONAL.SRC_INTERNATIONAL_SALES', COUNT(*) FROM sa_international.src_international_sales
UNION ALL
SELECT 'BL_3NF.CE_PRODUCTS',                 COUNT(*) FROM bl_3nf.ce_products
UNION ALL
SELECT 'BL_3NF.CE_CUSTOMERS_SCD',            COUNT(*) FROM bl_3nf.ce_customers_scd
UNION ALL
SELECT 'BL_3NF.CE_CITIES',                   COUNT(*) FROM bl_3nf.ce_cities
UNION ALL
SELECT 'BL_3NF.CE_EMPLOYEES',                COUNT(*) FROM bl_3nf.ce_employees
UNION ALL
SELECT 'BL_3NF.CE_SALES',                    COUNT(*) FROM bl_3nf.ce_sales
UNION ALL
SELECT 'BL_DM.DIM_PRODUCTS',                 COUNT(*) FROM bl_dm.dim_products
UNION ALL
SELECT 'BL_DM.DIM_CUSTOMERS_SCD',            COUNT(*) FROM bl_dm.dim_customers_scd
UNION ALL
SELECT 'BL_DM.DIM_GEOGRAPHY',                COUNT(*) FROM bl_dm.dim_geography
UNION ALL
SELECT 'BL_DM.DIM_EMPLOYEES',                COUNT(*) FROM bl_dm.dim_employees
UNION ALL
SELECT 'BL_DM.DIM_ORDER_ATTRIBUTES',         COUNT(*) FROM bl_dm.dim_order_attributes
UNION ALL
SELECT 'BL_DM.FCT_SALES_DD',                 COUNT(*) FROM bl_dm.fct_sales_dd
ORDER BY layer_table;