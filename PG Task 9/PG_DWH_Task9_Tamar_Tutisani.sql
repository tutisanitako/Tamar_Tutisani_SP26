-- POSTGRESQL TASK 9: Loading Fact Tables on 3NF and DM Layers
-- Tamar Tutisani
-- Module: PG_DWH
-----------------------------------------------------------------------------


---------------------------------------------------------------
-- SECTION 0: PRIVILEGES
---------------------------------------------------------------

GRANT USAGE ON SCHEMA bl_dm TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA bl_dm TO PUBLIC;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA bl_dm TO PUBLIC;


---------------------------------------------------------------
-- SECTION 1: CREATE FCT_SALES_DD ON BL_DM (PARTITIONED BY MONTH)
---------------------------------------------------------------
-- FCT_SALES_DD is partitioned by RANGE on event_dt, one partition per month.
-- This follows the ATTACH/DETACH pattern from the Warehouse Refresh material:
-- new data is loaded into a staging table first, then attached as a partition.
-- Old partitions can be dropped with a single DDL command, no mass DELETE needed.

DROP TABLE IF EXISTS bl_dm.fct_sales_dd CASCADE;

CREATE TABLE bl_dm.fct_sales_dd (
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
    source_entity VARCHAR(100) NOT NULL
)
PARTITION BY RANGE (event_dt);

-- Indexes are created per partition after ATTACH inside the rolling window procedure.
-- A top-level index on a partitioned table creates child indexes automatically going
-- forward but won't help with the ATTACH pattern we're using here.


---------------------------------------------------------------
-- SECTION 2: INCREMENTAL LOAD PROCEDURE FOR CE_SALES (3NF FACT)
---------------------------------------------------------------
-- CE_SALES is the fact table in BL_3NF. It loads incrementally from both
-- SA layers using MAX(event_dt) in CE_SALES as the high-water mark.
-- First run: v_last_dt is NULL, so all rows are loaded (full initial load).
-- Subsequent runs: only rows with event_dt > v_last_dt are processed.
-- Idempotency is enforced by an EXISTS check on order_id + event_dt + source_system
-- before each insert, so re-running produces 0 new rows if data hasn't changed.

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_sales_incremental()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc CONSTANT VARCHAR := 'bl_cl.prc_load_ce_sales_incremental';
    v_count INTEGER := 0;
    v_last_dt DATE;
    v_row RECORD;
    v_date_id INTEGER;
    v_product_id BIGINT;
    v_customer_id BIGINT;
    v_city_id BIGINT;
    v_employee_id BIGINT;
    v_order_attr_id BIGINT;
BEGIN
    -- high-water mark: NULL on first run means load everything
    SELECT MAX(event_dt) INTO v_last_dt
    FROM bl_3nf.ce_sales;

    RAISE NOTICE '% incremental from: %', v_proc, COALESCE(v_last_dt::TEXT, 'full load');

    FOR v_row IN
        SELECT
            s.order_date,
            s.order_id,
            s.product_id,
            s.customer_id,
            s.city,
            'N/A'::VARCHAR(100) AS state,
            s.country,
            s.region,
            s.employee_id,
            s.ship_mode,
            s.order_priority,
            s.sales AS sales_amt,
            s.cost AS cost_amt,
            s.profit AS profit_amt,
            s.shipping_cost AS shipping_cost_amt,
            s.quantity AS quantity_cnt,
            s.discount AS discount_amt,
            'SA_DOMESTIC' AS source_system,
            'SRC_DOMESTIC_SALES' AS source_entity
        FROM sa_domestic.src_domestic_sales s
        WHERE s.order_date IS NOT NULL
        AND s.order_id IS NOT NULL
        AND s.product_id IS NOT NULL
        AND (v_last_dt IS NULL OR s.order_date > v_last_dt)

        UNION ALL

        SELECT
            s.order_date,
            s.order_id,
            s.product_id,
            s.customer_id,
            s.city,
            COALESCE(NULLIF(TRIM(s.state), ''), 'N/A') AS state,
            s.country,
            s.region,
            s.employee_id,
            NULL::VARCHAR(50) AS ship_mode,
            NULL::VARCHAR(25) AS order_priority,
            s.sales,
            s.cost,
            s.profit,
            s.shipping_cost,
            s.quantity,
            s.discount,
            'SA_INTERNATIONAL',
            'SRC_INTERNATIONAL_SALES'
        FROM sa_international.src_international_sales s
        WHERE s.order_date IS NOT NULL
        AND s.order_id IS NOT NULL
        AND s.product_id IS NOT NULL
        AND (v_last_dt IS NULL OR s.order_date > v_last_dt)
    LOOP
        -- skip if this row was already loaded (idempotency guard)
        IF EXISTS (
            SELECT 1 FROM bl_3nf.ce_sales existing
            WHERE existing.order_id = v_row.order_id
            AND existing.event_dt = v_row.order_date
            AND existing.source_system = v_row.source_system
        ) THEN
            CONTINUE;
        END IF;

        -- resolve surrogate keys, fall back to -1 if lookup misses
        SELECT COALESCE(date_id, TO_CHAR(v_row.order_date, 'YYYYMMDD')::INTEGER)
        INTO v_date_id
        FROM bl_3nf.ce_dates WHERE date_dt = v_row.order_date;

        SELECT COALESCE(product_id, -1) INTO v_product_id
        FROM bl_3nf.ce_products WHERE product_src_id = v_row.product_id;

        SELECT COALESCE(customer_id, -1) INTO v_customer_id
        FROM bl_3nf.ce_customers_scd
        WHERE customer_src_id = v_row.customer_id AND is_active = 'Y';

        SELECT COALESCE(city_id, -1) INTO v_city_id
        FROM bl_3nf.ce_cities
        WHERE city_src_id = (v_row.city || '_' || v_row.country || '_' || v_row.region);

        SELECT COALESCE(employee_id, -1) INTO v_employee_id
        FROM bl_3nf.ce_employees WHERE employee_src_id = v_row.employee_id;

        SELECT COALESCE(order_attr_id, -1) INTO v_order_attr_id
        FROM bl_3nf.ce_order_attributes
        WHERE ship_mode = COALESCE(NULLIF(v_row.ship_mode, ''), 'n.a.')
        AND order_priority = COALESCE(NULLIF(v_row.order_priority, ''), 'n.a.');

        INSERT INTO bl_3nf.ce_sales (
            event_dt, date_id, product_id, customer_id, city_id,
            employee_id, order_attr_id, order_id,
            sales_amt, cost_amt, profit_amt, shipping_cost_amt,
            quantity_cnt, discount_amt,
            insert_dt, update_dt, source_system, source_entity
        )
        VALUES (
            v_row.order_date,
            COALESCE(v_date_id, TO_CHAR(v_row.order_date, 'YYYYMMDD')::INTEGER),
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
        || ': ' || v_count || ' new rows inserted into bl_3nf.ce_sales');

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'SQLERRM: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


---------------------------------------------------------------
-- SECTION 3: ROLLING-WINDOW PARTITION LOAD FOR FCT_SALES_DD (DM FACT)
---------------------------------------------------------------
-- The procedure finds the 2 most recent months in CE_SALES and refreshes
-- their partitions in FCT_SALES_DD on every run. Why 2 months: late-arriving
-- orders from month N-1 may only reach CE_SALES during month N, so the last
-- full month is always re-loaded alongside the current in-progress month.
--
-- Steps per month:
--   A. Drop any leftover staging table from a failed previous run
--   B. Create a plain staging table (LIKE fct_sales_dd, no partition key)
--   C. Populate it from CE_SALES joined to all DM dimension surrogate keys
--   D. DETACH + DROP the existing partition for that month if it exists
--   E. Rename staging table to the partition name, then ATTACH it
--   F. Create an index on event_dt on the newly attached partition
--
-- The main table stays available to readers throughout; the brief lock
-- happens only at ATTACH, not during the bulk INSERT into the staging table.

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_fct_sales_dd_rolling()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc CONSTANT VARCHAR := 'bl_cl.prc_load_fct_sales_dd_rolling';
    v_count INTEGER := 0;
    v_month_count INTEGER := 0;
    v_month_rec RECORD;
    v_staging_name TEXT;
    v_partition_name TEXT;
    v_month_start DATE;
    v_month_end DATE;
    v_row_count INTEGER;
BEGIN
    FOR v_month_rec IN
        SELECT DISTINCT DATE_TRUNC('month', event_dt)::DATE AS month_start
        FROM bl_3nf.ce_sales
        ORDER BY month_start DESC
        LIMIT 2
    LOOP
        v_month_start := v_month_rec.month_start;
        v_month_end := v_month_start + INTERVAL '1 month';
        v_staging_name := 'fct_sales_dd_stg_' || TO_CHAR(v_month_start, 'YYYYMM');
        v_partition_name := 'fct_sales_dd_' || TO_CHAR(v_month_start, 'YYYYMM');

        RAISE NOTICE '% processing month: %', v_proc, v_month_start;

        -- STEP A: clean up any leftover staging table from a failed run
        EXECUTE 'DROP TABLE IF EXISTS bl_dm.' || v_staging_name || ' CASCADE';

        -- STEP B: plain heap table, same columns as fct_sales_dd
        EXECUTE 'CREATE TABLE bl_dm.' || v_staging_name || ' (LIKE bl_dm.fct_sales_dd)';

        -- STEP C: fill staging table for this month
        -- join CE_SALES to all DM dimensions via src_id chains to get DM surrogate keys
        -- COALESCE to -1 on any lookup miss so no fact row is lost
        EXECUTE '
            INSERT INTO bl_dm.' || v_staging_name || ' (
                event_dt, date_id, product_surr_id, customer_surr_id,
                geography_surr_id, employee_surr_id, order_attr_surr_id,
                order_id, sales_amt, cost_amt, profit_amt,
                shipping_cost_amt, quantity_cnt, discount_amt,
                profit_margin_amt,
                insert_dt, update_dt, source_system, source_entity
            )
            SELECT
                s.event_dt,
                s.date_id,
                COALESCE(p.product_surr_id, -1),
                COALESCE(c.customer_surr_id, -1),
                COALESCE(g.geography_surr_id, -1),
                COALESCE(e.employee_surr_id, -1),
                COALESCE(oa.order_attr_surr_id, -1),
                s.order_id,
                s.sales_amt,
                s.cost_amt,
                s.profit_amt,
                s.shipping_cost_amt,
                s.quantity_cnt,
                s.discount_amt,
                CASE
                    WHEN s.sales_amt IS NOT NULL AND s.sales_amt != 0
                    THEN ROUND(s.profit_amt / s.sales_amt, 4)
                    ELSE NULL
                END AS profit_margin_amt,
                CURRENT_DATE,
                CURRENT_DATE,
                s.source_system,
                s.source_entity
            FROM bl_3nf.ce_sales s
            LEFT JOIN bl_3nf.ce_products cp ON cp.product_id = s.product_id
            LEFT JOIN bl_dm.dim_products p ON p.product_src_id = cp.product_src_id
            LEFT JOIN bl_3nf.ce_customers_scd cc ON cc.customer_id = s.customer_id AND cc.is_active = ''Y''
            LEFT JOIN bl_dm.dim_customers_scd c ON c.customer_src_id = cc.customer_src_id AND c.is_active = ''Y''
            LEFT JOIN bl_3nf.ce_cities cty ON cty.city_id = s.city_id
            LEFT JOIN bl_dm.dim_geography g ON g.geography_src_id = cty.city_src_id
            LEFT JOIN bl_3nf.ce_employees ce ON ce.employee_id = s.employee_id
            LEFT JOIN bl_dm.dim_employees e ON e.employee_src_id = ce.employee_src_id
            LEFT JOIN bl_3nf.ce_order_attributes coa ON coa.order_attr_id = s.order_attr_id
            LEFT JOIN bl_dm.dim_order_attributes oa
                ON oa.ship_mode = coa.ship_mode AND oa.order_priority = coa.order_priority
            WHERE s.event_dt >= $1
            AND s.event_dt < $2
        ' USING v_month_start, v_month_end;

        GET DIAGNOSTICS v_row_count = ROW_COUNT;
        RAISE NOTICE '  staged % rows for month %', v_row_count, v_month_start;

        -- STEP D: detach and drop existing partition if it exists
        -- ALTER TABLE does not support IF EXISTS for DETACH, so check pg_class first
        IF EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'bl_dm' AND c.relname = v_partition_name
        ) THEN
            EXECUTE 'ALTER TABLE bl_dm.fct_sales_dd DETACH PARTITION bl_dm.' || v_partition_name;
            EXECUTE 'DROP TABLE bl_dm.' || v_partition_name;
            RAISE NOTICE '  dropped old partition: %', v_partition_name;
        END IF;

        -- STEP E: rename staging to final partition name, then attach
        EXECUTE 'ALTER TABLE bl_dm.' || v_staging_name || ' RENAME TO ' || v_partition_name;

        EXECUTE '
            ALTER TABLE bl_dm.fct_sales_dd
            ATTACH PARTITION bl_dm.' || v_partition_name || '
            FOR VALUES FROM (''' || v_month_start || ''') TO (''' || v_month_end || ''')
        ';

        -- STEP F: index on event_dt for this partition
        -- declarative partitions don't inherit parent indexes for the ATTACH pattern
        EXECUTE '
            CREATE INDEX IF NOT EXISTS idx_' || v_partition_name || '_event_dt
            ON bl_dm.' || v_partition_name || ' (event_dt)
        ';

        v_count := v_count + v_row_count;
        v_month_count := v_month_count + 1;
    END LOOP;

    CALL bl_cl.prc_log(v_proc, v_count, 'SUCCESS',
        'Rolling window load: refreshed ' || v_month_count
        || ' month partitions, ' || v_count || ' total rows in bl_dm.fct_sales_dd');

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'SQLERRM: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


---------------------------------------------------------------
-- SECTION 4: MASTER PROCEDURE
---------------------------------------------------------------
-- Runs CE_SALES incremental load first, then the DM rolling window.
-- Order is important: DM fact resolves surrogate keys from CE_ tables,
-- so the 3NF layer must be current before the DM load starts.

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_all_facts()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc CONSTANT VARCHAR := 'bl_cl.prc_load_all_facts';
BEGIN
    RAISE NOTICE '% started at %', v_proc, NOW();

    CALL bl_cl.prc_load_ce_sales_incremental();
    CALL bl_cl.prc_load_fct_sales_dd_rolling();

    CALL bl_cl.prc_log(v_proc, 0, 'SUCCESS', 'All fact load procedures completed successfully');

    RAISE NOTICE '% finished at %', v_proc, NOW();

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'Master fact loader failed: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;


---------------------------------------------------------------
-- SECTION 5: RUN 1
---------------------------------------------------------------

CALL bl_cl.prc_load_all_facts();

-- screenshot: task2.png
-- Log after run 1: prc_load_ce_sales_incremental shows N rows (full initial load),
-- prc_load_fct_sales_dd_rolling shows the total across the 2 month partitions.
SELECT 
	procedure_name, 
	rows_affected, 
	status, 
	log_message, 
	log_dt
FROM bl_cl.mta_load_log
WHERE procedure_name IN (
    'bl_cl.prc_load_ce_sales_incremental',
    'bl_cl.prc_load_fct_sales_dd_rolling',
    'bl_cl.prc_load_all_facts'
)
ORDER BY log_dt DESC, log_id DESC
LIMIT 6;

-- screenshot: task3.png
-- Sample rows from CE_SALES to confirm 3NF fact is populated
SELECT 
	event_dt, 
	order_id, 
	product_id, 
	customer_id, 
	city_id, 
	sales_amt, 
	profit_amt, 
	source_system
FROM bl_3nf.ce_sales
WHERE product_id != -1
LIMIT 5;

-- screenshot: task4.png
-- Sample rows from FCT_SALES_DD to confirm DM fact is populated with surrogate keys
SELECT event_dt, order_id, product_surr_id, customer_surr_id, geography_surr_id,
       sales_amt, profit_margin_amt, source_system
FROM bl_dm.fct_sales_dd
WHERE product_surr_id != -1
LIMIT 5;

-- screenshot: task5.png
-- Confirm which partitions were created and their sizes
SELECT 
	c.relname AS partition_name, 
	pg_size_pretty(pg_relation_size(c.oid)) AS size
FROM pg_class c
INNER JOIN pg_namespace n ON n.oid = c.relnamespace
INNER JOIN pg_inherits i ON i.inhrelid = c.oid
INNER JOIN pg_class p ON p.oid = i.inhparent
INNER JOIN pg_namespace pn ON pn.oid = p.relnamespace
WHERE pn.nspname = 'bl_dm'
AND p.relname = 'fct_sales_dd'
ORDER BY c.relname;


---------------------------------------------------------------
-- SECTION 6: RUN 2 (idempotency check)
---------------------------------------------------------------

CALL bl_cl.prc_load_all_facts();

-- screenshot: task6.png
-- CE_SALES incremental: rows_affected = 0 because MAX(event_dt) in CE_SALES already
-- equals the max date in SA, so nothing passes the high-water mark filter.
-- FCT_SALES_DD rolling: rows_affected = same as run 1. The rolling window always
-- re-loads the 2 most recent months by design (DETACH old, build fresh, ATTACH).
-- This is expected and correct - re-running on unchanged source produces identical partitions.
SELECT 
	procedure_name, 
	rows_affected, 
	status, 
	log_message, 
	log_dt
FROM bl_cl.mta_load_log
WHERE procedure_name IN (
    'bl_cl.prc_load_ce_sales_incremental',
    'bl_cl.prc_load_fct_sales_dd_rolling',
    'bl_cl.prc_load_all_facts'
)
ORDER BY log_dt DESC, log_id DESC
LIMIT 6;


---------------------------------------------------------------
-- SECTION 7: DUPLICATE CHECKS
---------------------------------------------------------------

-- screenshot: task7_1.png
-- No duplicate rows in CE_SALES. Business key: order_id + product_id + customer_id + event_dt + source_system.
-- The EXISTS check in the incremental procedure prevents the same row from being inserted twice.
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
-- RESULT: 0 ROWS

-- screenshot: task7_2.png
-- No duplicate rows in FCT_SALES_DD. The DETACH+DROP+ATTACH pattern replaces each
-- partition completely on every rolling window run, so duplicates within a partition
-- are structurally impossible.
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
-- RESULT: 0 ROWS


---------------------------------------------------------------
-- SECTION 8: BUSINESS KEY COVERAGE CHECKS
---------------------------------------------------------------

-- screenshot: task8_1.png
-- Every order_id from SA_DOMESTIC appears in CE_SALES.
SELECT COUNT(*) AS sa_domestic_orders_not_in_3nf
FROM (
    SELECT DISTINCT order_id
    FROM sa_domestic.src_domestic_sales
    WHERE order_id IS NOT NULL
) sa
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_sales ce
    WHERE ce.order_id = sa.order_id
    AND ce.source_system = 'SA_DOMESTIC'
);
-- RESULT: 0

-- screenshot: task8_2.png
-- Every order_id from SA_INTERNATIONAL appears in CE_SALES.
SELECT COUNT(*) AS sa_international_orders_not_in_3nf
FROM (
    SELECT DISTINCT order_id
    FROM sa_international.src_international_sales
    WHERE order_id IS NOT NULL
) sa
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_sales ce
    WHERE ce.order_id = sa.order_id
    AND ce.source_system = 'SA_INTERNATIONAL'
);
-- RESULT: 0

-- screenshot: task8_3.png
-- Every CE_SALES row in the 2-month rolling window appears in FCT_SALES_DD.
-- Rows outside the window are not expected in FCT_SALES_DD (only 2 months kept in DM).
WITH rolling_window_months AS (
    SELECT DISTINCT DATE_TRUNC('month', event_dt)::DATE AS month_start
    FROM bl_3nf.ce_sales
    ORDER BY month_start DESC
    LIMIT 2
),
window_range AS (
    SELECT MIN(month_start) AS from_dt,
           MAX(month_start) + INTERVAL '1 month' AS to_dt
    FROM rolling_window_months
)
SELECT COUNT(*) AS ce_sales_orders_not_in_dm
FROM bl_3nf.ce_sales ce
INNER JOIN window_range wr ON ce.event_dt >= wr.from_dt AND 
							  ce.event_dt < wr.to_dt
WHERE NOT EXISTS (
    SELECT 1 
    FROM bl_dm.fct_sales_dd fct
    WHERE fct.order_id = ce.order_id
    AND fct.source_system = ce.source_system
    AND fct.event_dt = ce.event_dt
);
-- RESULT: 0


---------------------------------------------------------------
-- SECTION 9: FINAL COMPREHENSIVE OBJECT VERIFICATION
---------------------------------------------------------------

-- screenshots: task9_1.png and task9_2.png
-- Single master check to showcase that all required tables and procedures are loaded correctly
SELECT 
	table_schema, 
	table_name, 
	table_type 
FROM information_schema.tables 
WHERE table_schema IN ('bl_3nf', 'bl_dm')
UNION ALL
SELECT 
	routine_schema AS table_schema, 
	routine_name AS table_name, 
	routine_type AS table_type
FROM information_schema.routines
WHERE routine_schema IN ('bl_cl', 'bl_3nf', 'bl_dm')
ORDER BY table_schema, 
		 table_type, 
table_name;