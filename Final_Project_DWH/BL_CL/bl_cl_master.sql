-- BL_CL MASTER: Single procedure that runs the entire ETL pipeline
-- Global Retail Superstore Sales
-- Tamar Tutisani
-- Call CALL bl_cl.prc_run_all() to execute the full load in correct order.
--
-- Correct execution order:
--   1. prc_load_all_3nf()      (all CE_ dimension tables)
--   2. prc_load_all_dm_dims()  (all DIM_ tables)
--   3. prc_load_all_facts()    (CE_SALES incremental + FCT_SALES_DD rolling)
-----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE bl_cl.prc_run_all()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc CONSTANT VARCHAR := 'bl_cl.prc_run_all';
BEGIN
    RAISE NOTICE '% started at %', v_proc, NOW();

    -- Step 0: Load SA staging (EXT_ -> SRC_)
    CALL bl_cl.prc_load_all_src();

    -- Step 1: Load all 3NF dimensions
    CALL bl_cl.prc_load_all_3nf();

    -- Step 2: Load all DM dimensions
    CALL bl_cl.prc_load_all_dm_dims();

    -- Step 3: Load fact tables
    CALL bl_cl.prc_load_all_facts();

    CALL bl_cl.prc_log(v_proc, 0, 'SUCCESS',
        'Full ETL pipeline completed successfully at ' || NOW()::TEXT);

    RAISE NOTICE '% finished at %', v_proc, NOW();

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.prc_log(v_proc, 0, 'ERROR',
        'Full ETL pipeline failed: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE);
    RAISE;
END;
$$;

-- Execute the full pipeline
CALL bl_cl.prc_run_all();

-- Verify the log after full run
SELECT procedure_name, rows_affected, status, log_message, log_dt
FROM bl_cl.mta_load_log
ORDER BY log_dt DESC, log_id DESC
LIMIT 20;