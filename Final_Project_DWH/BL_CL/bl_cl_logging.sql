-- BL_CL LOGGING: Log table and logging procedure
-- Global Retail Superstore Sales
-- Tamar Tutisani
-- Creates the centralized log table and the prc_log helper procedure.
-----------------------------------------------------------------------------

-- STEP 1: Create log table
-- Using CREATE TABLE IF NOT EXISTS so it is safe to re-run without data loss.
-- Columns: log_id (auto), log_dt (when), procedure_name (who),
--          rows_affected (how many), status (SUCCESS/ERROR), log_message (detail).
-----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bl_cl.mta_load_log (
    log_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    log_dt TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    procedure_name VARCHAR(200) NOT NULL,
    rows_affected INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(10) NOT NULL CHECK (status IN ('SUCCESS', 'ERROR')),
    log_message TEXT NOT NULL
);

-- STEP 2: Create logging helper procedure
-- Every loading procedure calls CALL bl_cl.prc_log(...) instead of writing
-- INSERT INTO mta_load_log directly. If the log table structure ever changes,
-- only this one procedure needs updating.
-----------------------------------------------------------------------------
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