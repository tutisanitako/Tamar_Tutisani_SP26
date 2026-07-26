-- BL_CL SETUP: Schema, Role, and Grants
-- Global Retail Superstore Sales
-- Tamar Tutisani
-- Creates the ETL role bl_cl_role and grants it access to all schemas.
-- Grants are to bl_cl_role specifically, NOT to PUBLIC.
-----------------------------------------------------------------------------

-- STEP 0: Create BL_CL schema
-----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS bl_cl;

-- STEP 1: Create the dedicated ETL role
-- This role is used by the ETL procedures in BL_CL to read from SA and
-- read/write to BL_3NF and BL_DM. Never grant to PUBLIC.
-----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bl_cl_role') THEN
        CREATE ROLE bl_cl_role;
    END IF;
END;
$$;

-- STEP 2: Grant SA layer access (read only for ETL)
-----------------------------------------------------------------------------
GRANT USAGE ON SCHEMA sa_domestic TO bl_cl_role;
GRANT USAGE ON SCHEMA sa_international TO bl_cl_role;
GRANT SELECT ON ALL TABLES IN SCHEMA sa_domestic TO bl_cl_role;
GRANT SELECT ON ALL TABLES IN SCHEMA sa_international TO bl_cl_role;

-- STEP 3: Grant BL_3NF access (read + write for ETL)
-----------------------------------------------------------------------------
GRANT USAGE ON SCHEMA bl_3nf TO bl_cl_role;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA bl_3nf TO bl_cl_role;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA bl_3nf TO bl_cl_role;

-- STEP 4: Grant BL_DM access (read + write + delete for ETL, needed for partition operations)
-----------------------------------------------------------------------------
GRANT USAGE ON SCHEMA bl_dm TO bl_cl_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA bl_dm TO bl_cl_role;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA bl_dm TO bl_cl_role;

-- STEP 5: Grant BL_CL itself (so the role can execute procedures and functions)
-----------------------------------------------------------------------------
GRANT USAGE ON SCHEMA bl_cl TO bl_cl_role;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA bl_cl TO bl_cl_role;
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA bl_cl TO bl_cl_role;