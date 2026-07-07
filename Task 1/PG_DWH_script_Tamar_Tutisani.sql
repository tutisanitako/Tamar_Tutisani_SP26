-- POSTGRESQL TASK 1: DATABASE ARCHITECTURE
-- Global Retail Superstore Sales
-- Tamar Tutisani
-- Module: PG_DWH
-----------------------------------------------------------------------------


-- TASK 1: CREATE NEW DATABASE
-----------------------------------------------------------------------------

CREATE DATABASE test_db;

-- Investigate the pg_database catalog: shows all databases in the cluster
-- and which tablespace each one lives in
SELECT d.oid,
	   d.datname,
	   d.datistemplate,
	   d.datallowconn,
	   t.spcname
FROM pg_database d
JOIN pg_tablespace t ON t.oid = d.dattablespace;

/*
Expected observations:
- test_db appears with datistemplate = false, datallowconn = true
- spcname = 'pg_default' (default tablespace, inside the PostgreSQL data directory)
- System databases postgres, template0, template1 are also visible
- All databases in the cluster share the same config files and port
- Objects in one database cannot be accessed from another database directly
*/


-- TASK 2: CREATE NEW TABLESPACE
-----------------------------------------------------------------------------

-- OS prerequisite (run in terminal before this SQL):
-- mkdir -p /var/lib/postgresql/data/tblspc_test
-- chown postgres:postgres /var/lib/postgresql/data/tblspc_test

CREATE TABLESPACE mytablespace_v2
LOCATION 'C:/Users/tutis/pg_tablespaces/tblspc_test';

-- Verify the new tablespace exists
SELECT *
FROM pg_tablespace;
 
/*
Expected observations:
- Three tablespaces now visible: pg_global, pg_default, mytablespace_v2
- pg_global holds cluster-wide system objects shared across all databases
- pg_default is the storage location for all user objects created without a
  tablespace specification
- mytablespace_v2 maps to the new filesystem path we created
- Tablespaces provide physical separation of data (different disks/paths)
*/
 
-- Move test_db to the new tablespace
ALTER DATABASE test_db SET TABLESPACE mytablespace_v2;
 
-- Re-run the database query to confirm the change
SELECT d.oid,
	   d.datname,
	   d.datistemplate,
	   d.datallowconn,
	   t.spcname
FROM pg_database d
JOIN pg_tablespace t ON t.oid = d.dattablespace;
 
/*
Expected observations:
- test_db row now shows spcname = 'mytablespace_v2' instead of 'pg_default'
- Database files physically moved to /var/lib/postgresql/data/tblspc_test
- Unlike schemas (logical), tablespaces are a physical concept, they map
  directly to filesystem directories
- Cannot be used independently of the cluster (depends on cluster metadata)
*/
 


-- TASK 3: CREATE NEW SCHEMA
-- Connect to test_db first (\c test_db in psql), then run:
-----------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS labs;

CREATE TABLE labs.person (
	id INTEGER NOT NULL,
	name VARCHAR(15)
);

-- Verify the table lives in the correct schema
SELECT schemaname,
	   tablename
FROM pg_tables
WHERE tablename = 'person';

/*
Expected observations:
- schemaname = 'labs', tablename = 'person'
- The public schema has no 'person' table
- Schemas provide logical separation within a database with no physical impact
*/

-- Insert rows with explicit schema qualification
INSERT INTO labs.person VALUES (1, 'Bob');
INSERT INTO labs.person VALUES (2, 'Alice');
INSERT INTO labs.person VALUES (3, 'Robert');

-- Demonstrate search_path: default does not include 'labs'
SHOW search_path;

-- After SET, bare table name resolves to labs.person
SET search_path TO labs, public;

INSERT INTO labs.person (id, name)
SELECT v.id, v.name
FROM (VALUES 
    (1, 'Bob'),
    (2, 'Alice'),
    (3, 'Robert')
) AS v(id, name)
WHERE NOT EXISTS (
    SELECT 1 
    FROM labs.person p 
    WHERE p.id = v.id
);

SELECT * FROM labs.person 

/*
Expected observations:
- Before SET search_path: INSERT INTO person fails ("relation does not exist")
  because default search_path resolves against "$user" and public, not labs
- After SET search_path TO labs, public: bare table name resolves correctly
- search_path controls schema resolution order, similar to PATH in an OS
*/


-- TASK 4: INVESTIGATE MVCC
-----------------------------------------------------------------------------

-- Prerequisite: install pageinspect extension (requires superuser)
CREATE EXTENSION IF NOT EXISTS pageinspect;

-- Baseline: system columns showing MVCC metadata before any DML
SELECT p.id,
	   p.name,
	   p.ctid,
	   p.xmin,
	   p.xmax
FROM labs.person p;

SELECT t_xmin,
	   t_xmax,
	   t_ctid,
	   tuple_data_split('labs.person'::regclass, t_data, t_infomask, t_infomask2, t_bits)
FROM heap_page_items(get_raw_page('labs.person', 0));

/*
Expected observations (baseline):
- xmin = transaction ID that inserted each row
- xmax = 0 for all rows (no deletions yet, all rows are live)
- ctid = (0,1), (0,2), (0,3), page 0, slots 1-3
*/

-- INSERT: new row gets xmin = current txid, xmax = 0
INSERT INTO labs.person VALUES (4, 'John');

-- UPDATE: marks old row version with xmax, inserts new row version with new xmin
-- PostgreSQL does NOT update in-place; two physical rows exist simultaneously
UPDATE labs.person
SET name = 'Alex'
WHERE id = 2;

-- DELETE: sets xmax on the row, does NOT physically remove it from the page
DELETE FROM labs.person
WHERE id = 3;

-- INSERT then DELETE: both xmin and xmax set, immediately a dead tuple
INSERT INTO labs.person VALUES (999, 'Test');
DELETE FROM labs.person WHERE id = 999;

-- Final inspection: shows all row versions including dead tuples
SELECT t_xmin,
	   t_xmax,
	   t_ctid,
	   tuple_data_split('labs.person'::regclass, t_data, t_infomask, t_infomask2, t_bits)
FROM heap_page_items(get_raw_page('labs.person', 0));

/*
Expected observations (after DML):
- INSERT: new row has xmin = insert txid, xmax = 0
- UPDATE: old 'Alice' row has xmax set (logically deleted); new 'Alex' row has
  xmin = update txid, xmax = 0; ctid of old row points to new row (update chain)
- DELETE: id=3 row has xmax set; still physically present on the page
- id=999 row: both xmin and xmax set, inserted and deleted in same session,
  immediately a dead tuple invisible to all future transactions
- Dead tuples remain on the page until VACUUM runs
- MVCC gives every transaction a consistent snapshot via xmin/xmax comparisons;
  readers never block writers, writers never block readers
*/


-- TASK 5: INVESTIGATE VACUUM
-----------------------------------------------------------------------------

-- Confirm dead tuples are present before VACUUM
SELECT t_xmin,
	   t_xmax,
	   t_ctid,
	   tuple_data_split('labs.person'::regclass, t_data, t_infomask, t_infomask2, t_bits)
FROM heap_page_items(get_raw_page('labs.person', 0));

-- Step 5a: Standard VACUUM, non-blocking, marks dead space as reusable
VACUUM labs.person;

SELECT t_xmin,
	   t_xmax,
	   t_ctid,
	   tuple_data_split('labs.person'::regclass, t_data, t_infomask, t_infomask2, t_bits)
FROM heap_page_items(get_raw_page('labs.person', 0));

/*
Expected observations after VACUUM:
- Dead tuple slots are removed from the item pointer array
- Physical file size on disk does NOT shrink, space is marked reusable in
  the Free Space Map (FSM) for future inserts
- VACUUM is non-blocking: other transactions can read/write concurrently
*/

-- Step 5b: Insert a new row, should reuse space freed by VACUUM
INSERT INTO labs.person VALUES (5, 'Sarah');

SELECT t_xmin,
	   t_xmax,
	   t_ctid,
	   tuple_data_split('labs.person'::regclass, t_data, t_infomask, t_infomask2, t_bits)
FROM heap_page_items(get_raw_page('labs.person', 0));

/*
Expected observations after INSERT:
- Sarah's row may occupy a slot previously freed by VACUUM (lower slot number)
- Confirms VACUUM correctly updates the FSM so new inserts reuse freed space
  without extending the table file
*/

-- Step 5c: VACUUM FULL, blocking, rewrites table compactly, releases disk space
VACUUM FULL labs.person;

SELECT t_xmin,
	   t_xmax,
	   t_ctid,
	   tuple_data_split('labs.person'::regclass, t_data, t_infomask, t_infomask2, t_bits)
FROM heap_page_items(get_raw_page('labs.person', 0));

/*
Expected observations after VACUUM FULL:
- Table fully rewritten into a new compact file; ctid values restart from (0,1)
- Dead tuples gone AND physical disk space released back to the OS
- Requires an EXCLUSIVE LOCK on the table, all reads/writes blocked during run
- Not suitable for busy production tables; use only during maintenance windows

Summary:
  VACUUM      — removes dead tuples, marks space reusable, non-blocking,
                file size unchanged on disk; run automatically by autovacuum
  VACUUM FULL — rewrites entire table, releases disk space to OS, requires
                exclusive lock; manual only, never run by autovacuum
*/