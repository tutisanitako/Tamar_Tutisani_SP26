-- Task: applying the TCL & DML Statement

-------------------------------------------------------

-- Task 2

-- STEP 1: CREATE table_to_delete

CREATE TABLE table_to_delete AS
SELECT 'veeeeeeery_long_string' || x AS col
FROM   generate_series(1, (10^7)::INT) x;
-- Execute time	22s

-------------------------------------------------------
-- STEP 2: Check initial space consumption
SELECT *, pg_size_pretty(total_bytes) AS total,
		  pg_size_pretty(index_bytes) AS INDEX,
		  pg_size_pretty(toast_bytes) AS toast,
		  pg_size_pretty(table_bytes) AS TABLE
FROM ( SELECT *, total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes
	   FROM (SELECT c.oid,nspname AS table_schema,
				    relname AS TABLE_NAME,
				    c.reltuples AS row_estimate,
				    pg_total_relation_size(c.oid) AS total_bytes,
				    pg_indexes_size(c.oid) AS index_bytes,
				    pg_total_relation_size(reltoastrelid) AS toast_bytes
			 FROM pg_class c
			 LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
			 WHERE relkind = 'r'
			 ) a
	   ) a
WHERE table_name LIKE '%table_to_delete%';

/*
oid		table_schema	table_name			row_estimate	total_bytes		index_bytes		toast_bytes		table_bytes		total		INDEX		toast		table
26153	public			table_to_delete		-1.0			602464256		0				8192			602456064		575 MB		0 bytes		8192 bytes	575 MB
*/


-------------------------------------------------------
-- STEP 3: DELETE 1/3 of rows
--3a: Note how much time it takes to perform this DELETE statement
DELETE FROM table_to_delete 
WHERE REPLACE(col, 'veeeeeeery_long_string','')::int % 3 = 0;
-- Execute time	13s


-- STEP 3b: Lookup how much space this table consumes after previous DELETE
/*
oid		table_schema	table_name			row_estimate	total_bytes		index_bytes		toast_bytes		table_bytes		total		INDEX		toast		table
26153	public			table_to_delete		9999896.0		602611712		0				8192			602603520		575 MB		0 bytes		8192 bytes	575 MB
*/


-- STEP 3c: VACUUM FULL to reclaim space
VACUUM FULL VERBOSE table_to_delete;
/*
Execute time 20s
output:
vacuuming "public.table_to_delete"
"public.table_to_delete": found 0 removable and 6666667 non-removable row versions in 73536 pages
*/


-- STEP 3d: Check space consumption of the table once again and make conclusions
/*
oid		table_schema	table_name			row_estimate	total_bytes		index_bytes		toast_bytes		table_bytes		total		INDEX		toast		table
26153	public			table_to_delete		6666667.0		401580032		0				8192			401571840		383 MB		0 bytes		8192 bytes	383 MB
*/


-- STEP 3e: Recreate ‘table_to_delete’ table
DROP TABLE table_to_delete;

CREATE TABLE table_to_delete AS 
SELECT 'veeeeeeery_long_string' || x AS col 
FROM generate_series(1,(10^7)::int) x;
-- Execute time	21s

-------------------------------------------------------
-- STEP 4: Issue the following TRUNCATE operation: TRUNCATE table_to_delete
-- 4a: Note how much time it takes to perform this TRUNCATE statement.
TRUNCATE table_to_delete;
-- Execute time	0.093s

-- STEP 4b: Compare with previous results and make conclusion.
--ran the check again
/*
oid		table_schema	table_name			row_estimate	total_bytes		index_bytes		toast_bytes		table_bytes		total		INDEX		toast		table
26163	public			table_to_delete		0.0				8192			0				8192			0				8192 bytes	0 bytes		8192 bytes	0 bytes
*/


-------------------------------------------------------
-- STEP 5: INVESTIGATION RESULTS SUMMARY

-- 5a: Space consumption of ‘table_to_delete’ table before and after each operation
/*
Operation					Total Size
-----------------------------------------------
After CREATE				575 MB
After DELETE (1/3 rows)		575 MB  (no change)
After VACUUM FULL			383 MB  (192 MB reclaimed)
After TRUNCATE				8192 bytes
*/


-- 5b: Compare DELETE and TRUNCATE

-- Execution time:
/*
DELETE		13s   (processes rows one by one, logs each row change)
TRUNCATE	0.093s (drops the data file entirely, no row-level work)
*/

-- Disk space usage:
/*
DELETE		does NOT free space immediately, dead rows stay on disk
			space only reclaimed after VACUUM FULL (575 MB -> 383 MB)
TRUNCATE	frees space immediately, table drops to 8192 bytes (metadata only)
*/

-- Transaction behavior:
/*
DELETE		fully transactional, every deleted row is written to the WAL log
			this is why it is slow and why space is not freed right away
TRUNCATE	also transactional in PostgreSQL, but only logs the file removal,
			not individual rows, which makes it much lighter
*/

-- Rollback possibility:
/*
DELETE		can be rolled back (if autocommit is off), all row changes are logged
TRUNCATE	can also be rolled back in PostgreSQL
			because PostgreSQL logs the operation at file level
*/


-- 5c: Explanations

-- Why DELETE does not free space immediately:
/*
PostgreSQL uses MVCC (Multi-Version Concurrency Control).
When rows are deleted, they are only marked as "dead", they remain
physically on disk so that other active transactions can still see
the old version of the data if needed.
The actual disk space is not reclaimed until VACUUM runs.
*/

-- Why VACUUM FULL changes table size:
/*
Regular VACUUM only marks dead row space as reusable for future inserts,
it does NOT return space to the operating system.
VACUUM FULL rewrites the entire table into a new file, physically removing
all dead rows and compacting the data.
The old file is discarded and the OS reclaims the freed blocks.
That is why size dropped from 575 MB to 383 MB after VACUUM FULL.
*/

-- Why TRUNCATE behaves differently:
/*
TRUNCATE does not delete rows one by one.
It simply removes the underlying data file and creates a new empty one.
There are no rows to log, no dead tuples to clean up, the table is
instantly empty and all disk space is immediately returned.
*/

-- How these operations affect performance and storage:
/*
DELETE	-- causes table bloat over time if VACUUM is not run regularly
		-- dead rows remain on disk and PostgreSQL still scans them,
		   slowing down queries and wasting storage
		-- autovacuum handles routine cleanup but VACUUM FULL is needed
		   for significant size reduction

TRUNCATE -- ideal when clearing an entire table
		 -- no bloat, no cleanup needed, instant space recovery
		 -- cannot target specific rows

Conclusion: for clearing full tables it's better to use TRUNCATE.
			for partial deletes, schedule regular VACUUM
			to avoid performance degradation from table bloat.
*/