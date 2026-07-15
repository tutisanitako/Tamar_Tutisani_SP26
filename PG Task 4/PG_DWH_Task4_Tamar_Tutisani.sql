-- TASK 4: POSTGRESQL DATA ACCESS AND QUERY OPTIMIZER
-- Tamar Tutisani
-- Module: PG_DWH
-----------------------------------------------------------------------------


-- PREREQUISITE: Connect to test_db and set the search path to labs schema.
-- SET search_path TO labs, public;
-----------------------------------------------------------------------------


----------------------------------------------------------------
-- TASK 1.1: TABLE WITHOUT INDEX
----------------------------------------------------------------
-- Create the table, fill it with data, and examine EXPLAIN plans.

DROP TABLE IF EXISTS labs.test_index_plan;

CREATE TABLE labs.test_index_plan (
    num  FLOAT NOT NULL,
    load_date TIMESTAMPTZ NOT NULL
);

-- Fill the table with ~5 years of data (one row every 10 seconds).
-- This produces approximately 15.7 million rows. Execute time - 1m 0s
INSERT INTO labs.test_index_plan (num, load_date)
SELECT random(),
       x
FROM generate_series(
    '2017-01-01 0:00'::TIMESTAMPTZ,
    '2021-12-31 23:59:59'::TIMESTAMPTZ,
    '10 seconds'::INTERVAL
) x;

-- Disable parallel query to get a clean single-worker plan.
SET max_parallel_workers_per_gather = 0;

-- Run 1: EXPLAIN only (no execution, shows estimated plan)
EXPLAIN
SELECT *
FROM labs.test_index_plan
WHERE load_date BETWEEN '2021-09-01 0:00' AND '2021-10-31 11:59:59'
ORDER BY 1;

-- result shown in task1_1a.png
/*
Plan:
  Sort  (cost=382024.20..383362.60 rows=535358 width=16)
    -> Seq Scan on test_index_plan (cost=0.00..321932.00 rows=535358 width=16)
         Filter: (load_date >= ... AND load_date <= ...)

Observation: with no index the planner picks a Sequential Scan,it reads
every page of the table and applies the filter row by row. The Sort node
appears because ORDER BY num cannot be satisfied by any index. Only cost
estimates are shown; nothing is actually executed.
*/

-- Run 2: EXPLAIN ANALYZE (actually executes, shows actual rows and timing)
EXPLAIN ANALYZE
SELECT *
FROM labs.test_index_plan
WHERE load_date BETWEEN '2021-09-01 0:00' AND '2021-10-31 11:59:59'
ORDER BY 1;
-- result shown in task1_1b.png
/*
Plan adds actual execution data:
  Sort Method: external merge, Disk: 13312kB
  Seq Scan: actual rows=522720, removed by filter=15,253,920
  Execution Time: 26887.525 ms

Observation: The query performs a full table scan, discarding 15.2M rows. The 
sort operation spills to disk due to insufficient work_mem. Total I/O overhead 
is high (85,280 pages read).
*/

-- Running again, no changes, to check caching effect
EXPLAIN ANALYZE
SELECT *
FROM labs.test_index_plan
WHERE load_date BETWEEN '2021-09-01 0:00' AND '2021-10-31 11:59:59'
ORDER BY 1;
/*
Run 2b: shared hit=16226, read=69054, Execution Time: 2225.208 ms.
Observation: Second execution is ~12x faster than the first, benefiting from 
cached index/heap pages in shared_buffers. Disk I/O remains a factor for the 
remainder of the pages.
*/

-- Run 3: EXPLAIN (ANALYZE, BUFFERS) (adds buffer hit/read counts)
EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM labs.test_index_plan
WHERE load_date BETWEEN '2021-09-01 0:00' AND '2021-10-31 11:59:59'
ORDER BY 1;
-- result shown in task1_1c.png
/*
Plan is identical to Run 2 with buffer details already included there.
Comparing Run 2 and Run 3:
  Run 2: shared hit=15965  read=69315  Execution Time: ~1974ms
  Run 3: shared hit=16059  read=69221  Execution Time: ~1962ms

Observation: the buffer counts and execution time are nearly the same between
the two runs. Disk reads remain dominant (69k+ pages) because the table is
too large to fit in shared_buffers. The difference between the three variants:
  EXPLAIN                 		- estimated plan only, nothing executed
  EXPLAIN ANALYZE      		    - executes the query, adds actual rows and timing
  EXPLAIN (ANALYZE, BUFFERS) 	- also reports shared_buffers hit/read counts
                               and temp I/O (visible here as disk sort spill)
*/


----------------------------------------------------------------
-- TASK 1.2: ADDING INDEX
----------------------------------------------------------------

-- Step 1: Create a B-Tree index on load_date
CREATE INDEX idx_test_index_plan_load_date
    ON labs.test_index_plan USING BTREE (load_date);
--execute time 21s

SET max_parallel_workers_per_gather = 0;

-- Run 1: EXPLAIN only
EXPLAIN
SELECT *
FROM labs.test_index_plan
WHERE load_date BETWEEN '2021-09-01 0:00' AND '2021-10-31 11:59:59'
ORDER BY 1;
-- result shown in task1_2a.png

/*
Plan:
  Sort  (cost=79568.18..80906.57 rows=535353 width=16)
    -> Index Scan using idx_test_index_plan_load_date on test_index_plan
         (cost=0.43..19476.49 rows=535353 width=16)
         Index Cond: (load_date >= ... AND load_date <= ...)

Observation: the planner switched from Seq Scan to Index Scan because the
WHERE clause is selective, only ~2 months out of 5 years. Instead of reading
all 85,280 pages of the table, it traverses the B-Tree to find only the pages
containing matching rows. The Sort node remains because the index is on
load_date but ORDER BY is on num, the index cannot provide the rows pre-sorted
by num. Only cost estimates are shown; nothing is executed.
*/

-- Run 2: EXPLAIN ANALYZE
EXPLAIN ANALYZE
SELECT *
FROM labs.test_index_plan
WHERE load_date BETWEEN '2021-09-01 0:00' AND '2021-10-31 11:59:59'
ORDER BY 1;
-- result shown in task1_2b.png

/*
Plan adds actual execution data:
  Bitmap Heap Scan: actual time=20.262..84.850ms, rows=522720
  Sort Method: external merge, Disk: 13312kB
  Buffers: shared hit=2826, read=1431
  Execution Time: 539.566 ms

Observation: Index usage reduces scan time significantly. The plan uses a 
Bitmap Heap Scan to retrieve rows. The sort remains the bottleneck, spilling 
to disk as in Task 1.1. Performance is ~50x better than the initial cold Seq Scan.
*/

-- Run again, no changes, to check caching effect
EXPLAIN ANALYZE
SELECT *
FROM labs.test_index_plan
WHERE load_date BETWEEN '2021-09-01 0:00' AND '2021-10-31 11:59:59'
ORDER BY 1;

/*
Run 2b: shared hit=4257, read=0, Execution Time: 519.859 ms.
Observation: Zero disk reads confirm all data is now in shared_buffers. The 
slight performance gain (539ms -> 519ms) reflects the elimination of disk 
latency; however, the Sort remains the primary time consumer.
*/

-- Run 3: EXPLAIN (ANALYZE, BUFFERS)
EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM labs.test_index_plan
WHERE load_date BETWEEN '2021-09-01 0:00' AND '2021-10-31 11:59:59'
ORDER BY 1;
-- result shown in task1_2c.png

/*
Plan:
  shared hit=4257  read=0
  Execution Time: 544ms

Observation: on this second execution all 4257 pages were served from
shared_buffers (read=0). This is a clear caching effect, the Index Scan
in Run 2 loaded the relevant index and heap pages into shared_buffers, so
Run 3 needed no disk I/O at all for the scan. Execution time dropped from
770ms to 544ms purely because of this. The sort still spills to disk
(temp read=1664) because work_mem has not changed. Comparing all three runs:
  Run 1 (EXPLAIN)            	  - estimated plan only, no execution
  Run 2 (EXPLAIN ANALYZE)   	  - actual scan time 207ms, mostly disk reads
  Run 3 (EXPLAIN ANALYZE BUFFERS) - actual scan time 95ms, all cache hits,
                                    confirms the buffer warming effect
*/

-- Step 3: What change to the query enables Index Only Scan?
-- To trigger an Index Only Scan the query must select ONLY columns that are
-- in the index (load_date here). If additional columns (num, *) are requested,
-- PostgreSQL must visit the heap to fetch them - Index Only Scan is impossible.
-- Also run VACUUM first so the visibility map is up to date (heap fetches = 0).
VACUUM labs.test_index_plan;

EXPLAIN ANALYZE
SELECT load_date
FROM labs.test_index_plan
WHERE load_date BETWEEN '2021-09-01 0:00' AND '2021-10-31 11:59:59'
ORDER BY load_date;
-- result shown in task1_2d.png

/*
Result:
  Index Only Scan using idx_test_index_plan_load_date
    Heap Fetches: 0, Buffers: shared hit=1432
  Execution Time: 79.304 ms

Observation: Heap Fetches: 0 confirms no heap access. All 1432 buffers came
from shared_buffers (VACUUM kept the visibility map current). No Sort node
needed since the index already returns rows in load_date order. Fastest plan
of all tested so far.
*/

-- Step 4: Replace B-Tree with BRIN index
DROP INDEX IF EXISTS labs.idx_test_index_plan_load_date;

CREATE INDEX idx_test_index_plan_brin
    ON labs.test_index_plan USING BRIN (load_date);

SET max_parallel_workers_per_gather = 0;

-- Run 1: EXPLAIN only
EXPLAIN
SELECT *
FROM labs.test_index_plan
WHERE load_date BETWEEN '2021-09-01 0:00' AND '2021-10-31 11:59:59'
ORDER BY 1;
-- result shown in task1_2e.png
/*
Result:
  Sort (cost=294381.12..295719.50)
    -> Bitmap Heap Scan (cost=154.57..234289.43)
         -> Bitmap Index Scan on idx_test_index_plan_brin (cost=0.00..20.74)

Observation: BRIN probe itself is nearly free (cost 20.74), but the Bitmap
Heap Scan cost is high because BRIN only excludes whole block ranges, not
individual rows, so more heap pages must be checked than with a B-Tree.
*/

-- Run 2: EXPLAIN ANALYZE
EXPLAIN ANALYZE
SELECT *
FROM labs.test_index_plan
WHERE load_date BETWEEN '2021-09-01 0:00' AND '2021-10-31 11:59:59'
ORDER BY 1;
-- result shown in task1_2f.png

/*
Result:
  Sort (actual time=426.454..545.378, Disk: 13312kB)
    -> Bitmap Heap Scan (actual time=2.080..107.674 rows=522720)
         Rows Removed by Index Recheck: 21920
         Heap Blocks: lossy=2944
         -> Bitmap Index Scan (actual time=0.213..0.213 rows=29440)
  Execution Time: 598.068 ms

Observation: the BRIN scan itself takes 0.213ms (10 buffer hits) - tiny, as
expected. But the bitmap is lossy: BRIN only knows which block ranges might
match, so all 2944 heap blocks get rechecked, discarding 21,920 non-matching
rows. This pushes execution time to 598ms, ~7.5x slower than the B-Tree Index
Only Scan (79.304ms). Sort still spills to disk since num has no index.

B-Tree vs BRIN, actual numbers:
  B-Tree Index Only Scan   - 79.304 ms  (no heap access, no sort)
  BRIN Bitmap Heap Scan    - 598.068 ms (cheap probe, expensive recheck)

BRIN's small size and fast build time matter more on tables too large for a
B-Tree to be practical; here, on this dataset/query, B-Tree wins on speed.
*/

-- Running again, no changes, to check caching effect
EXPLAIN ANALYZE
SELECT *
FROM labs.test_index_plan
WHERE load_date BETWEEN '2021-09-01 0:00' AND '2021-10-31 11:59:59'
ORDER BY 1;
-- result shown in task1_2f2.png
/*
Run 2:  shared hit=3004+2988  read=0  Execution Time: 450.523 ms
Run 2b: shared hit=3004+2988  read=0  Execution Time: 392.831 ms

Observation: no disk reads in either run, pages were already cached from
the prior B-Tree runs. Timing difference is normal run-to-run variance, not
a caching effect.
*/

----------------------------------------------------------------
-- TASK 2.1: BULK INSERT
----------------------------------------------------------------

-- Step 1: Create test_inserts table
DROP TABLE IF EXISTS labs.test_inserts;

CREATE TABLE labs.test_inserts (
    num FLOAT NOT NULL,
    load_date TIMESTAMPTZ NOT NULL
);

-- Step 2: Add B-Tree index on load_date before the bulk insert
CREATE INDEX idx_test_inserts_load_date
    ON labs.test_inserts USING BTREE (load_date);

-- Step 3: Bulk INSERT from test_index_plan
INSERT INTO labs.test_inserts (num, load_date)
SELECT num,
       load_date
FROM labs.test_index_plan;
-- execute time 1m 40s
/*
Observation: inserting into a table that already has an index is slower than
inserting into an unindexed table, because each row must also update the B-Tree
structure. For very large loads the recommended pattern is: drop the index,
insert all rows, then recreate the index. The bulk index build is faster than
15M incremental insertions into an existing tree.
*/

-- Step 4: Create emp table (do NOT drop after this task)
DROP TABLE IF EXISTS labs.emp;

CREATE TABLE labs.emp (
    empno NUMERIC(4)  NOT NULL CONSTRAINT emp_pk PRIMARY KEY,
    ename VARCHAR(10) UNIQUE,
    job VARCHAR(9),
    mgr NUMERIC(4),
    hiredate DATE
);

-- Step 5: Rewrite 14 separate INSERTs as a single multi-row INSERT.
-- Original: 14 individual INSERT statements, each paying full overhead
-- (lock check, schema resolution, type checks, WAL write) independently.
-- Efficient: one INSERT ... VALUES (...), (...), ... pays overhead once
-- for all 14 rows and produces a single WAL record for the batch.
INSERT INTO labs.emp (empno, ename, job, mgr, hiredate)
VALUES
    (1,  'SMITH',  'CLERK',     13,   '1980-12-17'),
    (2,  'ALLEN',  'SALESMAN',   6,   '1981-02-20'),
    (3,  'WARD',   'SALESMAN',   6,   '1981-02-22'),
    (4,  'JONES',  'MANAGER',    9,   '1981-04-02'),
    (5,  'MARTIN', 'SALESMAN',   6,   '1981-09-28'),
    (6,  'BLAKE',  'MANAGER',    9,   '1981-05-01'),
    (7,  'CLARK',  'MANAGER',    9,   '1981-06-09'),
    (8,  'SCOTT',  'ANALYST',    4,   '1987-04-19'),
    (9,  'KING',   'PRESIDENT',  NULL, '1981-11-17'),
    (10, 'TURNER', 'SALESMAN',   6,   '1981-09-08'),
    (11, 'ADAMS',  'CLERK',      8,   '1987-05-23'),
    (12, 'JAMES',  'CLERK',      6,   '1981-12-03'),
    (13, 'FORD',   'ANALYST',    4,   '1981-12-03'),
    (14, 'MILLER', 'CLERK',      7,   '1982-01-23')
ON CONFLICT DO NOTHING;

SELECT * FROM labs.emp ORDER BY empno;
-- results shown in task2_1.png

----------------------------------------------------------------
-- TASK 2.2: COPY COMMAND
----------------------------------------------------------------

-- Step 1: Export full test_index_plan to CSV, load_date quoted, num unquoted
-- ran this in psql:
-- \copy labs.test_index_plan (num, load_date) TO 'C:/temp/test_index_plan.csv' 
-- WITH (FORMAT CSV, DELIMITER ',', HEADER, FORCE_QUOTE (load_date))
-- COPY 15776640

-- Step 2: Export only September 1 morning rows to a separate file
-- ran this in psql:
-- \copy (SELECT num, load_date FROM labs.test_index_plan WHERE load_date 
-- BETWEEN '2021-09-01 0:00' AND '2021-09-01 11:59:59') TO 
-- 'C:/temp/test_index_plan_short.csv' DELIMITER ',' CSV HEADER
-- COPY 4320


-- Step 3: Create test_copy table
DROP TABLE IF EXISTS labs.test_copy;

CREATE TABLE labs.test_copy (
    num FLOAT NOT NULL,
    load_date TIMESTAMPTZ NOT NULL
);

-- Step 4: Add B-Tree index on load_date
CREATE INDEX idx_test_copy_load_date
    ON labs.test_copy USING BTREE (load_date);

-- Step 5: COPY from CSV into test_copy.
-- ran this in psql:
--   \copy labs.test_copy FROM 'C:/temp/test_index_plan.csv' DELIMITER ',' CSV HEADER;

/*
Observation: COPY FROM reads the entire file in one transaction and inserts
all rows at once. Much faster than equivalent INSERT ... VALUES because
overhead is paid once per COPY statement, not once per row.
Trade-off:
  COPY   - very fast for bulk loads, strict format, entire operation fails
           if any single line is malformed (no partial commit)
  INSERT - flexible, error-tolerant per row, much slower at scale because
           each statement carries its own overhead and WAL record
*/


----------------------------------------------------------------
-- TASK 2.3: UPSERT
----------------------------------------------------------------

-- Insert / update five rows using a single UPSERT statement.
-- empno is the PRIMARY KEY, so ON CONFLICT (empno) is the arbiter.
--
-- empno 1  (SMITH)  already exists -> UPDATE to MANAGER / 2021-12-01
-- empno 4  (JONES)  already exists -> UPDATE to ANALIST / 2021-12-01
--                                     (task specifies 'ANALIST', kept as-is)
-- empno 11 (ADAMS)  already exists -> UPDATE to SALESMAN / 2021-12-01
-- empno 14 (MILLER) already exists -> UPDATE ename to KELLY, job to CLERK
-- empno 15 (HANNAH) does not exist -> INSERT new row

INSERT INTO labs.emp (empno, ename, job, mgr, hiredate)
VALUES
    (1, 'SMITH', 'MANAGER', 13, '2021-12-01'),
    (14, 'KELLY',  'CLERK',  1, '2021-12-01'),
    (15, 'HANNAH', 'CLERK', 1, '2021-12-01'),
    (11, 'ADAMS',  'SALESMAN', 8, '2021-12-01'),
    (4, 'JONES', 'ANALIST', 9,  '2021-12-01')
ON CONFLICT (empno) DO UPDATE
    SET ename = EXCLUDED.ename,
        job = EXCLUDED.job,
        mgr = EXCLUDED.mgr,
        hiredate = EXCLUDED.hiredate;

SELECT * FROM labs.emp ORDER BY empno;
-- results shown in task2_3.png
/*
Expected outcome:
  empno 1  : SMITH  | MANAGER  |  2021-12-01 | 13 |   (updated from CLERK / 1980-12-17)
  empno 4  : JONES  | ANALIST  |  2021-12-01 |  9 |   (updated from MANAGER / 1981-04-02)
  empno 11 : ADAMS  | SALESMAN |  2021-12-01 |  8 |   (updated from CLERK / 1987-05-23)
  empno 14 : KELLY  | CLERK    |  2021-12-01 |  1 |   (ename changed from MILLER)
  empno 15 : HANNAH | CLERK    |  2021-12-01 |  1 |   (new row inserted)
  all other rows: unchanged

How UPSERT works:
  - ON CONFLICT (empno) checks the PRIMARY KEY as the conflict target.
  - Conflict found -> DO UPDATE applies EXCLUDED (the proposed row's values)
    to the existing row. EXCLUDED is PostgreSQL's alias for the row that was
    rejected by the constraint.
  - No conflict -> normal INSERT proceeds.
  - The operation is atomic: no intermediate state is visible to other
    transactions. This is the UPSERT guarantee ("UPDATE or INSERT").
  - Requires a unique constraint or primary key on the conflict target column.
*/