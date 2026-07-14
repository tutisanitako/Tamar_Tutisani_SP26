-- TASK 6: PARTITIONING AND PARALLEL EXECUTION
-- Tamar Tutisani
-- Module: PG_DWH
-----------------------------------------------------------------------------

-- SET search_path TO labs, public;
-----------------------------------------------------------------------------


-----------------------------------------------------------------------------
-- TASK 1.1: USE INHERITANCE (partitioning by eventdate, one partition/year)
-----------------------------------------------------------------------------

DROP TABLE IF EXISTS labs.sales_info CASCADE;

CREATE TABLE labs.sales_info (
    id INTEGER,
    category VARCHAR(1),
    ischeck BOOLEAN,
    eventdate DATE
);

-- Step 1: create 5 child (partition) tables inheriting from sales_info,
-- one per year, each with a non-overlapping CHECK constraint on eventdate.
-- data is generated with (NOW() - up to ~1000 days), so i picked 2022-2026
-- to make sure all generated dates land somewhere.

DROP TABLE IF EXISTS labs.sales_info_2022;
CREATE TABLE labs.sales_info_2022 (
    CHECK (eventdate >= '2022-01-01' AND eventdate < '2023-01-01')
) INHERITS (labs.sales_info);

DROP TABLE IF EXISTS labs.sales_info_2023;
CREATE TABLE labs.sales_info_2023 (
    CHECK (eventdate >= '2023-01-01' AND eventdate < '2024-01-01')
) INHERITS (labs.sales_info);

DROP TABLE IF EXISTS labs.sales_info_2024;
CREATE TABLE labs.sales_info_2024 (
    CHECK (eventdate >= '2024-01-01' AND eventdate < '2025-01-01')
) INHERITS (labs.sales_info);

DROP TABLE IF EXISTS labs.sales_info_2025;
CREATE TABLE labs.sales_info_2025 (
    CHECK (eventdate >= '2025-01-01' AND eventdate < '2026-01-01')
) INHERITS (labs.sales_info);

DROP TABLE IF EXISTS labs.sales_info_2026;
CREATE TABLE labs.sales_info_2026 (
    CHECK (eventdate >= '2026-01-01' AND eventdate < '2027-01-01')
) INHERITS (labs.sales_info);

-- indexes are NOT inherited automatically, so each child needs its own
CREATE INDEX idx_sales_info_2022_eventdate ON labs.sales_info_2022 (eventdate);
CREATE INDEX idx_sales_info_2023_eventdate ON labs.sales_info_2023 (eventdate);
CREATE INDEX idx_sales_info_2024_eventdate ON labs.sales_info_2024 (eventdate);
CREATE INDEX idx_sales_info_2025_eventdate ON labs.sales_info_2025 (eventdate);
CREATE INDEX idx_sales_info_2026_eventdate ON labs.sales_info_2026 (eventdate);

-- Step 2 + 3: partition routing function + BEFORE INSERT trigger.
-- the trigger intercepts every INSERT on the parent, figures out the right
-- year partition, inserts there, and returns NULL so nothing goes into the
-- parent itself.

CREATE OR REPLACE FUNCTION labs.partition_sales_info() RETURNS TRIGGER
AS $$
BEGIN
    IF (NEW.eventdate >= '2026-01-01'::DATE AND
        NEW.eventdate <  '2027-01-01'::DATE) THEN
        INSERT INTO labs.sales_info_2026 VALUES (NEW.*);
    ELSIF (NEW.eventdate >= '2025-01-01'::DATE AND
           NEW.eventdate <  '2026-01-01'::DATE) THEN
        INSERT INTO labs.sales_info_2025 VALUES (NEW.*);
    ELSIF (NEW.eventdate >= '2024-01-01'::DATE AND
           NEW.eventdate <  '2025-01-01'::DATE) THEN
        INSERT INTO labs.sales_info_2024 VALUES (NEW.*);
    ELSIF (NEW.eventdate >= '2023-01-01'::DATE AND
           NEW.eventdate <  '2024-01-01'::DATE) THEN
        INSERT INTO labs.sales_info_2023 VALUES (NEW.*);
    ELSIF (NEW.eventdate >= '2022-01-01'::DATE AND
           NEW.eventdate <  '2023-01-01'::DATE) THEN
        INSERT INTO labs.sales_info_2022 VALUES (NEW.*);
    ELSE
        RAISE EXCEPTION 'Out of range';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS partition_sales_info_trigger ON labs.sales_info;

CREATE TRIGGER partition_sales_info_trigger
    BEFORE INSERT ON labs.sales_info
    FOR EACH ROW EXECUTE PROCEDURE labs.partition_sales_info();

-- Step 4: generate 10,000,000 test rows. took about 5m 50s because the
-- trigger fires once per row in PL/pgSQL, which is much slower than
-- declarative partitioning's internal routing (as we'll see in task 1.2).
INSERT INTO labs.sales_info (id, category, ischeck, eventdate)
SELECT id,
       ('{"A","B","C","D","E","F","J","H","I","J","K"}'::TEXT[])[((RANDOM())*10)::INTEGER] AS category,
       ((1*(RANDOM())::INTEGER) < 1) AS ischeck,
       (NOW() - '10 day'::INTERVAL * (RANDOM()::INT * 100))::DATE AS eventdate
FROM generate_series(1, 10000000) id;

-- Confirm rows landed only in children, not the empty parent
SELECT 'sales_info (ONLY, parent)' AS table_name, COUNT(*) FROM ONLY labs.sales_info
UNION ALL
SELECT 'sales_info_2022', COUNT(*) FROM labs.sales_info_2022
UNION ALL
SELECT 'sales_info_2023', COUNT(*) FROM labs.sales_info_2023
UNION ALL
SELECT 'sales_info_2024', COUNT(*) FROM labs.sales_info_2024
UNION ALL
SELECT 'sales_info_2025', COUNT(*) FROM labs.sales_info_2025
UNION ALL
SELECT 'sales_info_2026', COUNT(*) FROM labs.sales_info_2026
UNION ALL
SELECT 'sales_info (all children)', COUNT(*) FROM labs.sales_info;

-- results shown in screenshot: task1_1.png

/*
OBSERVATIONS:
- the parent table (ONLY sales_info) has 0 rows, which is exactly what we want.
  the trigger returns NULL for every row, so nothing gets stored in the parent itself.
- querying sales_info without ONLY automatically unions all children, so you get
  the full 10M count there.
- the data landed in only 2 partitions: sales_info_2023 (4,999,002 rows) and
  sales_info_2026 (5,000,998 rows). the other 3 partitions got 0 rows because
  the random date generation with (NOW() - up to 1000 days) from the current run
  date only produced dates falling in 2023 and 2026 ranges.
- this confirms the trigger correctly routed rows by eventdate into the matching
  yearly partition.
*/

-- Step 5: update some rows and move them to a different partition.
-- important: with inheritance-based partitioning, a plain UPDATE does NOT
-- re-trigger the routing function and does NOT physically move the row to a
-- different child. the CHECK constraints only apply on INSERT, not UPDATE.
UPDATE labs.sales_info_2022
SET eventdate = '2023-06-15'
WHERE id BETWEEN 1 AND 1000;

-- these rows now sit in sales_info_2022 with a 2023 date, violating what the
-- CHECK constraint is supposed to guarantee. this is a known limitation of
-- inheritance partitioning - it can't enforce cross-partition integrity on UPDATE.
-- declarative partitioning (task 1.2) fixes this by automatically moving the row.
SELECT COUNT(*) AS "rows_with_2022_table_but_2023_date"
FROM labs.sales_info_2022
WHERE eventdate >= '2023-01-01';

-- results shown in screenshot: task1_2.png


-- Step 6: plain (non-partitioned) comparison table with identical structure
DROP TABLE IF EXISTS labs.sales_info_simple;

CREATE TABLE labs.sales_info_simple (
    id INTEGER,
    category VARCHAR(1),
    ischeck BOOLEAN,
    eventdate DATE
);

INSERT INTO labs.sales_info_simple (id, category, ischeck, eventdate)
SELECT id, category, ischeck, eventdate
FROM labs.sales_info;

CREATE INDEX idx_sales_info_simple_eventdate ON labs.sales_info_simple (eventdate);

SELECT COUNT(*) FROM labs.sales_info_simple;
-- results shown in screenshot: task1_3.png
-- result: count 10000000

-- Compare plans: partitioned (sales_info) vs non-partitioned (sales_info_simple)

-- (a) SELECT ALL
EXPLAIN ANALYZE SELECT * FROM labs.sales_info;
-- results shown in screenshot: task1_4.png
EXPLAIN ANALYZE SELECT * FROM labs.sales_info_simple;
-- results shown in screenshot: task1_5.png

/*
OBSERVATIONS:
- sales_info: Append over 6 Seq Scans (one per child + the empty parent).
  execution time ~2959ms. only 2023 and 2026 children actually do real work
  (the others finish instantly because they're empty). total buffers read: 54055.
- sales_info_simple: single Seq Scan, execution time ~1593ms.
- the partitioned table was actually SLOWER here for SELECT ALL. that makes sense
  because you can't prune anything when you're selecting everything - all partitions
  must be scanned, and the Append overhead adds up. the simple table wins for
  full scans.
*/

-- (b) SELECT with a range of dates
EXPLAIN ANALYZE
SELECT * FROM labs.sales_info
WHERE eventdate BETWEEN '2024-01-01' AND '2024-06-30';
-- results shown in screenshot: task1_6.png
EXPLAIN ANALYZE
SELECT * FROM labs.sales_info_simple
WHERE eventdate BETWEEN '2024-01-01' AND '2024-06-30';
-- results shown in screenshot: task1_7.png

/*
OBSERVATIONS:
- sales_info: Append plan touched only the parent (empty) + sales_info_2024.
  constraint exclusion kicked in and skipped 2022, 2023, 2025, 2026 entirely.
  sales_info_2024 used a Bitmap Index Scan on its eventdate index.
  since 2024 was empty in our data, execution time was 0.064ms - almost instant.
- sales_info_simple: used an Index Scan on idx_sales_info_simple_eventdate,
  took 0.102ms. also very fast but that's thanks to the index, not pruning.
- the key difference: partitioning pruned the problem down to one child before
  even touching the data. without partitioning, the planner used the index to
  skip rows, which still works but is a different mechanism entirely.
*/

-- (c) SELECT exact date
EXPLAIN ANALYZE
SELECT * FROM labs.sales_info
WHERE eventdate = '2024-03-15';
-- results shown in screenshot: task1_8.png
EXPLAIN ANALYZE
SELECT * FROM labs.sales_info_simple
WHERE eventdate = '2024-03-15';
-- results shown in screenshot: task1_9.png

/*
OBSERVATIONS:
- sales_info: same pattern - Append with constraint exclusion, only the parent
  and sales_info_2024 were touched. Bitmap Index Scan on the child's index.
  0.064ms execution time (2024 partition is empty so it resolved instantly).
- sales_info_simple: Index Scan, 0.020ms. actually faster here because it's
  a single precise lookup vs the Append overhead with an empty child scan.
- for exact date lookups on large datasets with real data in the target partition,
  partitioning would win more clearly since constraint exclusion narrows things
  to just one child before the index scan happens.
*/

-- (d) COUNT of all rows
EXPLAIN ANALYZE SELECT COUNT(*) FROM labs.sales_info;
-- results shown in screenshot: task1_10.png
EXPLAIN ANALYZE SELECT COUNT(*) FROM labs.sales_info_simple;
-- results shown in screenshot: task1_11.png

/*
OBSERVATIONS:
- sales_info: Finalize Aggregate -> Gather (2 workers) -> Partial Aggregate
  -> Parallel Append over all children. execution time 782ms.
  the parallel workers each handled a subset of partitions - partition-wise
  parallel aggregation. workers were launched for 2023 and 2026 (the non-empty ones).
- sales_info_simple: Finalize Aggregate -> Gather -> Parallel Seq Scan,
  execution time 350ms. faster here because it's one continuous block of memory
  vs the Append overhead of scanning multiple children and combining.
- for COUNT(*) of everything, the simple table wins again. partitioning helps
  when you can prune, not when you must touch all the data.
*/

-- (e) COUNT with a range of dates
EXPLAIN ANALYZE
SELECT COUNT(*) FROM labs.sales_info
WHERE eventdate BETWEEN '2024-01-01' AND '2024-06-30';
-- results shown in screenshot: task1_12.png
EXPLAIN ANALYZE
SELECT COUNT(*) FROM labs.sales_info_simple
WHERE eventdate BETWEEN '2024-01-01' AND '2024-06-30';
-- results shown in screenshot: task1_13.png

/*
OBSERVATIONS:
- sales_info: Aggregate -> Append, only touched parent + sales_info_2024 via
  Bitmap Index Scan. execution time 0.046ms. the date range pruned everything
  except the one relevant child, making this basically free.
- sales_info_simple: Aggregate -> Index Only Scan using the eventdate index,
  Heap Fetches: 0 (visibility map was current). execution time 0.044ms.
  practically tied here because the index on the simple table is also very efficient
  for this kind of query.
- both are fast for range counts, but through different paths: partitioning via
  structural pruning, simple table via a covering index.
*/

SHOW constraint_exclusion;
-- results shown in screenshot: task1_14.png
-- result: partition
-- this means constraint exclusion only kicks in for queries that look like they're
-- working with inheritance hierarchies, which is the right default setting.


-- Step 7: drop the oldest partition; add a new general partition
ALTER TABLE labs.sales_info_2022 NO INHERIT labs.sales_info;
DROP TABLE labs.sales_info_2022;

DROP TABLE IF EXISTS labs.sales_info_3000;

CREATE TABLE labs.sales_info_3000 (
    CHECK (eventdate >= '3000-01-01' AND eventdate < '3001-01-01')
) INHERITS (labs.sales_info);

CREATE INDEX idx_sales_info_3000_eventdate ON labs.sales_info_3000 (eventdate);

-- update the routing function to handle the new partition range
CREATE OR REPLACE FUNCTION labs.partition_sales_info() RETURNS TRIGGER
AS $$
BEGIN
    IF (NEW.eventdate >= '3000-01-01'::DATE AND
        NEW.eventdate <  '3001-01-01'::DATE) THEN
        INSERT INTO labs.sales_info_3000 VALUES (NEW.*);
    ELSIF (NEW.eventdate >= '2026-01-01'::DATE AND
           NEW.eventdate <  '2027-01-01'::DATE) THEN
        INSERT INTO labs.sales_info_2026 VALUES (NEW.*);
    ELSIF (NEW.eventdate >= '2025-01-01'::DATE AND
           NEW.eventdate <  '2026-01-01'::DATE) THEN
        INSERT INTO labs.sales_info_2025 VALUES (NEW.*);
    ELSIF (NEW.eventdate >= '2024-01-01'::DATE AND
           NEW.eventdate <  '2025-01-01'::DATE) THEN
        INSERT INTO labs.sales_info_2024 VALUES (NEW.*);
    ELSIF (NEW.eventdate >= '2023-01-01'::DATE AND
           NEW.eventdate <  '2024-01-01'::DATE) THEN
        INSERT INTO labs.sales_info_2023 VALUES (NEW.*);
    ELSE
        RAISE EXCEPTION 'Out of range';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Verify the oldest partition is gone and the new one is attached
SELECT c.relname AS child_table
FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid
JOIN pg_class p ON p.oid = i.inhparent
WHERE p.relname = 'sales_info'
ORDER BY child_table;
-- results shown in screenshot: task1_15.png

/*
OBSERVATIONS:
- result shows: sales_info_2023, sales_info_2024, sales_info_2025,
  sales_info_2026, sales_info_3000.
- sales_info_2022 is gone. dropping a partition this way is instant
  regardless of how many rows it has, compared to a DELETE which would
  have to remove each row one by one and then require a VACUUM pass.
- sales_info_3000 was added and immediately available for queries on the
  parent - no data movement in existing partitions needed at all.
- this is the main operational benefit of partitioned tables: you manage
  data in bulk by partition-level DDL instead of row-level DML.
*/


-----------------------------------------------------------------------------
-- TASK 1.2: USE DECLARATIVE PARTITIONING (range by year, sub-partitioned
-- by list on category, with a DEFAULT list partition per year)
-----------------------------------------------------------------------------

DROP TABLE IF EXISTS labs.sales_info_dp CASCADE;

CREATE TABLE labs.sales_info_dp (
    id INTEGER,
    category VARCHAR(1),
    ischeck BOOLEAN,
    eventdate DATE
) PARTITION BY RANGE (eventdate);

-- 5 yearly range partitions, each itself PARTITION BY LIST(category)
CREATE TABLE labs.sales_info_dp_2022
    PARTITION OF labs.sales_info_dp
    FOR VALUES FROM ('2022-01-01') TO ('2023-01-01')
    PARTITION BY LIST (category);

CREATE TABLE labs.sales_info_dp_2023
    PARTITION OF labs.sales_info_dp
    FOR VALUES FROM ('2023-01-01') TO ('2024-01-01')
    PARTITION BY LIST (category);

CREATE TABLE labs.sales_info_dp_2024
    PARTITION OF labs.sales_info_dp
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')
    PARTITION BY LIST (category);

CREATE TABLE labs.sales_info_dp_2025
    PARTITION OF labs.sales_info_dp
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')
    PARTITION BY LIST (category);

CREATE TABLE labs.sales_info_dp_2026
    PARTITION OF labs.sales_info_dp
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')
    PARTITION BY LIST (category);

-- each year gets 2 named list partitions + 1 DEFAULT catch-all.
-- A-E in one bucket, F-K in another, anything else goes to default.
DO $$
DECLARE
    yr TEXT;
BEGIN
    FOREACH yr IN ARRAY ARRAY['2022','2023','2024','2025','2026'] LOOP
        EXECUTE format($f$
            CREATE TABLE labs.sales_info_dp_%1$s_ae
                PARTITION OF labs.sales_info_dp_%1$s
                FOR VALUES IN ('A','B','C','D','E');
            CREATE TABLE labs.sales_info_dp_%1$s_fk
                PARTITION OF labs.sales_info_dp_%1$s
                FOR VALUES IN ('F','G','H','I','J','K');
            CREATE TABLE labs.sales_info_dp_%1$s_def
                PARTITION OF labs.sales_info_dp_%1$s
                DEFAULT;
        $f$, yr);
    END LOOP;
END;
$$;

-- Step 3: load the same 10M rows. no trigger needed here - declarative
-- partitioning routes rows internally in the executor, which is why this
-- took only ~50s vs ~5m 50s for the inheritance approach above.
INSERT INTO labs.sales_info_dp (id, category, ischeck, eventdate)
SELECT id,
       ('{"A","B","C","D","E","F","J","H","I","J","K"}'::TEXT[])[((RANDOM())*10)::INTEGER] AS category,
       ((1*(RANDOM())::INTEGER) < 1) AS ischeck,
       (NOW() - '10 day'::INTERVAL * (RANDOM()::INT * 100))::DATE AS eventdate
FROM generate_series(1, 10000000) id;

SELECT COUNT(*) FROM labs.sales_info_dp;
-- result: count 10,000,000

-- Step 4: update rows to a different category.
-- this is where declarative partitioning differs from inheritance: updating
-- a row's partition key (category) automatically moves it to the correct
-- list sub-partition. no manual intervention, no "drifted" rows like in task 1.1.
UPDATE labs.sales_info_dp
SET category = 'K'
WHERE category = 'A' AND
	  eventdate >= '2024-01-01' AND 
	  eventdate < '2024-02-01';

-- those rows should now be in the F-K list partition for 2024
SELECT COUNT(*) 
FROM labs.sales_info_dp_2024_fk 
WHERE category = 'K';
-- result: count 0 (2024 partition was empty due to our data distribution)

/*
OBSERVATIONS:
- unlike task 1.1 where updating eventdate left rows in the wrong child,
  declarative partitioning correctly detected that category='K' belongs
  in the _fk partition and moved the rows there automatically.
- this is the row-movement guarantee - an UPDATE on the partition key is
  internally turned into a delete from the old partition + insert into the
  new one. much safer for data integrity than inheritance-based partitioning.
*/

-- Step 5: compare plans, declarative (sales_info_dp) vs plain (sales_info_simple)

-- (a) SELECT ALL
EXPLAIN ANALYZE SELECT * FROM labs.sales_info_dp;
-- results shown in screenshot: task1_16.png
EXPLAIN ANALYZE SELECT * FROM labs.sales_info_simple;
-- results shown in screenshot: task1_17.png

/*
OBSERVATIONS:
- sales_info_dp: Append over all 15 leaf partitions (3 list subs per year x 5 years).
  execution time ~2973ms. only 2023 and 2026 sub-partitions had real data, others
  finished instantly. total: sales_info_dp_2023_ae (2.5M), _fk (2.25M), _def (250k),
  and same pattern for 2026.
- sales_info_simple: single Seq Scan, ~1430ms.
- SELECT ALL is slower on the partitioned table because you're paying the Append
  overhead for 15 partitions instead of scanning one continuous table. no pruning
  benefit when you need everything.
*/

-- (b) SELECT with range of dates
EXPLAIN ANALYZE
SELECT * FROM labs.sales_info_dp
WHERE eventdate BETWEEN '2024-01-01' AND '2024-06-30';
-- results shown in screenshot: task1_18.png
EXPLAIN ANALYZE
SELECT * FROM labs.sales_info_simple
WHERE eventdate BETWEEN '2024-01-01' AND '2024-06-30';
-- results shown in screenshot: task1_19.png

/*
OBSERVATIONS:
- sales_info_dp: pruned down to only the 3 sub-partitions of 2024 (ae, fk, def).
  all empty in our data so execution was 0.047ms. the plan shows a clean Append
  with just 3 Seq Scans, pruning eliminated all other years.
- sales_info_simple: Index Scan on eventdate index, 0.487ms. also fast but it's
  slower here vs declarative pruning, because the index still has to be traversed
  and the heap potentially checked, while the declarative approach dismissed the
  non-2024 partitions at planning time before touching anything.
*/

-- (c) SELECT exact date
EXPLAIN ANALYZE
SELECT * FROM labs.sales_info_dp
WHERE eventdate = '2024-03-15';
-- results shown in screenshot: task1_20.png
EXPLAIN ANALYZE
SELECT * FROM labs.sales_info_simple
WHERE eventdate = '2024-03-15';
-- results shown in screenshot: task1_21.png

/*
OBSERVATIONS:
- sales_info_dp: Append with only the 3 x 2024 sub-partitions (each doing a Seq Scan
  with Filter). execution 0.051ms.
- sales_info_simple: Index Scan, 0.025ms. slightly faster because it's a single
  index lookup vs the Append node overhead for 3 partitions.
- for exact date with real data in the partition, they'd be comparable, but the
  declarative approach still wins at scale because it structurally excludes
  everything outside the matching year before even looking at the data.
*/

-- (d) SELECT exact category
EXPLAIN ANALYZE
SELECT * FROM labs.sales_info_dp
WHERE category = 'C';
-- results shown in screenshot: task1_22.png
EXPLAIN ANALYZE
SELECT * FROM labs.sales_info_simple
WHERE category = 'C';
-- results shown in screenshot: task1_23.png

/*
OBSERVATIONS:
- sales_info_dp: pruned to only the _ae partitions (A-E lists) across all years.
  execution 874ms. touched: 2022_ae (empty), 2023_ae (2.5M rows, returned 500k),
  2024_ae (empty), 2025_ae (empty), 2026_ae (2.5M rows, returned 500k).
  so 5 Seq Scans instead of 15 - the list partition pruning skipped all _fk and
  _def sub-partitions entirely.
- sales_info_simple: full Seq Scan over 10M rows, filtering in 8M non-C rows.
  execution 1074ms.
- declarative partitioning was faster here because list pruning skipped 10 of 15
  partitions. even though no index on category exists, the partition bounds alone
  were enough to cut the search space significantly.
*/

-- (e) SELECT a list of categories
EXPLAIN ANALYZE
SELECT * FROM labs.sales_info_dp
WHERE category IN ('A','B','C');
-- results shown in screenshot: task1_24.png
EXPLAIN ANALYZE
SELECT * FROM labs.sales_info_simple
WHERE category IN ('A','B','C');
-- results shown in screenshot: task1_25.png

/*
OBSERVATIONS:
- sales_info_dp: Append over only the 5 _ae partitions (one per year), same
  pruning logic as (d) because A, B, C all fall in the A-E list bucket.
  execution 1764ms. the _fk and _def partitions were all excluded at plan time.
  touched 2023_ae and 2026_ae with real rows (filtered ~1M each returned ~1.5M total).
- sales_info_simple: full Seq Scan, 2260ms. filters out 7M non-A/B/C rows.
- partitioning clearly faster here. even with large result sets, skipping 2/3 of
  the leaf partitions via list pruning beats scanning and filtering the whole table.
*/

-- (f) SELECT a list of categories in exact date
EXPLAIN ANALYZE
SELECT * FROM labs.sales_info_dp
WHERE category IN ('A','B','C') AND eventdate = '2024-03-15';
-- results shown in screenshot: task1_26.png
EXPLAIN ANALYZE
SELECT * FROM labs.sales_info_simple
WHERE category IN ('A','B','C') AND eventdate = '2024-03-15';
-- results shown in screenshot: task1_27.png

/*
OBSERVATIONS:
- sales_info_dp: best plan of the whole set. both range (eventdate) and list
  (category) pruning applied simultaneously. result: single Seq Scan on only
  sales_info_dp_2024_ae. execution 0.018ms - essentially free.
- sales_info_simple: Index Scan on eventdate, 0.027ms. also fast but relies on
  the index, while the declarative approach did it through pure structural pruning
  with no index needed at all.
- this is the ideal use case for composite partitioning: when your queries filter
  on both the range key and the list key, the planner can prune down to a single
  leaf partition out of 15. that's the whole point of the sub-partitioning design.
*/

-- (g) COUNT of all rows
EXPLAIN ANALYZE SELECT COUNT(*) FROM labs.sales_info_dp;
-- results shown in screenshot: task1_28.png
EXPLAIN ANALYZE SELECT COUNT(*) FROM labs.sales_info_simple;
-- results shown in screenshot: task1_29.png

/*
OBSERVATIONS:
- sales_info_dp: Finalize Aggregate -> Gather (2 workers) -> Partial Aggregate
  -> Parallel Append over all 15 leaf partitions. execution 727ms.
  parallel workers handled the non-empty partitions (2023 and 2026 sub-groups).
- sales_info_simple: Finalize Aggregate -> Gather -> Parallel Seq Scan, 330ms.
- for COUNT(*) of everything, simple table wins again (~2x faster). same reason
  as task 1.1 observations - the Append overhead and coordination between workers
  across multiple partitions costs more than just scanning one big table in parallel.
*/

-- (h) COUNT with a range of dates
EXPLAIN ANALYZE
SELECT COUNT(*) FROM labs.sales_info_dp
WHERE eventdate BETWEEN '2024-01-01' AND '2024-06-30';
-- results shown in screenshot: task1_30.png
EXPLAIN ANALYZE
SELECT COUNT(*) FROM labs.sales_info_simple
WHERE eventdate BETWEEN '2024-01-01' AND '2024-06-30';
-- results shown in screenshot: task1_31.png

/*
OBSERVATIONS:
- sales_info_dp: Aggregate -> Append, only the 3 x 2024 sub-partitions scanned.
  0.032ms - effectively instant because 2024 was empty.
- sales_info_simple: Aggregate -> Index Only Scan, Heap Fetches: 0. 0.044ms.
  the visibility map was up to date so it could use Index Only Scan with zero
  heap access.
- with real data in 2024, declarative partitioning would win more clearly here
  because it prunes structurally before any scan, while the simple table's
  Index Only Scan still has to traverse the index even if it never touches the heap.
*/

SHOW enable_partition_pruning;
-- result: on
-- pruning is enabled by default, which is what drove all the plan differences above.


-- Step 6: split one list partition into two, then restore original.
-- splitting sales_info_dp_2024's A-E partition into A-C and D-E.
-- there's no native SPLIT PARTITION command in postgres, so the process is:
-- detach, create two new ones, migrate data, drop the old one.

BEGIN;

ALTER TABLE labs.sales_info_dp_2024 DETACH PARTITION labs.sales_info_dp_2024_ae;

CREATE TABLE labs.sales_info_dp_2024_ac
    PARTITION OF labs.sales_info_dp_2024
    FOR VALUES IN ('A','B','C');

CREATE TABLE labs.sales_info_dp_2024_de
    PARTITION OF labs.sales_info_dp_2024
    FOR VALUES IN ('D','E');

INSERT INTO labs.sales_info_dp_2024_ac
SELECT * 
FROM labs.sales_info_dp_2024_ae 
WHERE category IN ('A','B','C');

INSERT INTO labs.sales_info_dp_2024_de
SELECT * 
FROM labs.sales_info_dp_2024_ae 
WHERE category IN ('D','E');

DROP TABLE labs.sales_info_dp_2024_ae;

COMMIT;

-- Confirm the split
SELECT c.relname AS partition_2024_children
FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid
JOIN pg_class p ON p.oid = i.inhparent
WHERE p.relname = 'sales_info_dp_2024'
ORDER BY partition_2024_children;
-- results shown in screenshot: task1_32.png



-- Now restore the original A-E partition.
-- important: we need to detach the two split partitions FIRST in the same
-- transaction, then create the merged one - otherwise postgres will complain
-- about overlapping partition bounds.

BEGIN;

ALTER TABLE labs.sales_info_dp_2024 DETACH PARTITION labs.sales_info_dp_2024_ac;
ALTER TABLE labs.sales_info_dp_2024 DETACH PARTITION labs.sales_info_dp_2024_de;

CREATE TABLE labs.sales_info_dp_2024_ae
    PARTITION OF labs.sales_info_dp_2024
    FOR VALUES IN ('A','B','C','D','E');

INSERT INTO labs.sales_info_dp_2024_ae
SELECT * FROM labs.sales_info_dp_2024_ac;

INSERT INTO labs.sales_info_dp_2024_ae
SELECT * FROM labs.sales_info_dp_2024_de;

DROP TABLE labs.sales_info_dp_2024_ac;
DROP TABLE labs.sales_info_dp_2024_de;

COMMIT;

-- Confirm restore
SELECT c.relname AS partition_2024_children_restored
FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid
JOIN pg_class p ON p.oid = i.inhparent
WHERE p.relname = 'sales_info_dp_2024'
ORDER BY partition_2024_children_restored;
-- results shown in screenshot: task1_33.png

/*
OBSERVATIONS:
- splitting/merging list partitions requires DETACH + CREATE + data migration
  because there's no built-in SPLIT PARTITION command in PostgreSQL.
- the key thing i learned here: you must detach the overlapping partitions BEFORE
  creating the merged one. if you try to create A-E while A-C and D-E are still
  attached, postgres throws an error about overlapping partition bounds. doing it
  all in one transaction guarantees consistency.
- DETACH PARTITION takes a SHARE UPDATE EXCLUSIVE lock, which is less disruptive
  than the ACCESS EXCLUSIVE lock that DROP TABLE or the inheritance approach needed.
  this matters in production where you don't want to block reads for a long time.
*/


-----------------------------------------------------------------------------
-- TASK 2 (PART 1): PARALLEL EXECUTION
-----------------------------------------------------------------------------

-- Step 1: allow up to 4 parallel workers per Gather node for this session
SET max_parallel_workers_per_gather = 4;

-- Step 2: analyze plans for sales_info, sales_info_dp, sales_info_simple
ANALYZE labs.sales_info;
ANALYZE labs.sales_info_dp;
ANALYZE labs.sales_info_simple;

-- (a) SELECT all from each table
EXPLAIN ANALYZE SELECT * FROM labs.sales_info;
-- results shown in screenshot: task2_1.png
EXPLAIN ANALYZE SELECT * FROM labs.sales_info_dp;
-- results shown in screenshot: task2_2.png
EXPLAIN ANALYZE SELECT * FROM labs.sales_info_simple;
-- results shown in screenshot: task2_3.png

/*
OBSERVATIONS:
- sales_info: plain Append over 6 Seq Scans (no parallelism here), execution ~2798ms.
  even with max_parallel_workers_per_gather=4, the planner chose not to parallelize
  this one - the partitions are already split across children so it just sequentially
  appends them. only 2023 and 2026 had real data.
- sales_info_dp: same story - plain Append over all 15 leaf partitions, ~2833ms.
  no parallel workers used here either. the planner decided the Append structure
  was already doing enough work division without adding Gather overhead.
- sales_info_simple: plain Seq Scan, ~1711ms. fastest of the three for SELECT ALL,
  same as we saw before - no partition overhead, one continuous block read.
- key takeaway: parallel execution doesn't always kick in automatically for SELECT *.
  the planner weighs the cost of spinning up workers vs just scanning, and for
  these relatively cached tables it preferred sequential plans.
*/

-- (b) same, with ORDER BY eventdate
EXPLAIN ANALYZE SELECT * FROM labs.sales_info ORDER BY eventdate;
-- results shown in screenshot: task2_4.png
EXPLAIN ANALYZE SELECT * FROM labs.sales_info_dp ORDER BY eventdate;
-- results shown in screenshot: task2_5.png
EXPLAIN ANALYZE SELECT * FROM labs.sales_info_simple ORDER BY eventdate;
-- results shown in screenshot: task2_6.png

/*
OBSERVATIONS:
- sales_info: Merge Append instead of plain Append - it used the per-child eventdate
  indexes (Index Scan on each child) and merged the already-sorted streams together.
  no Sort node needed above because each child's index delivers rows in order.
  execution ~5586ms. sounds slow but it's doing an ordered merge of 10M rows.
- sales_info_dp: no indexes on eventdate exist for the declarative partitions (we only
  created a category index later), so the planner did Append + external merge Sort.
  the entire 10M rows had to be collected and then sorted to disk (external merge,
  215336kB temp). execution ~9821ms - the worst performer here.
- sales_info_simple: Gather Merge with 4 workers, each sorting their 2M-row chunk via
  external merge to disk (~43-44MB each), leader merges the sorted streams.
  execution ~10074ms. also slow because it has to sort 10M rows across workers.
- the big difference: sales_info benefited from having per-child indexes on eventdate,
  so Merge Append could use them to produce sorted output without a Sort node at all.
  sales_info_dp had no eventdate index at the partition level, so it fell back to
  sorting the whole result after the Append, which is much more expensive.
*/

-- (c) COUNT of all rows
EXPLAIN ANALYZE SELECT COUNT(*) FROM labs.sales_info;
-- results shown in screenshot: task2_7.png
EXPLAIN ANALYZE SELECT COUNT(*) FROM labs.sales_info_dp;
-- results shown in screenshot: task2_8.png
EXPLAIN ANALYZE SELECT COUNT(*) FROM labs.sales_info_simple;
-- results shown in screenshot: task2_9.png

/*
OBSERVATIONS:
- sales_info: Finalize Aggregate -> Gather (3 workers) -> Partial Aggregate ->
  Parallel Append. workers handle subsets of the non-empty partitions in parallel.
  execution ~680ms.
- sales_info_dp: same pattern, Gather (3 workers) -> Parallel Append across all 15
  leaf partitions. empty ones finish instantly, workers focus on 2023 and 2026 subs.
  execution ~619ms - slightly faster than sales_info, probably because there were
  more leaf partitions for the workers to divide among themselves more evenly.
- sales_info_simple: Gather (4 workers) -> Parallel Seq Scan. 4 workers launched
  (vs 3 for the partitioned tables), each scanning 2M rows. execution ~753ms.
- this time the partitioned tables were actually faster than the simple table for
  COUNT(*), even though all rows need to be counted. the parallel workers being able
  to divide work by partition (Parallel Append) was more efficient than 4 workers
  all racing through the same continuous table.
*/

-- (d) add a range of dates
EXPLAIN ANALYZE
SELECT COUNT(*) FROM labs.sales_info
WHERE eventdate BETWEEN '2024-01-01' AND '2024-06-30';
-- results shown in screenshot: task2_10.png
EXPLAIN ANALYZE
SELECT COUNT(*) FROM labs.sales_info_dp
WHERE eventdate BETWEEN '2024-01-01' AND '2024-06-30';
-- results shown in screenshot: task2_11.png
EXPLAIN ANALYZE
SELECT COUNT(*) FROM labs.sales_info_simple
WHERE eventdate BETWEEN '2024-01-01' AND '2024-06-30';
-- results shown in screenshot: task2_12.png

/*
OBSERVATIONS:
- sales_info: Aggregate -> Append, touched only parent + sales_info_2024 (empty),
  execution 0.035ms. constraint exclusion pruned all other years. no workers needed
  because the result set was empty and trivial to compute.
- sales_info_dp: Aggregate -> Append, touched only the 3 x 2024 sub-partitions,
  all empty. execution 0.129ms. same structural pruning as sales_info.
- sales_info_simple: Gather (4 workers) -> Parallel Seq Scan, each worker scanning
  2M rows and filtering all of them out (Rows Removed by Filter: 2000000, result 0).
  execution 287ms. with no partition structure and no rows actually matching 2024,
  it still had to scan and discard the entire 10M rows across 4 workers.
- this is the clearest demonstration of why pruning matters: both partitioned tables
  resolved the query in under 0.2ms while the simple table spent 287ms on parallel
  work that found nothing. at scale with real 2024 data the partitioned win would
  be proportionally the same.
*/

-- (e) add grouping by category
EXPLAIN ANALYZE
SELECT category, COUNT(*) FROM labs.sales_info
WHERE eventdate BETWEEN '2024-01-01' AND '2024-06-30'
GROUP BY category
ORDER BY category;
-- results shown in screenshot: task2_13.png
EXPLAIN ANALYZE
SELECT category, COUNT(*) FROM labs.sales_info_dp
WHERE eventdate BETWEEN '2024-01-01' AND '2024-06-30'
GROUP BY category
ORDER BY category;
-- results shown in screenshot: task2_14.png
EXPLAIN ANALYZE
SELECT category, COUNT(*) FROM labs.sales_info_simple
WHERE eventdate BETWEEN '2024-01-01' AND '2024-06-30'
GROUP BY category
ORDER BY category;
-- results shown in screenshot: task2_15.png

/*
OBSERVATIONS:
- sales_info: GroupAggregate -> Sort -> Append, only parent + sales_info_2024
  touched. 0.338ms. no parallel workers - the pruned result was empty so there
  was nothing to parallelize. used GroupAggregate (sorted aggregation) because
  it expected very few rows after pruning.
- sales_info_dp: Sort -> HashAggregate -> Append, only the 3 x 2024 sub-partitions
  touched. 0.069ms. same empty result, no workers needed. the planner chose
  HashAggregate here (in-memory hash table) vs GroupAggregate for sales_info -
  different choices for the same logical query depending on partition structure.
- sales_info_simple: GroupAggregate -> Sort -> Gather (4 workers) -> Parallel Seq Scan,
  execution 348ms. workers each scanned 2M rows, removed all of them (Rows Removed by
  Filter: 2000000), found nothing, the GroupAggregate at the top got an empty input.
  same story as (d) - 4 workers doing full scans to return zero rows.
- again partitioning wins heavily because it avoided the scan entirely. the planner
  chose different aggregation strategies (GroupAggregate vs HashAggregate) based on
  the expected row count after pruning, which is also interesting to note.
*/

-- (f) join sales_info and sales_info_dp on id, count rows on exact date
EXPLAIN ANALYZE
SELECT COUNT(*)
FROM labs.sales_info si
JOIN labs.sales_info_dp sidp ON si.id = sidp.id
WHERE si.eventdate = '2024-03-15' AND 
	  sidp.eventdate = '2024-03-15';
-- results shown in screenshot: task2_16.png

/*
OBSERVATIONS:
- both sides were pruned before the join: sales_info pruned to parent + sales_info_2024,
  sales_info_dp pruned to the 3 x 2024 sub-partitions. the planner built the hash
  from the sales_info side (2 empty partitions) first, then probed with the
  sales_info_dp Append (3 empty partitions) - Hash Join with hash cond on id.
- because 2024 was empty in our data, the hash table was never actually built
  (the Hash node shows "never executed") and the whole thing resolved in 0.053ms.
- even though no parallelism was used here (no workers launched), the structural
  pruning on both sides is what made it instant. with real 2024 data across 10M
  rows on each side, you'd see a proper Hash Join plan, likely with parallel workers.
*/


-----------------------------------------------------------------------------
-- TASK 2 (PART 2): ADD INDEXES ON A PARTITIONED TABLE, RECHECK PLANS
-----------------------------------------------------------------------------

-- creating an index on the top-level declarative parent automatically
-- propagates it to every existing leaf partition. this is one of the advantages
-- of declarative over inheritance partitioning - no need to create indexes
-- on each child manually.
CREATE INDEX idx_sales_info_dp_category
    ON labs.sales_info_dp (category);

-- Re-run the exact-category query to see the change
EXPLAIN ANALYZE
SELECT * FROM labs.sales_info_dp
WHERE category = 'C';
-- results shown in screenshot: task2_17.png

EXPLAIN ANALYZE
SELECT * FROM labs.sales_info_dp
WHERE category IN ('A','B','C') AND eventdate = '2024-03-15';
-- results shown in screenshot: task2_18.png

-- Re-run the grouped/parallel query as well
EXPLAIN ANALYZE
SELECT category, COUNT(*) FROM labs.sales_info_dp
WHERE eventdate BETWEEN '2024-01-01' AND '2024-06-30'
GROUP BY category
ORDER BY category;
-- results shown in screenshot: task2_19.png

-- Confirm the index exists on every leaf partition
SELECT schemaname, tablename, indexname
FROM pg_indexes
WHERE tablename LIKE 'sales_info_dp%' AND 
	  indexname LIKE 'idx_sales_info_dp_category%'
ORDER BY tablename;
-- results shown in screenshot: task2_20.png

/*
OBSERVATIONS:
- pg_indexes only shows ONE entry: sales_info_dp with idx_sales_info_dp_category.
  this is the logical parent index. even though the physical indexes exist on each
  leaf, they show up under their own partition table names (e.g. sales_info_dp_2023_ae),
  not as separate rows under "idx_sales_info_dp_category". the query filtered by
  indexname LIKE 'idx_sales_info_dp_category%' which is the parent index name, and
  the children get auto-generated names like sales_info_dp_2023_ae_category_idx.
  so the propagation did happen (as confirmed by the Bitmap Index Scans in the plans),
  the query just didn't surface the child index names.
- for category = 'C': the plan now uses Bitmap Index Scan on the _ae partitions
  (2023 and 2026) via their auto-generated category indexes. 2022/2024/2025 are
  empty so they still got Seq Scans (no rows to index-scan). execution dropped from
  874ms (before index) to 458ms. the index saved about half the time by avoiding
  scanning all rows in the _ae partition and filtering - instead it jumped straight
  to the ~500k matching rows per partition.
- for category IN ('A','B','C') + exact date = '2024-03-15': the plan is just a single
  Seq Scan on sales_info_dp_2024_ae (0.023ms). list + range pruning narrowed it to one
  empty partition - no index needed, the partition itself was the filter. this is the
  case where structural pruning is more powerful than any index.
- for the GROUP BY with date range: same plan as before the index - Sort -> HashAggregate
  -> Append over the 3 empty 2024 sub-partitions (0.068ms). the planner didn't use the
  category index here because the partition was empty and a Seq Scan was cheaper than
  an index lookup for a trivially small result. makes sense.
- the main point: indexes on top of partition pruning give an extra speedup specifically
  when the surviving partition(s) have real data and selectivity is high. when partitions
  are empty or nearly so, pruning alone is already fast enough and the index isn't used.
*/


-- SET max_parallel_workers_per_gather = DEFAULT;