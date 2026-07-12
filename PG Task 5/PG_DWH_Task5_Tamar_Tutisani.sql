-- POSTGRESQL TASK 5: JOIN METHODS
-- Tamar Tutisani
-- Module: PG_DWH
-----------------------------------------------------------------------------

-- SET search_path TO labs, public;
-----------------------------------------------------------------------------


---------------------------------------------------------------
-- TASK 1: NESTED LOOP JOIN
---------------------------------------------------------------

-- Step 1: Create and populate test tables
-----------------------------------------------------------------------------

DROP TABLE IF EXISTS labs.test_joins_a;
DROP TABLE IF EXISTS labs.test_joins_b;

CREATE TABLE labs.test_joins_a (
    id1 INT,
    id2 INT
);

CREATE TABLE labs.test_joins_b (
    id1 INT,
    id2 INT
);

INSERT INTO labs.test_joins_a
VALUES (generate_series(1, 10000), 3);

INSERT INTO labs.test_joins_b
VALUES (generate_series(1, 10000), 3);

ANALYZE labs.test_joins_a;
ANALYZE labs.test_joins_b;


-- Step 2: Check NESTED LOOP JOIN usage
-----------------------------------------------------------------------------

-- Query 1: inequality join condition (>)
EXPLAIN ANALYZE
SELECT *
FROM labs.test_joins_a a,
     labs.test_joins_b b
WHERE a.id1 > b.id1;
-- results shown in screenshot: task1_1a.png
/*
The plan shows a Nested Loop here. The join condition is an inequality (>),
which rules out both Hash Join (needs =) and Merge Join (also needs =).
Nested Loop is the only strategy that can handle non-equality conditions,
it goes through each outer row and scans the inner table to find all rows
where b.id1 < a.id1. The result set is very large (close to n^2/2 combinations
for 10,000 rows each), so execution takes a while.
*/


-- Query 2: CROSS JOIN (cartesian product)
EXPLAIN ANALYZE
SELECT *
FROM labs.test_joins_a a
CROSS JOIN labs.test_joins_b b;
-- results shown in screenshot: task1_1b.png
/*
Again a Nested Loop, which is the only way PostgreSQL can execute a CROSS JOIN.
There is no join condition at all, so neither Hash nor Merge Join can be applied.
The result is a full cartesian product: 10,000 * 10,000 = 100,000,000 rows.
*/


---------------------------------------------------------------
-- TASK 2: HASH JOIN
---------------------------------------------------------------

-- Step 1: Rewrite the query to equality so the planner can use Hash Join
-----------------------------------------------------------------------------

-- The original inequality (>) cannot use Hash Join. Changing to = gives
-- the planner what it needs:
EXPLAIN ANALYZE
SELECT *
FROM labs.test_joins_a a
INNER JOIN labs.test_joins_b b ON a.id1 = b.id1;
-- results shown in screenshot: task2_1.png
/*
With an equality condition and no index on id1, the planner chose Hash Join.
It builds a hash table from one side (inner), then probes it for each row
from the outer side. Both tables have 10,000 rows so the hash table fits
in memory and this is the cheapest option here.
*/

-- Step 2: Semi Join -> Hash Semi Join
-----------------------------------------------------------------------------

EXPLAIN ANALYZE
SELECT *
FROM labs.test_joins_a a
WHERE EXISTS (
    SELECT 1
    FROM labs.test_joins_b b
    WHERE b.id1 = a.id1
);
-- results shown in screenshot: task2_2.png
/*
EXISTS gets translated internally into a Semi Join. The plan shows
Hash Semi Join, it builds a hash table from b, then for each row in a
it checks whether a match exists, returning that row at most once even
if there are multiple matches. This is more efficient than a regular join
because it stops searching as soon as the first match is found.
*/


-- Step 3: Disable Hash Join and recheck plan
-----------------------------------------------------------------------------

SET enable_hashjoin = OFF;

EXPLAIN ANALYZE
SELECT *
FROM labs.test_joins_a a
INNER JOIN labs.test_joins_b b ON a.id1 = b.id1;
-- results shown in screenshot: task2_3.png
/*
With Hash Join disabled, the planner fell back to Merge Join with Sort nodes
on both sides. The equality condition still allows Merge Join, but now it has
to sort both tables first, which makes it more expensive than Hash Join was.
*/

-- Re-enable Hash Join
SET enable_hashjoin = ON;


---------------------------------------------------------------
-- TASK 3: MERGE JOIN
---------------------------------------------------------------

-- Disable Hash Join and Nested Loop to force the planner to use Merge Join
SET enable_hashjoin = OFF;
SET enable_nestloop = OFF;

EXPLAIN ANALYZE
SELECT *
FROM labs.test_joins_a a
INNER JOIN labs.test_joins_b b ON a.id1 = b.id1;
-- results shown in screenshot: task3_1.png
/*
With Hash Join and Nested Loop both disabled, the planner used Merge Join.
It added Sort nodes under both table scans to sort by id1, then merged the
two sorted lists in one pass. Merge Join needs an equality condition and
sorted input, both are satisfied here. It works well when both sides are
large and can be sorted, since it only reads each side once after sorting.
*/

-- Step 2: Disable Merge Join too and recheck
-----------------------------------------------------------------------------

SET enable_mergejoin = OFF;

EXPLAIN ANALYZE
SELECT *
FROM labs.test_joins_a a
INNER JOIN labs.test_joins_b b ON a.id1 = b.id1;
-- results shown in screenshot: task3_2.png
/*
With all three join methods disabled, PostgreSQL still produced a plan,
it does not error out, it just picks the least-bad option it has left.
In this case it fell back to Nested Loop even though all strategies are
technically "off", because it has no other choice and ignores the hint
rather than failing. The cost is noticeably higher.
*/

-- Re-enable all join strategies
SET enable_hashjoin = ON;
SET enable_mergejoin = ON;
SET enable_nestloop = ON;


---------------------------------------------------------------
-- TASK 4: CHANGING JOIN ORDER
---------------------------------------------------------------

-- Step 1: Create table test_joins_c
-----------------------------------------------------------------------------

DROP TABLE IF EXISTS labs.test_joins_c;

CREATE TABLE labs.test_joins_c (
    id1 INT,
    id2 INT
);

INSERT INTO labs.test_joins_c
VALUES (generate_series(1, 1000000), (random() * 10)::INT);

ANALYZE labs.test_joins_c;


-- Step 2: Check the default join order
-----------------------------------------------------------------------------

EXPLAIN
SELECT c.id2
FROM labs.test_joins_b b
JOIN labs.test_joins_a a ON b.id1 = a.id1
LEFT JOIN labs.test_joins_c c ON c.id1 = b.id1;
-- results shown in screenshot: task4_1.png
/*
With join_collapse_limit = 8 (default), the planner is free to reorder joins.
The plan shows it joined a and b first (both have 10,000 rows, small and equal),
then joined the result to c (1,000,000 rows) last. This makes sense because
joining the two smaller tables first reduces the intermediate result size before
touching the large table. The planner picked Hash Join for the first pair.
*/


-- Step 3: Set join_collapse_limit = 1 to lock the order as written in SQL
-----------------------------------------------------------------------------

SET join_collapse_limit = 1;

EXPLAIN
SELECT c.id2
FROM labs.test_joins_b b
JOIN labs.test_joins_a a ON b.id1 = a.id1
LEFT JOIN labs.test_joins_c c ON c.id1 = b.id1;
-- results shown in screenshot: task4_2.png
/*
With join_collapse_limit = 1, the planner must join tables in exactly the
order they appear in the SQL statement, no reordering is allowed.
The structure of the plan is the same here because the written order
(b JOIN a, then LEFT JOIN c) happens to match what the planner would have
chosen anyway. But if the written order was suboptimal, the planner would
be forced to follow it anyway and we could see a worse cost estimate.
This setting is useful when you want to control join order manually.
*/

-- Restore default
SET join_collapse_limit = 8;


---------------------------------------------------------------
-- TASK 5: LATERAL JOIN
---------------------------------------------------------------

-- Step 1: Create orders and stores tables
-----------------------------------------------------------------------------

DROP TABLE IF EXISTS labs.orders;
DROP TABLE IF EXISTS labs.stores;

CREATE TABLE labs.orders AS
SELECT id AS order_id,
       (id * 10 * random() * 10)::INT AS order_cost,
       'order number ' || id AS order_num
FROM generate_series(1, 1000) AS id;

CREATE TABLE labs.stores (
    store_id INT,
    store_name TEXT,
    max_order_cost INT
);

INSERT INTO labs.stores (store_id, store_name, max_order_cost)
VALUES
    (1, 'grocery shop', 800),
    (2, 'bakery', 100),
    (3, 'manufactured goods', 3000);

ANALYZE labs.orders;
ANALYZE labs.stores;


-- Step 2: TOP 10 orders per store using LATERAL JOIN
-----------------------------------------------------------------------------

SELECT s.store_id,
       s.store_name,
       s.max_order_cost,
       top_orders.order_id,
       top_orders.order_cost,
       top_orders.order_num
FROM labs.stores s
LEFT JOIN LATERAL (
    SELECT o.order_id,
           o.order_cost,
           o.order_num
    FROM labs.orders o
    WHERE o.order_cost < s.max_order_cost
    ORDER BY o.order_cost DESC
    LIMIT 10
) AS top_orders ON TRUE
ORDER BY s.store_id,
         top_orders.order_cost DESC;
-- results shown in screenshot: task5_1.png
/*
LATERAL makes the subquery run separately for each store row, which lets
it reference s.max_order_cost from the outer query, something a regular
subquery in FROM cannot do. For each store it filters orders below that
store's cost limit, sorts them by cost descending, and takes the top 10.
LEFT JOIN ON TRUE means stores with zero qualifying orders still appear
in the result rather than being dropped. The output shows up to 10 orders
per store, each with a different cost ceiling depending on the store.
*/

EXPLAIN ANALYZE
SELECT s.store_id,
       s.store_name,
       s.max_order_cost,
       top_orders.order_id,
       top_orders.order_cost,
       top_orders.order_num
FROM labs.stores s
LEFT JOIN LATERAL (
    SELECT o.order_id,
           o.order_cost,
           o.order_num
    FROM labs.orders o
    WHERE o.order_cost < s.max_order_cost
    ORDER BY o.order_cost DESC
    LIMIT 10
) AS top_orders ON TRUE
ORDER BY s.store_id,
         top_orders.order_cost DESC;
-- results shown in screenshot: task5_2.png
/*
The EXPLAIN plan confirms the LATERAL behavior: the inner subquery (Seq Scan
on orders with a Sort and Limit) appears as a nested loop driven by the outer
Seq Scan on stores. The planner runs the subquery once per store row, which
is exactly what we want, three stores = three executions of the subquery.
*/


---------------------------------------------------------------
-- TASK 6: RECURSIVE CTE
---------------------------------------------------------------

-- Recreate emp table if needed
CREATE TABLE IF NOT EXISTS labs.emp (
    empno NUMERIC(4) NOT NULL CONSTRAINT emp_pk PRIMARY KEY,
    ename VARCHAR(10) UNIQUE,
    job VARCHAR(9),
    mgr NUMERIC(4),
    hiredate DATE
);

INSERT INTO labs.emp (empno, ename, job, mgr, hiredate)
SELECT v.empno,
       v.ename,
       v.job,
       v.mgr,
       v.hiredate::DATE
FROM (
    VALUES
        (1, 'SMITH',  'CLERK', 13, '17-DEC-80'),
        (2, 'ALLEN', 'SALESMAN', 6, '20-FEB-81'),
        (3, 'WARD', 'SALESMAN', 6, '22-FEB-81'),
        (4, 'JONES', 'MANAGER', 9, '02-APR-81'),
        (5, 'MARTIN', 'SALESMAN', 6, '28-SEP-81'),
        (6, 'BLAKE', 'MANAGER', 9, '01-MAY-81'),
        (7, 'CLARK', 'MANAGER', 9, '09-JUN-81'),
        (8, 'SCOTT', 'ANALYST', 4, '19-APR-87'),
        (9, 'KING', 'PRESIDENT', NULL,'17-NOV-81'),
        (10, 'TURNER', 'SALESMAN', 6, '08-SEP-81'),
        (11, 'ADAMS', 'CLERK', 8, '23-MAY-87'),
        (12, 'JAMES', 'CLERK', 6, '03-DEC-81'),
        (13, 'FORD', 'ANALYST', 4, '03-DEC-81'),
        (14, 'MILLER', 'CLERK', 7, '23-JAN-82')
) AS v(empno, ename, job, mgr, hiredate)
WHERE NOT EXISTS (
    SELECT 1 
    FROM labs.emp e 
    WHERE e.empno = v.empno
);


WITH RECURSIVE emp_hierarchy AS (

    -- Non-recursive term: seed with KING (mgr IS NULL = top of hierarchy)
    SELECT e.empno,
           e.ename AS employee_name,
           e.job,
           e.mgr,
           CAST(NULL AS VARCHAR) AS manager_name,
           1 AS level
    FROM labs.emp e
    WHERE e.mgr IS NULL

    UNION ALL

    -- Recursive term: find direct reports of the previous level
    SELECT e.empno,
           e.ename AS employee_name,
           e.job,
           e.mgr,
           h.employee_name AS manager_name,
           h.level + 1 AS level
    FROM labs.emp e
    INNER JOIN emp_hierarchy h ON h.empno = e.mgr

)
SELECT level,
       REPEAT('  ', level - 1) || employee_name AS employee_name,
       job,
       COALESCE(manager_name, '— top of hierarchy —') AS manager_name
FROM emp_hierarchy
ORDER BY level, employee_name;
-- results shown in screenshot: task6_1.png

/*
The non-recursive term seeds the CTE with KING (empno=9, mgr IS NULL) at level 1.
Each iteration of the recursive term joins the emp table against the current
working table to find all employees who report to someone already in the result.
It carries the manager's name forward and increments the level each time.
The loop stops automatically when no more employees report to the current level.
REPEAT adds indentation so the hierarchy is easy to read visually.

The result shows:
  Level 1: KING
  Level 2: BLAKE, CLARK, JONES (report to KING)
  Level 3: ALLEN, JAMES, MARTIN, MILLER, TURNER, FORD, SCOTT
           (report to BLAKE, CLARK, or JONES depending on their mgr value)
  Level 4: ADAMS, SMITH (leaf employees)
*/


---------------------------------------------------------------
-- TASK 7: DATA-MODIFYING CTE
---------------------------------------------------------------

-- Step 1: Create order_log table
-----------------------------------------------------------------------------

DROP TABLE IF EXISTS labs.order_log;

CREATE TABLE labs.order_log (
    log_id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    order_id INTEGER,
    order_cost INTEGER,
    order_num TEXT,
    action_type VARCHAR(1) CHECK (action_type IN ('U', 'D')),
    log_date TIMESTAMPTZ DEFAULT NOW()
);


-- Step 2: UPDATE + DELETE + log both; all in one CTE statement
-----------------------------------------------------------------------------

WITH updated_orders AS (
    -- UPDATE: halve the cost for orders between 100 and 1000
    UPDATE labs.orders
    SET order_cost = order_cost / 2
    WHERE order_cost BETWEEN 100 AND 1000
    RETURNING order_id,
              order_cost,   -- this is the NEW halved value
              order_num
),
deleted_orders AS (
    -- DELETE: remove orders with cost < 50
    -- Important: this sees the ORIGINAL cost values, not what the UPDATE wrote,
    -- because all CTEs in the same statement share the same data snapshot.
    DELETE FROM labs.orders
    WHERE order_cost < 50
    RETURNING order_id,
              order_cost,
              order_num
),
log_insert AS (
    -- Log the updated rows as action type 'U'
    INSERT INTO labs.order_log (order_id, order_cost, order_num, action_type)
    SELECT order_id, order_cost, order_num, 'U'
    FROM updated_orders
    RETURNING *
)
-- Log the deleted rows as action type 'D', this final statement also
-- drives execution of all the CTEs above
INSERT INTO labs.order_log (order_id, order_cost, order_num, action_type)
SELECT order_id, order_cost, order_num, 'D'
FROM deleted_orders;

/*
Data-modifying CTEs all run against the same snapshot of the data at the
start of the statement, so the DELETE sees the original order_cost values
even though the UPDATE ran "first". They are completely independent of each
other's changes, neither can see what the other wrote.
RETURNING is what lets us pipe affected rows from the UPDATE/DELETE into the
log inserts. Without it, there would be no way to capture which rows changed.
The final INSERT is what actually drives execution of all the CTEs above it,
CTE sub-statements only run if they are referenced (directly or indirectly)
by the main statement.
*/


-- Verification queries
-----------------------------------------------------------------------------

SELECT *
FROM labs.orders
ORDER BY order_id
LIMIT 20;
-- results shown in screenshot: task7_1.png

SELECT action_type,
       COUNT(*) AS logged_rows
FROM labs.order_log
GROUP BY action_type
ORDER BY action_type;
-- results shown in screenshot: task7_2.png

SELECT *
FROM labs.order_log
ORDER BY log_id
LIMIT 20;
-- results shown in screenshot: task7_3.png