-- TASK 2: POSTGRESQL TRANSACTIONS
-- Tamar Tutisani
-- Module: PG_DWH
-----------------------------------------------------------------------------


----------------------------------------------------------------
-- TASK 1: CREATE EMPLOYEE TABLE
----------------------------------------------------------------

DROP TABLE IF EXISTS public.employee;

CREATE TABLE public.employee (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL
);


----------------------------------------------------------------
-- TASK 2: REPLICATE THE LECTURE EXAMPLE
----------------------------------------------------------------
-- The lecture showed two parallel transactions to demonstrate
-- how xmin, xmax behave during INSERT, DELETE, UPDATE.
----------------------------------------------------------------


-- STEP 2.1: INSERT + concurrent read
----------------------------------------------------------------

-- [SESSION A - Tab A] Runing this first:
BEGIN;
SELECT txid_current();
-- Shown txid will appear as xmin on Alice's row.

INSERT INTO public.employee (name, status)
VALUES ('Alice', 'Not fired');

SELECT *,
       xmin,
       xmax
FROM public.employee e;

-- Alice is visible here because we are the inserting transaction.
-- xmin = txid shown above, xmax = 0 (not deleted).

-- screenshot task2_1a.png shows Alice's row with xmin set and xmax = 0,
-- inside Session A before COMMIT.


-- [SESSION B - Tab B] Running this while Session A has not been committed yet:
BEGIN;

SELECT *,
       xmin,
       xmax
FROM public.employee e;
-- Alice does NOT appear here because Session A has not committed.
-- This demonstrates that Read Committed prevents dirty reads.

-- screenshot task2_1b.png shows empty result, confirming no dirty read.

COMMIT;


-- [SESSION A - Tab A] Now commiting:
COMMIT;


-- [SESSION B - Tab B] Reading again after Session A committed:
BEGIN;

SELECT *,
       xmin,
       xmax
FROM public.employee e;
-- Alice now appears. xmin = Session A's txid, xmax = 0.

-- screenshot task2_1a.png shows Alice visible with xmin set 
-- and xmax = 0 after Session A committed.

COMMIT;


-- STEP 2.2: DELETE + concurrent read (non-repeatable read demo)
----------------------------------------------------------------

-- Insert a second row for the DELETE/UPDATE demos.
INSERT INTO public.employee (name, status)
VALUES ('Alice', 'Not fired');

-- Table now has two rows: id=1 and id=2, both named Alice.


-- [SESSION B - Tab B] Starting read BEFORE Session A deletes:
BEGIN;

SELECT *,
       xmin,
       xmax
FROM public.employee e;
-- Both rows visible, xmax = 0 on both.

-- screenshot task2_2a.png shows both rows visible with xmax = 0.


-- [SESSION A - Tab A] Performing DELETE id = 1 while Session B is still open:

DELETE FROM public.employee
WHERE id = 1;

SELECT *,
       xmin,
       xmax
FROM public.employee e;
-- id=1 is no longer visible inside Session A (deleted within same txn).
-- Only id=2 shows.

-- screenshot task2_2b.png shows only id=2 inside Session A. id=1 is 
-- invisible to the deleting session.

COMMIT;


-- [SESSION B - Tab B] Read again after Session A committed the DELETE:
SELECT *,
       xmin,
       xmax
FROM public.employee e;
-- At Read Committed: id=1 now disappears from Session B as well.
-- This is a non-repeatable read: first read saw 2 rows, second read sees 1.

-- screenshot task2_2c.png shows only id=2, demonstrating the non-repeatable read at Read Committed.

COMMIT;


-- STEP 2.3: UPDATE + concurrent read (two physical row versions)
----------------------------------------------------------------

-- [SESSION B - Tab B] Starting read before Session A updates:
BEGIN;

SELECT *,
       xmin,
       xmax
FROM public.employee e;
-- Only id=2 visible (id=1 was deleted). xmax = 0, screenshot task2_3a.png shows this.


-- [SESSION A - Tab A] UPDATE id = 2:
BEGIN;
SELECT txid_current();
-- this txid becomes xmax on the old version and xmin on the new one.

UPDATE public.employee
SET status = 'Fired'
WHERE id = 2;

SELECT *,
       xmin,
       xmax
FROM public.employee e;
-- PostgreSQL UPDATE = delete old row version + insert new row version.
-- Inside Session A: only the new version (status='Fired') is visible.
-- xmin = this txid, xmax = 0. screenshot task2_3b.png shows this.

COMMIT;


-- [SESSION B - Tab B] Read again after Session A committed the UPDATE:
SELECT *,
       xmin,
       xmax
FROM public.employee e;
-- At Read Committed: Session B now sees status='Fired'.
-- Another non-repeatable read: first read showed 'Not fired', second shows 'Fired'.

-- screenshot task2_3c.png shows id=2 with status='Fired' after Session A committed.

COMMIT;


----------------------------------------------------------------
-- TASK 3 + 4: ISOLATION LEVELS
----------------------------------------------------------------

-- Setting Repeatable Read for the current transaction:
BEGIN;
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SHOW TRANSACTION ISOLATION LEVEL;
-- Output: repeatable read
-- At Repeatable Read, the entire transaction sees ONE snapshot taken at
-- the start of the first statement. Rows committed by other transactions
-- after that point remain invisible for the rest of this transaction.
-- Prevents dirty reads and non-repeatable reads.
-- Does NOT prevent serialization anomalies (requires Serializable for that).

-- screenshot task3_1.png shows SHOW TRANSACTION ISOLATION LEVEL returning 'repeatable read'.

COMMIT;


-- Check the default isolation level outside any explicit transaction:
SHOW TRANSACTION ISOLATION LEVEL;

-- screenshot task3_2.png shows default isolation level = read committed.


----------------------------------------------------------------
-- TASK 5: ADD cmin AND cmax TO SELECTS
----------------------------------------------------------------

DROP TABLE IF EXISTS public.employee;

CREATE TABLE public.employee (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL
);

-- [SESSION A - Tab A] INSERT two rows in one transaction:
BEGIN;
SELECT txid_current();

INSERT INTO public.employee (name, status) 
VALUES ('Alice', 'Not fired');
INSERT INTO public.employee (name, status) 
VALUES ('Bob',   'Not fired');

SELECT *,
       xmin,
       xmax,
       cmin,
       cmax
FROM public.employee e;
-- cmin = command ID within the inserting transaction (starts at 0).
-- Alice: cmin = 0 (first INSERT command in this transaction).
-- Bob:   cmin = 1 (second INSERT command in this transaction).
-- cmin lets PostgreSQL distinguish rows created by different statements
-- within the same transaction. Without it, intra-transaction ordering
-- would be impossible to determine.

-- screenshot task5_1.png shows both rows with cmin = 0 and cmin = 1 respectively, xmax = 0.

COMMIT;


-- [SESSION A - Tab A] UPDATE and DELETE to demonstrate cmax:
BEGIN;
SELECT txid_current();

UPDATE public.employee SET status = 'Fired' WHERE id = 1;
DELETE FROM public.employee WHERE id = 2;

SELECT *,
       xmin,
       xmax,
       cmin,
       cmax
FROM public.employee e;
-- The new version of id=1 (status='Fired'): xmin = this txid, cmin = 0, xmax = 0.
-- id=2 was deleted: it no longer appears in this SELECT (deleted in same txn).
-- cmax on the deleted row is set to the command ID of the DELETE statement.

-- screenshot task5_2.png shows id=1 with new status='Fired', cmin=0. id=2 is gone from the visible result.

COMMIT;


----------------------------------------------------------------
-- TASK 6 (*): SERIALIZATION ANOMALY
----------------------------------------------------------------

DROP TABLE IF EXISTS public.employee;

CREATE TABLE public.employee (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL
);

INSERT INTO public.employee (name, status)
VALUES
    ('Alice', 'Active'),
    ('Bob', 'Active'),
    ('Carol', 'Inactive');


-- Attempt 1: Read Committed (anomaly occurs, no error raised)

-- [SESSION A - Tab A]:
BEGIN;
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

SELECT COUNT(*) 
FROM public.employee 
WHERE status = 'Active';
-- Returns 2. Based on this, Session A decides to insert an Inactive employee.
INSERT INTO public.employee (name, status) 
VALUES ('Dave_RC', 'Inactive');

-- [SESSION B - Tab B] (runing this at the same time as Session A above):
BEGIN;
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

SELECT COUNT(*) 
FROM public.employee 
WHERE status = 'Inactive';
-- Returns 1. Based on this, Session B decides to insert an Active employee.
INSERT INTO public.employee (name, status) 
VALUES ('Eve_RC', 'Active');

COMMIT;

COMMIT; --SESSION A - Tab A commit

-- After both commit:
SELECT * 
FROM public.employee;
-- Both Dave_RC and Eve_RC are present. No error was raised.
-- But the counts each session acted on are now stale.
-- The outcome is inconsistent with any serial execution order: this is a
-- serialization anomaly. Read Committed does not prevent it.

-- screenshot task6_1.png shows final table with Dave_RC and Eve_RC both 
-- inserted, no error raised.


-- Attempt 2: Serializable (anomaly detected, one transaction aborted)

DELETE FROM public.employee 
WHERE name IN ('Dave_RC', 'Eve_RC');

-- [SESSION A - Tab A]:
BEGIN;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

SELECT COUNT(*) 
FROM public.employee 
WHERE status = 'Active';

INSERT INTO public.employee (name, status) 
VALUES ('Dave_SER', 'Inactive');


-- [SESSION B - Tab B] (run at the same time as Session A above):
BEGIN;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SELECT COUNT(*) 
FROM public.employee 
WHERE status = 'Inactive';

INSERT INTO public.employee (name, status) 
VALUES ('Eve_SER', 'Active');

COMMIT;

COMMIT; 
-- One of the two sessions will receive:
-- ERROR: could not serialize access due to read/write dependencies among transactions

-- screenshot task6_2.png shows the ERROR message.


SELECT * 
FROM public.employee;

-- screenshot task6_3.png shows only one (Eve_SER) present. The other was rolled back.


----------------------------------------------------------------
-- TASK 7 (*): LOST UPDATE AT READ COMMITTED
----------------------------------------------------------------

DROP TABLE IF EXISTS public.employee;

CREATE TABLE public.employee (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    salary NUMERIC(10,2) NOT NULL DEFAULT 0
);

INSERT INTO public.employee (name, status, salary)
VALUES ('Alice', 'Active', 1000.00);

-- Both sessions read salary = 1000 and both try to add 100.
-- The last write overwrites the first: a lost update.

-- [SESSION A - Tab A]:
BEGIN;
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

SELECT salary 
FROM public.employee 
WHERE id = 1;
-- Returns 1000. Session A will set salary to 1000 + 100 = 1100.

UPDATE public.employee 
SET salary = 1000 + 100 
WHERE id = 1;

COMMIT; --SESSION A - Tab A commit


-- [SESSION B - Tab B] (run concurrently with Session A):
BEGIN;
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

SELECT salary 
FROM public.employee 
WHERE id = 1;
-- Also returns 1000 (read before Session A committed).
-- Session B also computes 1000 + 100 = 1100.
-- Session B's UPDATE blocks until Session A commits, then re-reads and
-- still sets salary to 1100, overwriting Session A's result.

UPDATE public.employee 
SET salary = 1000 + 100 
WHERE id = 1;

COMMIT;


SELECT salary FROM public.employee WHERE id = 1;
-- Returns 1100. Expected 1200. Session A's +100 was lost. No error raised.

-- task7_1.png demonstrates the lost update.

-- Downside of PostgreSQL's approach:
-- No error is raised. Application gets a success from both sessions but
-- the data is silently wrong. These bugs are extremely hard to find in production.

-- Fix: use a relative UPDATE so the read and write happen atomically:
UPDATE public.employee 
SET salary = salary + 100
WHERE id = 1;
-- This can never produce a lost update regardless of isolation level.

SELECT salary 
FROM public.employee 
WHERE id = 1;
-- Now returns 1200 (1100 + 100 from the fix above), shown in task7_2.png