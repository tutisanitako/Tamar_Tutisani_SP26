-- SQL DCL Homework: Tasks 2 and 3
-- Role-based Access Control and Row-Level Security for dvdrental
-----------------------------------------------------------------------------

-- Task 2. Implement role-based authentication model for dvd_rental database
-----------------------------------------------------------------------------


-- 2.1  Create rentaluser with CONNECT only
-----------------------------------------------------------------------------
/*
Task:
Create a new user with the username "rentaluser" and the password "rentalpassword".
Give the user the ability to connect to the database but no other permissions.
*/

-- At this point they can connect to the database but can't do 
-- anything once inside, no schema access, no table access.
CREATE USER rentaluser WITH LOGIN PASSWORD 'rentalpassword';

GRANT CONNECT ON DATABASE dvdrental TO rentaluser;

-- check to confirm the role was created with login enabled
SELECT
	rolname,
	rolcanlogin,
	rolsuper
FROM pg_catalog.pg_roles
WHERE rolname = 'rentaluser';


-- 2.2  Grant SELECT on customer; verify allowed and denied access
-----------------------------------------------------------------------------
/*
Task:
Grant "rentaluser" permission allows reading data from the "customer" table. 
Сheck to make sure this permission works correctly: write a SQL query to select all customers.
*/

-- USAGE on the schema is required first, without it PostgreSQL can't
-- even resolve the table name, regardless of any table-level grant.
GRANT USAGE ON SCHEMA public TO rentaluser;
GRANT SELECT ON public.customer TO rentaluser;

-- rentaluser can now read the customer table.
SET ROLE rentaluser;

SELECT *
FROM public.customer;

RESET ROLE;

-- DENIED: rentaluser has no INSERT on customer. should get permission error.
SET ROLE rentaluser;

INSERT INTO public.customer (
	store_id,
	first_name,
	last_name,
	address_id,
	activebool,
	create_date
)
VALUES (1, 'Test', 'User', 1, true, now());
-- Expected: ERROR: permission denied for table customer

RESET ROLE;


-- 2.3  Create group role "rental" and add rentaluser to it
-----------------------------------------------------------------------------
/*
Task:
Create a new user group called "rental" and add "rentaluser" to the group.
*/

-- NOLOGIN because this is a group role, not a real user.
-- INHERIT (default) means rentaluser automatically gets whatever we
-- grant to this group without needing SET ROLE.
CREATE ROLE rental NOLOGIN INHERIT;

GRANT rental TO rentaluser;

-- Confirm the membership was created.
SELECT
	r.rolname AS group_role,
	m.rolname AS member_role
FROM pg_auth_members am
JOIN pg_roles r ON r.oid = am.roleid
JOIN pg_roles m ON m.oid = am.member
WHERE r.rolname = 'rental';


-- 2.4  Grant INSERT and UPDATE on rental
-----------------------------------------------------------------------------
/*
Task:
Grant the "rental" group INSERT and UPDATE permissions for the "rental" table.
Insert a new row and update one existing row in the "rental" table under that role. 
*/

-- The sequence grant is needed so INSERT can get the next rental_id.
-- SELECT is granted here because our UPDATE uses a WHERE clause that
-- references column values (rental_date, customer_id), which requires
-- reading those columns first. A simple UPDATE with no WHERE or with
-- only a known primary key and no column value references would not
-- need SELECT. Since the task requires a realistic UPDATE with a WHERE
-- clause, SELECT is necessary. (Hardcoded values are used here for
-- testing purposes only.)
GRANT SELECT, INSERT, UPDATE ON public.rental TO rental;
GRANT USAGE ON SEQUENCE public.rental_rental_id_seq TO rental;

-- ALLOWED:

SET ROLE rentaluser;

INSERT INTO public.rental (
	rental_date,
	inventory_id,
	customer_id,
	return_date,
	staff_id,
	last_update
)
VALUES (
	'2017-01-20 12:00:00+00',
	367,
	130,
	NULL,
	1,
	now()
)
RETURNING
	rental_id,
	rental_date,
	inventory_id,
	customer_id,
	return_date,
	staff_id;


-- ALLOWED: UPDATE — RETURNING shows the row in its updated state.
UPDATE public.rental
SET return_date = '2017-01-27 12:00:00+00'
WHERE rental_date = '2017-01-20 12:00:00+00' AND 
	  customer_id = 130
RETURNING
	rental_id,
	rental_date,
	return_date,
	customer_id;

RESET ROLE;

-- DENIED: rentaluser has no DELETE on rental.
SET ROLE rentaluser;

DELETE FROM public.rental
WHERE rental_date = '2017-01-20 12:00:00+00' AND
	  customer_id = 130;
-- Expected: ERROR: permission denied for table rental

RESET ROLE;


-- 2.5  Revoke INSERT from rental group. verify insert is now denied
-----------------------------------------------------------------------------
/*
Task:
Revoke the "rental" group's INSERT permission for the "rental" table.
Try to insert new rows into the "rental" table make sure this action is denied.
*/

REVOKE INSERT ON public.rental FROM rental;

-- DENIED: INSERT should now fail for rentaluser.
SET ROLE rentaluser;

INSERT INTO public.rental (
	rental_date,
	inventory_id,
	customer_id,
	return_date,
	staff_id,
	last_update
)
VALUES (
	'2017-01-21 12:00:00+00',
	367,
	130,
	NULL,
	1,
	now()
);
-- Expected: ERROR: permission denied for table rental

RESET ROLE;

-- STILL ALLOWED: UPDATE was not revoked, so it should still work.
SET ROLE rentaluser;

UPDATE public.rental
SET last_update = now()
WHERE rental_date = '2017-01-20 12:00:00+00'
  AND customer_id = 130
RETURNING
	rental_id,
	last_update;

RESET ROLE;


-- 2.6  Create a personalized role for customer Marcia Dean
-----------------------------------------------------------------------------
/*
Task:
Create a personalized role for any customer already existing in the dvd_rental database.
The name of the role name must be client_{first_name}_{last_name} (omit curly brackets).
The customer's payment and rental history must not be empty. 
*/

-- I picked Marcia Dean (customer_id = 236) by running this query first
-- to find customers who actually have both rental and payment history,
-- sorted by most active:

--SELECT
--	c.customer_id,
--	c.first_name,
--	c.last_name,
--	'client_' || LOWER(c.first_name) || '_' || LOWER(c.last_name) AS proposed_role_name,
--	COUNT(DISTINCT r.rental_id) AS rental_count,
--	COUNT(DISTINCT p.payment_id) AS payment_count
--FROM public.customer  c
--INNER JOIN public.rental r ON r.customer_id = c.customer_id
--INNER JOIN public.payment p ON p.customer_id = c.customer_id
--WHERE c.activebool = TRUE
--GROUP BY
--	c.customer_id,
--	c.first_name,
--	c.last_name
--HAVING
--	COUNT(DISTINCT r.rental_id) > 0 AND
--	COUNT(DISTINCT p.payment_id) > 0
--ORDER BY
--	rental_count DESC,
--	payment_count DESC
--LIMIT 10;

-- Result: customer_id = 236, 84 rentals, 84 payments

CREATE ROLE client_marcia_dean LOGIN PASSWORD 'client_marcia_dean_pass';

GRANT CONNECT ON DATABASE dvdrental TO client_marcia_dean;
GRANT USAGE ON SCHEMA public TO client_marcia_dean;


-----------------------------------------------------------------------------
-- TASK 3. Implement row-level security
-----------------------------------------------------------------------------
/*
Goal: client_marcia_dean can SELECT from rental and payment, but she
should only ever see her own rows (customer_id = 236).

A SECURITY DEFINER function runs with the privileges of the person who
created it (the superuser) rather than the person calling it. So the
function can freely read the customer table to find Marcia's
customer_id, even though the client role itself has no access to that
table at all. The policy USING clause just calls the function, so it
never needs to touch the customer table directly.

The function parses current_setting('role') (the active SET ROLE value)
to extract first and last name, then returns the matching customer_id.
current_setting('role') is used instead of current_user because inside
a SECURITY DEFINER function current_user returns the function OWNER
(the superuser), not the calling role. current_setting('role') always
reflects the role set via SET ROLE regardless of the SECURITY DEFINER
context. this means the function is designed for use via proxy
authentication (SET ROLE). If client_marcia_dean connects directly
without SET ROLE, current_setting('role') returns an empty string and
the function wouldreturn NULL, filtering out all rows.

Notes:
1. EXECUTE on this function is revoked from PUBLIC and granted
   explicitly to client_marcia_dean. This means random roles cannot
   call the function directly. The RLS policy engine can still evaluate
   it because client_marcia_dean has the explicit grant.

2. SET search_path = public, pg_catalog on the function.
   A SECURITY DEFINER function runs as its owner (superuser), which
   means a malicious caller could potentially swap in a rogue schema
   before calling it and hijack the table lookup. Pinning the
   search_path to exactly the schemas we need closes that attack vector.

3. VOLATILE is used to prevent the planner from caching or inlining
   the current_setting('role') call across statements. Even though the
   call is wrapped in (SELECT ...) inside the policy to avoid per-row
   evaluation, VOLATILE ensures the value is always read fresh at
   query start rather than potentially being optimized away entirely.
*/

-- Grant SELECT on the tables first, RLS controls which rows are
-- visible, but the role still needs the table-level privilege to even
-- run a query.
GRANT SELECT ON public.rental TO client_marcia_dean;
GRANT SELECT ON public.payment TO client_marcia_dean;


-- Helper function: looks up the customer_id for whoever is currently
-- connected, based on their role name following the client_{fn}_{ln}
-- pattern. SECURITY DEFINER means it runs with the owner's privileges
-- (superuser), so it can read the customer table without the calling
-- role needing any grant on it.
CREATE OR REPLACE FUNCTION public.fn_current_customer_id()
RETURNS integer
LANGUAGE sql
SECURITY DEFINER
VOLATILE
SET search_path = public, pg_catalog
AS $$
    SELECT customer_id
    FROM public.customer
    WHERE LOWER(first_name) = LOWER(split_part(current_setting('role'), '_', 2)) AND 
		  LOWER(last_name) = LOWER(split_part(current_setting('role'), '_', 3))
    LIMIT 1;
$$;
-- Note: this parsing assumes the role name follows client_{single_first}_{single_last}
-- exactly. Customers with compound first or last names would not match correctly
-- and would need a different lookup strategy (e.g. a dedicated mapping table).


-- Grant EXECUTE explicitly to the client role.
-- revoke from PUBLIC so random roles can't call it directly,
-- but the policy engine still works because client_marcia_dean
-- has the explicit grant.
REVOKE EXECUTE ON FUNCTION public.fn_current_customer_id() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.fn_current_customer_id() TO client_marcia_dean;


-- Enable RLS and create the policy for rental.
ALTER TABLE public.rental ENABLE ROW LEVEL SECURITY;

-- FOR SELECT only. INSERT/UPDATE policies aren't needed because
-- client_marcia_dean has no INSERT or UPDATE grant on this table,
-- so those would fail at the privilege check before RLS even runs.
CREATE POLICY client_see_own_rentals
    ON public.rental
    FOR SELECT
    USING (customer_id = (SELECT public.fn_current_customer_id()));


-- payment is a partitioned table. RLS must be enabled on the parent
-- and on every partition individually, because enabling it on the
-- parent does not cascade to partitions automatically. However,
-- a policy created on the parent DOES apply to all partitions in
-- PostgreSQL 12+, so we only need one CREATE POLICY on the parent.
-- Enabling RLS on each partition without a partition-level policy is
-- intentional: they inherit the parent's policy, but RLS still needs
-- to be switched on per partition for that inheritance to take effect.
ALTER TABLE public.payment ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.payment_p2017_01 ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.payment_p2017_02 ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.payment_p2017_03 ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.payment_p2017_04 ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.payment_p2017_05 ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.payment_p2017_06 ENABLE ROW LEVEL SECURITY;

CREATE POLICY client_see_own_payments
	ON public.payment
	FOR SELECT
	USING (customer_id = (SELECT public.fn_current_customer_id()));


-- ALLOWED: client_marcia_dean should see only her own rows.
SET ROLE client_marcia_dean;

-- Expected: only rows where customer_id = 236.
SELECT
	rental_id,
	rental_date,
	customer_id,
	return_date
FROM public.rental
LIMIT 10;

-- Expected: only rows where customer_id = 236.
SELECT
	payment_id,
	customer_id,
	amount,
	payment_date
FROM public.payment
LIMIT 10;

-- Sanity check — if RLS is working, min and max customer_id must both
-- be 236 and the count should match Marcia's 84 rentals/payments.
SELECT
	COUNT(*) AS total_rentals,
	MIN(customer_id) AS min_cid,
	MAX(customer_id) AS max_cid
FROM public.rental;

SELECT
	COUNT(*) AS total_payments,
	MIN(customer_id) AS min_cid,
	MAX(customer_id) AS max_cid
FROM public.payment;

RESET ROLE;


-- DENIED: client_marcia_dean must not see rows belonging to other customers.
SET ROLE client_marcia_dean;

-- RLS silently filters these out. 0 rows returned, not an error, which's 
-- the correct RLS behaviour: the rows just don't appear.
SELECT *
FROM public.rental
WHERE customer_id = 1;

SELECT *
FROM public.payment
WHERE customer_id = 1;

-- No SELECT was granted on customer at all.
-- Expected: ERROR: permission denied for table customer
SELECT * FROM public.customer;

RESET ROLE;