-- Task: applying the TCL & DML Statement

-------------------------------------------------------

-- Task 1

/*
FILMS
	1. 13 Going on 30 (2004) – Comedy  | rate 4.99  | duration 7  days (1 week)
	2. Coherence      (2013) – Sci-Fi  | rate 9.99  | duration 14 days (2 weeks)
	3. The Prestige   (2006) – Drama   | rate 19.99 | duration 21 days (3 weeks)

ACTORS
	13 Going on 30: Jennifer Garner, Mark Ruffalo, Judy Greer
	Coherence: Emily Baldoni, Maury Sterling, Nicholas Brendon
	The Prestige: Hugh Jackman, Christian Bale, Michael Caine
*/


-- SUBTASK 1 & 2: INSERT FILMS
/*
WHY A SEPARATE TRANSACTION:
	All three film inserts are grouped into one atomic unit.
	If any insert fails (e.g. language not found), ALL roll back,
	preventing partially inserted catalog data.

IF THIS TRANSACTION FAILS:
	No films are inserted. film table remains unchanged.
	All three films must succeed together or not at all (atomicity).

ROLLBACK: Fully possible. no DDL is used inside this block.

INSERT...SELECT ADVANTAGE:
	language_id is resolved at runtime via JOIN to the language table
	rather than being hardcoded. This makes the script portable across
	DB instances and safe against ID drift over time.

REFERENTIAL INTEGRITY:
	The language_id FK is always satisfied because it is derived
	from an actual row in the language table via JOIN.

DUPLICATE PREVENTION:
	WHERE NOT EXISTS checks the title (case-insensitive) so re-running
	the script does not produce duplicate films.
*/

BEGIN;

INSERT INTO public.film (
	title,
	description,
	release_year,
	language_id,
	rental_duration,
	rental_rate,
	length,
	replacement_cost,
	rating,
	last_update
)
SELECT
	film_data.title,
	film_data.description,
	film_data.release_year,
	lang.language_id,
	film_data.rental_duration,
	film_data.rental_rate,
	film_data.length,
	film_data.replacement_cost,
	film_data.rating,
	CURRENT_TIMESTAMP AS last_update
FROM (
	VALUES
		('13 Going on 30',
		 'A 13-year-old girl magically wakes up as a 30-year-old woman in Manhattan.',
		 2004, 'English', 7, 4.99, 98, 24.99, 'PG-13'::public.mpaa_rating),
		('Coherence',
		 'Strange phenomena unfold for a group of friends at a dinner party during a comet pass.',
		 -- Note: Coherence is unrated (NR). using 'R' as the closest valid mpaa_rating enum value.
		 2013, 'English', 14, 9.99, 89, 24.99, 'R'::public.mpaa_rating),
		('The Prestige',
		 'Two rival magicians obsessively compete to create the ultimate illusion at great cost.',
		 2006, 'English', 21, 19.99, 130, 24.99, 'PG-13'::public.mpaa_rating)
) AS film_data(
	title,
	description,
	release_year,
	language_name,
	rental_duration,
	rental_rate,
	length,
	replacement_cost,
	rating
)
-- joining on name match since film_data is a VALUES subquery with no FK column
INNER JOIN public.language lang ON lower(lang.name) = lower(film_data.language_name)
WHERE NOT EXISTS (
	SELECT 1
	FROM public.film f
	WHERE lower(f.title) = lower(film_data.title)
)
RETURNING film_id, title, description, release_year, language_id,
	rental_duration, rental_rate, length, replacement_cost, rating, last_update;

COMMIT;


-- SUBTASK 3.1: INSERT ACTORS
/*
WHY A SEPARATE TRANSACTION:
	Actor inserts are logically independent of film inserts.
	A failure here should NOT roll back the already-committed films.

IF THIS TRANSACTION FAILS:
	No actors are inserted. Previously committed films remain intact.

ROLLBACK: Fully possible. no DDL used.

INSERT...SELECT ADVANTAGE:
	Using SELECT makes the pattern consistent and allows WHERE NOT EXISTS
	to be embedded cleanly across all rows at once.

DUPLICATE PREVENTION:
	WHERE NOT EXISTS checks the (first_name, last_name) pair
	case-insensitively. The actor table has no UNIQUE constraint on
	name columns, so ON CONFLICT cannot be used here. WHERE NOT EXISTS
	is the correct idiomatic approach.
*/

BEGIN;

INSERT INTO public.actor (
	first_name,
	last_name,
	last_update
)
SELECT
	actor_data.first_name,
	actor_data.last_name,
	CURRENT_TIMESTAMP AS last_update
FROM (
	VALUES
		-- 13 Going on 30
		('Jennifer', 'Garner'),
		('Mark', 'Ruffalo'),
		('Judy', 'Greer'),
		-- Coherence
		('Emily', 'Baldoni'),
		('Maury', 'Sterling'),
		('Nicholas', 'Brendon'),
		-- The Prestige
		('Hugh', 'Jackman'),
		('Christian', 'Bale'),
		('Michael', 'Caine')
) AS actor_data(first_name, last_name)
WHERE NOT EXISTS (
	SELECT 1
	FROM public.actor a
	WHERE lower(a.first_name) = lower(actor_data.first_name)
		AND lower(a.last_name) = lower(actor_data.last_name)
)
RETURNING actor_id, first_name, last_name, last_update;

COMMIT;


-- SUBTASK 3.2: LINK ACTORS TO FILMS (film_actor)
/*
WHY A SEPARATE TRANSACTION:
	film_actor links depend on both actors and films existing.
	On failure, films and actors remain committed, only this linking
	step rolls back cleanly.

IF THIS TRANSACTION FAILS:
	film and actor tables remain unchanged.
	film_actor links roll back cleanly with no orphaned data.

ROLLBACK: Fully possible. no DDL used.

INSERT...SELECT ADVANTAGE:
	Both film_id and actor_id are resolved dynamically by name at
	runtime via JOINs to the actor and film tables. No IDs hardcoded.

DUPLICATE PREVENTION:
	Because the table is already set up to prevent an actor from being
	linked to the same movie twice (the composite PK), we use the
	ON CONFLICT DO NOTHING command to skip duplicates.
	This is the cleanest way to make the script run-safe
	without needing extra checks.

REFERENTIAL INTEGRITY:
	actor_id and film_id come from actual rows in their respective
	tables via JOINs. Both FK values are guaranteed to exist before
	the row is produced.
*/

BEGIN;

INSERT INTO public.film_actor (
	actor_id,
	film_id,
	last_update
)
SELECT
	a.actor_id,
	f.film_id,
	CURRENT_TIMESTAMP AS last_update
FROM (
	VALUES
		-- 13 Going on 30
		('Jennifer', 'Garner', '13 Going on 30'),
		('Mark', 'Ruffalo', '13 Going on 30'),
		('Judy', 'Greer', '13 Going on 30'),
		-- Coherence
		('Emily', 'Baldoni', 'Coherence'),
		('Maury', 'Sterling', 'Coherence'),
		('Nicholas', 'Brendon', 'Coherence'),
		-- The Prestige
		('Hugh', 'Jackman', 'The Prestige'),
		('Christian', 'Bale', 'The Prestige'),
		('Michael', 'Caine', 'The Prestige')
) AS actor_data(first_name, last_name, film_title)
-- joining on name match since actor_data is a VALUES subquery with no FK column
INNER JOIN public.actor a ON lower(a.first_name) = lower(actor_data.first_name)
	AND lower(a.last_name) = lower(actor_data.last_name)
INNER JOIN public.film f ON lower(f.title) = lower(actor_data.film_title)
ON CONFLICT (actor_id, film_id) DO NOTHING
RETURNING actor_id, film_id, last_update;

COMMIT;


-- SUBTASK 3.3: LINK FILMS TO CATEGORIES (film_category)
/*
WHY A SEPARATE TRANSACTION:
	Category linking is a distinct catalog enrichment step.
	Failure here leaves film, actor, and film_actor intact.

IF THIS TRANSACTION FAILS:
	All three category links roll back.
	Films, actors, and film_actor rows remain committed and intact.

ROLLBACK: Fully possible. no DDL used.

INSERT...SELECT ADVANTAGE:
	Both film_id and category_id are resolved dynamically by name.
	No IDs hardcoded, works regardless of sequence state.

DUPLICATE PREVENTION:
	ON CONFLICT (film_id, category_id) DO NOTHING uses the composite
	PK on film_category, safe for re-runs.

REFERENTIAL INTEGRITY:
	film_id and category_id are derived from actual rows in their
	respective tables via JOINs. Both FK values are guaranteed to
	exist before the row is produced.

GENRE MAPPING (using dvdrental category names exactly):
	13 Going on 30 -> Comedy
	Coherence      -> Sci-Fi
	The Prestige   -> Drama
*/

BEGIN;

INSERT INTO public.film_category (
	film_id,
	category_id,
	last_update
)
SELECT
	f.film_id,
	cat.category_id,
	CURRENT_TIMESTAMP AS last_update
FROM (
	VALUES
		('13 Going on 30', 'Comedy'),
		('Coherence', 'Sci-Fi'),
		('The Prestige', 'Drama')
) AS cat_data(film_title, category_name)
-- joining on name match since cat_data is a VALUES subquery with no FK column
INNER JOIN public.film f ON lower(f.title) = lower(cat_data.film_title)
INNER JOIN public.category cat ON lower(cat.name) = lower(cat_data.category_name)
ON CONFLICT (film_id, category_id) DO NOTHING
RETURNING film_id, category_id, last_update;

COMMIT;


-- SUBTASK 4: ADD FILMS TO STORE INVENTORY
/*
WHY A SEPARATE TRANSACTION:
	Inventory additions are logically distinct from catalog setup.
	Failure here does not affect any previously committed data.

IF THIS TRANSACTION FAILS:
	All three inventory inserts roll back.
	Films remain in the catalog, only the store copy is missing.

ROLLBACK: Fully possible. no DDL used.

INSERT...SELECT ADVANTAGE:
	film_id is resolved from film by title. store_id is resolved
	dynamically from the store table. No IDs hardcoded.

STORE SELECTION:
	MIN(store_id) is used to pick a store deterministically
	on every re-run without hardcoding an ID.

DUPLICATE PREVENTION:
	WHERE NOT EXISTS prevents adding the same film to the same store
	a second time on re-runs.

REFERENTIAL INTEGRITY:
	Both film_id and store_id FKs are derived from actual table rows.
	The WHERE clause ensures both values exist before the row is produced.
*/

BEGIN;

INSERT INTO public.inventory (
	film_id,
	store_id,
	last_update
)
SELECT
	f.film_id,
	(SELECT MIN(store_id) FROM public.store) AS store_id,
	CURRENT_TIMESTAMP AS last_update
FROM (
	VALUES
		('13 Going on 30'),
		('Coherence'),
		('The Prestige')
) AS inv_data(film_title)
-- joining on name match since inv_data is a VALUES subquery with no FK column
INNER JOIN public.film f ON lower(f.title) = lower(inv_data.film_title)
WHERE NOT EXISTS (
	SELECT 1
	FROM public.inventory i
	WHERE i.film_id = f.film_id
		AND i.store_id = (SELECT MIN(store_id) FROM public.store)
)
RETURNING inventory_id, film_id, store_id, last_update;

COMMIT;


-- SUBTASK 5: UPDATE EXISTING CUSTOMER
/*
WHY A SEPARATE TRANSACTION:
	The UPDATE is independent, failure does not affect catalog data.

IF THIS TRANSACTION FAILS:
	customer table remains unchanged.
	No cascading effect on rental or payment records.

ROLLBACK: Fully possible before COMMIT.

TARGET SELECTION:
	Dynamically identifies the customer_id of a customer with
	>=43 rental records AND >=43 payment records, ordered DESC by
	rental count to pick the most active customer.
	LIMIT 1: multiple customers may satisfy both thresholds, we
	intentionally pick the single most active one as the best
	candidate. If the selection criterion changes, only ORDER BY
	needs updating.

ADDRESS NOTE:
	Because the address table is shared by many customers, I am
	not modifying an existing address record with my personal details
	(to avoid changing other people's data). Instead, I am updating the
	address_id on my customer record to point to an existing, valid
	location already in the system.

last_update NOTE:
	The customer table has a BEFORE UPDATE trigger (last_updated())
	that automatically sets last_update = CURRENT_TIMESTAMP on every
	UPDATE. last_update is therefore intentionally omitted from the
	SET clause, the trigger handles it and would overwrite any value
	provided anyway.

active NOTE:
	Both active (integer, legacy) and activebool (boolean, current) are
	updated together to keep them in sync. active = 1 means enabled.
*/

-- checking candidate before UPDATE
SELECT
	c.customer_id,
	c.first_name,
	c.last_name,
	c.email,
	c.address_id,
	COUNT(DISTINCT r.rental_id) AS rental_count,
	COUNT(DISTINCT p.payment_id) AS payment_count
FROM public.customer c
INNER JOIN public.rental r ON r.customer_id = c.customer_id
INNER JOIN public.payment p ON p.customer_id = c.customer_id
GROUP BY c.customer_id
HAVING COUNT(DISTINCT r.rental_id) >= 43
	AND COUNT(DISTINCT p.payment_id) >= 43
ORDER BY rental_count DESC;


BEGIN;

UPDATE public.customer
SET
	first_name = 'Tamar',
	last_name = 'Tutisani',
	email = 'tamartutisani@gmail.com',
	address_id = (SELECT MIN(address_id) FROM public.address),
	activebool = TRUE,
	active = 1
WHERE customer_id = (
	SELECT c.customer_id
	FROM public.customer c
	INNER JOIN public.rental r ON r.customer_id = c.customer_id
	INNER JOIN public.payment p ON p.customer_id = c.customer_id
	GROUP BY c.customer_id
	HAVING COUNT(DISTINCT r.rental_id) >= 43
		AND COUNT(DISTINCT p.payment_id) >= 43
	ORDER BY COUNT(DISTINCT r.rental_id) DESC
	LIMIT 1
)
RETURNING customer_id, first_name, last_name, email, address_id;

COMMIT;


-- SUBTASK 6: DELETE EXISTING RECORDS RELATED TO THIS CUSTOMER
/*
WHY A SEPARATE TRANSACTION:
	Deletions are irreversible once committed. Isolating them lets
	us verify with SELECT before committing.

IF THIS TRANSACTION FAILS:
	Both DELETEs roll back, no partial deletes occur.
	customer and inventory records are never touched.

ROLLBACK: Fully possible before COMMIT.

SAFETY & ORDER OF DELETION:
	1. payment - deleted FIRST because payment.rental_id is a FK
	             referencing rental.rental_id. Deleting rental first
	             would violate the FK constraint.
	2. rental  - deleted after payment.

Tables NOT touched: customer and inventory.
No other tables reference customer_id in this schema.

NO UNINTENDED DATA LOSS:
	Both DELETEs are scoped strictly to the customer_id of Tamar
	Tutisani, resolved dynamically. No other customers' data is affected.
*/

-- confirm row counts before deleting
SELECT
	'payment' AS source_table,
	COUNT(*) AS records_to_delete
FROM public.payment
WHERE customer_id = (
	SELECT customer_id
	FROM public.customer
	WHERE first_name = 'Tamar'
		AND last_name = 'Tutisani'
	ORDER BY customer_id
	LIMIT 1
)
UNION ALL
SELECT
	'rental' AS source_table,
	COUNT(*) AS records_to_delete
FROM public.rental
WHERE customer_id = (
	SELECT customer_id
	FROM public.customer
	WHERE first_name = 'Tamar'
		AND last_name = 'Tutisani'
	ORDER BY customer_id
	LIMIT 1
);


BEGIN;

DELETE FROM public.payment
WHERE customer_id = (
	SELECT customer_id
	FROM public.customer
	WHERE first_name = 'Tamar'
		AND last_name = 'Tutisani'
	ORDER BY customer_id
	LIMIT 1
)
RETURNING payment_id, customer_id, payment_date;

DELETE FROM public.rental
WHERE customer_id = (
	SELECT customer_id
	FROM public.customer
	WHERE first_name = 'Tamar'
		AND last_name = 'Tutisani'
	ORDER BY customer_id
	LIMIT 1
)
RETURNING rental_id, customer_id, return_date;

COMMIT;


-- SUBTASK 7: RENT FILMS AND ADD PAYMENT RECORDS
/*
WHY A SEPARATE TRANSACTION:
	Rental + payment is the business "checkout" event.
	Either all three film rentals AND their payments succeed, or none
	do. Partial commits (e.g. rental without payment) would corrupt
	data integrity.

IF THIS TRANSACTION FAILS:
	ALL 3 rentals AND all 3 payments roll back.
	No partial checkout records are left in the database.
	The customer row and inventory rows remain intact.

ROLLBACK: Fully possible before COMMIT.

INSERT...SELECT ADVANTAGE:
	customer_id, inventory_id, staff_id, and rental_id are ALL resolved
	dynamically via JOINs and subqueries. No IDs hardcoded anywhere.

CTE APPROACH (WITH ... INSERT ... RETURNING):
	A single rental INSERT handles all three films using a VALUES source.
	The RETURNING rental_id is fed directly into the payment INSERT within
	the same statement. This guarantees the rental_id FK in payment is
	always valid.

DUPLICATE PREVENTION:
	NOT EXISTS checks (inventory_id, customer_id, rental_date) to prevent
	re-inserting identical rentals on re-runs. If the rental row already
	exists, the CTE returns 0 rows for that film, so the payment INSERT
	also inserts 0 rows for it – fully idempotent.

REFERENTIAL INTEGRITY:
	inventory_id -> inventory (inserted in Subtask 4)
	customer_id  -> customer  (row with my own name)
	staff_id     -> staff     (MIN staff_id resolved dynamically)
	rental_id    -> rental    (resolved from the CTE RETURNING clause)

STORE FILTER:
	Matches the same store used in Subtask 4 via MIN(store_id).

STAFF SELECTION:
	MIN(staff_id) selected dynamically. Never hardcoded.

PAYMENT AMOUNT:
	Set to film.rental_rate.

PAYMENT DATE:
	Set in January 2017 (within the existing payment partition).
	Derived directly from rental_date carried through RETURNING –
	no re-query of public.rental needed, which also prevents the
	null payment_date error when NOT EXISTS blocks a re-run.

LIMIT 1 IN RENTAL CTE:
	A film can have multiple physical copies (inventory rows) in the
	same store. We rent exactly one copy per film. The specific copy
	chosen is non-deterministic but acceptable, all copies of the same
	film are equivalent for rental purposes.
*/

BEGIN;

WITH new_rentals AS (
	INSERT INTO public.rental (
		rental_date,
		inventory_id,
		customer_id,
		return_date,
		staff_id,
		last_update
	)
	SELECT DISTINCT ON (f.film_id)
		rental_data.rental_date,
		i.inventory_id,
		c.customer_id,
		rental_data.return_date,
		(SELECT MIN(stf.staff_id) FROM public.staff stf) AS staff_id,
		CURRENT_TIMESTAMP AS last_update
	FROM (
		VALUES
			('13 Going on 30', '2017-01-15 10:00:00'::TIMESTAMP, '2017-01-22 10:00:00'::TIMESTAMP),
			('Coherence', '2017-01-16 11:00:00'::TIMESTAMP, '2017-01-30 11:00:00'::TIMESTAMP),
			('The Prestige', '2017-01-17 12:00:00'::TIMESTAMP, '2017-02-07 12:00:00'::TIMESTAMP)
	) AS rental_data(film_title, rental_date, return_date)
	-- joining on name/id match since rental_data is a VALUES subquery with no FK column
	INNER JOIN public.film f ON lower(f.title) = lower(rental_data.film_title)
	INNER JOIN public.inventory i ON i.film_id = f.film_id
	CROSS JOIN (
		SELECT customer_id
		FROM public.customer
		WHERE first_name = 'Tamar'
			AND last_name = 'Tutisani'
		ORDER BY customer_id
		LIMIT 1
	) c
	WHERE i.store_id = (SELECT MIN(store_id) FROM public.store)
		AND NOT EXISTS (
			SELECT 1
			FROM public.rental r
			WHERE r.inventory_id = i.inventory_id
				AND r.customer_id = c.customer_id
				AND r.rental_date = rental_data.rental_date
		)
	ORDER BY f.film_id, i.inventory_id
	RETURNING rental_id, customer_id, inventory_id, rental_date
)
INSERT INTO public.payment (
	customer_id,
	staff_id,
	rental_id,
	amount,
	payment_date
)
SELECT
	nr.customer_id,
	(SELECT MIN(stf.staff_id) FROM public.staff stf) AS staff_id,
	nr.rental_id,
	f.rental_rate AS amount,
	nr.rental_date + INTERVAL '30 minutes' AS payment_date
FROM new_rentals nr
INNER JOIN public.inventory i ON i.inventory_id = nr.inventory_id
INNER JOIN public.film f ON f.film_id = i.film_id
RETURNING payment_id, rental_id, amount, payment_date;

COMMIT;