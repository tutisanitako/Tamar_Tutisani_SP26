-- SQL SELECT Task: writing queries with multiple solutions

-------------------------------------------------------

-- Part 1: Write SQL queries to retrieve the following data. 


/*
Task 1.1
The marketing team needs a list of animation movies between 2017 and 2019 
to promote family-friendly content in an upcoming season in stores. 
Show all animation movies released during this period with rate more than 1, sorted alphabetically

 how i saw business logic: film(film_id) -> film_category(film_id, category_id) -> category(category_id)

 Assumptions:
	1. "between 2017 and 2019" is interpreted as inclusive on both ends:
	   release_year BETWEEN 2017 AND 2019.
	2. "rate more than 1" means rental_rate > 1 (strictly greater than).

 Production choice: solution 1
 */

----------------------------------------
--solution 1: JOINs only
----------------------------------------
/*
INNER JOIN film_category - keeps only films that have at least one
category row assigned; uncategorised films are excluded.
INNER JOIN category - restricts to 'Animation' rows only.

Advantages:
	1. Most readable for this use-case. This approach allows the database to 
	   filter records immediately through the joins, making efficient use 
	   of existing indexes on foreign key columns.

Disadvantages:
	1. All filtering is done in a single block, adding a second genre filter
	   (e.g. also include Comedy) would require extending the WHERE clause or
	   restructuring the query, whereas a CTE would let you reuse the base set.
*/

SELECT f.title
FROM public.film f
INNER JOIN public.film_category fc ON f.film_id = fc.film_id
INNER JOIN public.category c ON fc.category_id = c.category_id
WHERE UPPER(c.name) = 'ANIMATION' AND
      f.release_year BETWEEN 2017 AND 2019 AND
      f.rental_rate > 1
ORDER BY f.title;


----------------------------------------
-- solution 2: CTE
----------------------------------------
/*
Advantages:
	1. Separates "which films are Animation" from the date/rate filter.
	   each step is independently testable and easy to extend.
Disadvantages:
	1. It takes more typing than a simple JOIN.
	2. Potentially Slower: The database has to "save" the first part 
	   of the query into its memory before finishing the second part.
	3. Poor Readability: Separating the filters into different blocks forces 
	   the reader to jump back and forth to understand how the final result is produced.
*/

WITH animated_films AS (
	SELECT 
		f.title,
		f.release_year, 
		f.rental_rate 
	FROM public.film f
	INNER JOIN public.film_category fc ON f.film_id = fc.film_id
	INNER JOIN public.category c ON fc.category_id = c.category_id
	WHERE UPPER(c.name) = 'ANIMATION'
)
SELECT af.title
FROM animated_films af
WHERE af.release_year BETWEEN 2017 AND 2019 AND
	  af.rental_rate > 1
ORDER BY af.title;


----------------------------------------
-- solution 3: Subquery
----------------------------------------
/*
Advantages:
   1. IN subquery clearly expresses "films whose ID exists
   in the Animation set".
 Disadvantages:
   1. IN with a large subquery result-set can be slower than an INNER JOIN
      because the optimiser may evaluate the subquery for each outer row.
   2. Harder to debug or extend than a CTE.
*/

SELECT f.title
FROM public.film f
WHERE f.release_year BETWEEN 2017 AND 2019 AND
	  f.rental_rate > 1 AND
	  f.film_id IN (
		  SELECT fc.film_id
		  FROM public.film_category fc 
		  INNER JOIN public.category c ON fc.category_id = c.category_id 
		  WHERE UPPER(c.name) = 'ANIMATION'
	  )
ORDER BY f.title;



/*
Task 1.2
The finance department requires a report on store performance to assess profitability 
and plan resource allocation for stores after March 2017. Calculate the revenue 
earned by each rental store after March 2017 (since April) 
(include columns: address and address2 – as one column, revenue)

how i saw business logic: 
payment(rental_id) -> rental(inventory_id) -> inventory(store_id) -> store(address_id) -> address(address_id)

Assumptions
	1. "after March 2017 (since April)" -> payment_date >= '2017-04-01'.
	2. Revenue = SUM(payment.amount).
	3. The store that owns the rented inventory copy is the revenue-earning store.
	4. address2 is concatenated with a comma separator; when address2 is NULL
	   or blank it is omitted to produce a clean display value.
	5. Querying the parent public.payment table automatically covers all
	   monthly partitions.
	6. GROUP BY includes s.store_id because store_id is the unique identifier
	   of the store entity. A store could theoretically change its address over
	   time, so grouping by address columns alone would incorrectly merge
	   distinct stores that share an address, or split one store across rows
	   if its address changed. Grouping by the ID guarantees one row per store.

Production choice: solution 1
*/

----------------------------------------
-- solution 1: JOINs only
----------------------------------------
/*
All INNER JOINs: every payment references an existing rental, every rental REFERENCES
an inventory item, and every store has an address. No rows are lost with INNER JOIN; 
LEFT JOIN would produce the same result but would be misleading given the data model.

Advantages:
	1. Direct FK traversal in a single pass; clean execution plan via hash joins.
Disadvantages:
	1. Long join chain can be harder to scan for readers unfamiliar with the schema.
*/

SELECT ad.address || COALESCE(', ' || NULLIF(TRIM(ad.address2), ''),  '') AS full_address, 
	  SUM(p.amount) AS revenue 
FROM public.payment p
INNER JOIN public.rental r ON p.rental_id = r.rental_id 
INNER JOIN public.inventory i ON r.inventory_id = i.inventory_id 
INNER JOIN public.store s ON i.store_id = s.store_id 
INNER JOIN public.address ad ON s.address_id = ad.address_id 
WHERE p.payment_date >= '2017-04-01'
GROUP BY s.store_id,
		 ad.address,
		 ad.address2
ORDER BY revenue DESC;


----------------------------------------
-- solution 2: CTE
----------------------------------------
/*
Advantages:
	1. Each logical step (filter payments, aggregate per store, attach address)
	   is named and readable on its own; easy to maintain and extend.
Disadvantages:
	1. Two materialisation points; may consume more memory than the single-pass
	   JOIN solution on large datasets.
*/

WITH payments_since2017 AS (
	SELECT p.rental_id,
		   p.amount
	FROM public.payment p
	WHERE p.payment_date >= '2017-04-01'
),
store_revenue AS (
	SELECT i.store_id,
		   SUM(ps.amount) AS revenue
	FROM payments_since2017 AS ps
	INNER JOIN public.rental r ON ps.rental_id = r.rental_id 
	INNER JOIN public.inventory i ON r.inventory_id = i.inventory_id
	GROUP BY i.store_id
)
SELECT ad.address || COALESCE(', ' || NULLIF(TRIM(ad.address2), ''),  '') AS full_address,
	   sr.revenue 
FROM store_revenue sr
INNER JOIN public.store s ON sr.store_id = s.store_id
INNER JOIN public.address ad ON s.address_id = ad.address_id 
ORDER BY revenue DESC;


----------------------------------------
-- solution 3: Subquery
----------------------------------------
/*
Advantages:
	1. No CTE dependency; portable to older SQL environments.
Disadvantages:
	1. Nested derived table harder to read and test independently.
*/

SELECT ad.address || COALESCE(', ' || NULLIF(TRIM(ad.address2), ''),  '') AS full_address,
	   store_r.revenue
FROM (
	  SELECT i.store_id, 
		     SUM(p.amount) AS revenue
	  FROM public.payment p
	  INNER JOIN public.rental r ON p.rental_id = r.rental_id
	  INNER JOIN public.inventory i ON r.inventory_id = i.inventory_id
	  WHERE p.payment_date >= '2017-04-01'
	  GROUP BY i.store_id
) AS store_r
INNER JOIN public.store s ON store_r.store_id = s.store_id
INNER JOIN public.address ad ON s.address_id = ad.address_id 
ORDER BY revenue DESC;



/*
Task 1.3
The marketing department in our stores aims to identify the most successful actors 
since 2015 to boost customer interest in their films. Show top-5 actors 
by number of movies (released since 2015) they took part in 
(columns: first_name, last_name, number_of_movies, sorted by number_of_movies in descending order)

Assumptions:
	1. "released since 2015" -> release_year >= 2015 (greater than or equal to).
	2. Actors with no qualifying films are excluded (INNER JOIN is intentional).
	3. The task says "top-5" but the dataset contains multiple actors tied at
	   the same film count for the 5th position. Since the task does not specify
	   how to handle ties, LIMIT 5 is applied with a secondary alphabetical sort
	   (first_name, last_name) to make the selection deterministic.

how i saw business logic:
actor -> film_actor(film_id, actor_id) -> film(film_id)

Production choice: solution 1
*/

----------------------------------------
-- solution 1: JOINs only
----------------------------------------
/*
INNER JOIN film_actor / film: only actors who appeared in at least one
post-2015 film are returned; actors with no qualifying credits are
excluded.

Advantages:
	1. Straightforward aggregation over indexed FK joins; efficient.
Disadvantages:
	1. LIMIT 5 is applied after a full sort of all aggregated rows.
*/

SELECT a.first_name, 
	   a.last_name, 
	   COUNT(fa.film_id) AS number_of_movies
FROM public.actor a
INNER JOIN public.film_actor fa ON a.actor_id = fa.actor_id 
INNER JOIN public.film f ON fa.film_id = f.film_id 
WHERE f.release_year >= 2015
GROUP BY a.actor_id,
		 a.first_name,
		 a.last_name 
ORDER BY number_of_movies DESC,
		 a.first_name ASC,
		 a.last_name ASC
LIMIT 5;


----------------------------------------
-- solution 2: CTE
----------------------------------------
/*
Advantages:
	1. Separates "count per actor" from "rank and limit"; each step testable.
Disadvantages:
	1. It uses up extra RAM because it has to save the entire middle step before it can even start the final step.
*/

WITH actor_movies_since2015 AS (
	SELECT ac.first_name,
		   ac.last_name,
		   COUNT(fa.film_id) AS number_of_movies
	FROM public.actor ac
	INNER JOIN public.film_actor fa ON ac.actor_id = fa.actor_id
	INNER JOIN public.film f ON fa.film_id = f.film_id 
	WHERE f.release_year >= 2015
	GROUP BY ac.actor_id,
			 ac.first_name,
			 ac.last_name
)
SELECT ams.first_name,
	   ams.last_name,
	   ams.number_of_movies
FROM actor_movies_since2015 ams
ORDER BY ams.number_of_movies DESC,
		 ams.first_name ASC,
		 ams.last_name ASC
LIMIT 5;


----------------------------------------
-- solution 3: Subquery
----------------------------------------
/*
Advantages:
	1. It keeps the counting logic (the GROUP BY and JOINs) inside a derived
	   table, keeping the outer SELECT clean: it only handles ORDER BY and LIMIT.
Disadvantages:
	1. If the same intermediate result is needed in multiple places within
	   the query, a derived table cannot be reused, a CTE would be required.
	2. The inner query cannot be executed or tested independently.
*/

SELECT movie_since2015.first_name,
	   movie_since2015.last_name,
	   movie_since2015.number_of_movies
FROM (
	  SELECT COUNT(fa.film_id) AS number_of_movies,
	  		 ac.first_name,
	  		 ac.last_name
	  FROM public.actor ac
	  INNER JOIN public.film_actor fa ON ac.actor_id = fa.actor_id 
	  INNER JOIN public.film f ON fa.film_id = f.film_id
	  WHERE f.release_year >= 2015
	  GROUP BY ac.actor_id,
	  		   ac.first_name,
	  		   ac.last_name 
) AS movie_since2015
ORDER BY number_of_movies DESC,
		 first_name ASC,
		 last_name ASC
LIMIT 5;



/*
Task 1.4
The marketing team needs to track the production trends of Drama, Travel, and Documentary films
to inform genre-specific marketing strategies. 
Show number of Drama, Travel, Documentary per year (include columns: release_year, 
number_of_drama_movies, number_of_travel_movies, number_of_documentary_movies), 
sorted by release year in descending order. Dealing with NULL values is encouraged)

Assumptions:
	1. A film belonging to Drama AND Travel is counted in both genre columns
	   (many-to-many relationship; double-counting per genre is intentional).
	2. Years that have no films in any of the three genres do not appear in
	   the result.
	3. NULL handling: COUNT(*) FILTER (WHERE ...) returns 0 (not NULL) when
	   no films of a given genre exist for a particular year, so all three
	   genre columns always show a numeric value with no additional
	   COALESCE needed.

how i see business logic: film -> film_category (film_id, category_id) -> category (category_id)

Production choice: solution 1
*/

----------------------------------------
-- solution 1: JOINs only
----------------------------------------
/*
INNER JOIN film_category / category: all category rows are scanned.
genre filtering is handled entirely by FILTER in the SELECT list.

Advantages:
	1. Genre selection has a single source of truth: only the FILTER
       conditions control which genres appear. No WHERE clause to keep
       in sync. adding a new genre column requires editing the SELECT
       list only.
	
Disadvantages:
	1. Scans all category rows before the FILTER discards non-matching
	   genres.
*/

SELECT f.release_year,
	   COUNT(*) FILTER (WHERE UPPER(c.name) = 'DRAMA') AS number_of_drama_movies,
	   COUNT(*) FILTER (WHERE UPPER(c.name) = 'TRAVEL') AS number_of_travel_movies,
	   COUNT(*) FILTER (WHERE UPPER(c.name) = 'DOCUMENTARY') AS number_of_documentary_movies
FROM public.film f 
INNER JOIN public.film_category fc ON f.film_id = fc.film_id 
INNER JOIN public.category c ON fc.category_id = c.category_id 
GROUP BY f.release_year
ORDER BY f.release_year DESC;


----------------------------------------
-- solution 2: CTE
----------------------------------------
/*
Advantages:
	1. Genre-filter step is isolated and independently testable. easy to add
	   more genres to the CTE without touching the pivot logic.
Disadvantages:
	1. Extra materialisation step.
*/

WITH genre_films AS (
	SELECT c.name AS genre,
		   f.release_year
	FROM public.film f 
	INNER JOIN public.film_category fc ON f.film_id = fc.film_id 
	INNER JOIN public.category c ON fc.category_id = c.category_id
	WHERE UPPER(c.name) IN ('DRAMA', 'TRAVEL', 'DOCUMENTARY')
)
SELECT gf.release_year,
	   COUNT(*) FILTER (WHERE UPPER(gf.genre) = 'DRAMA') AS number_of_drama_movies,
	   COUNT(*) FILTER (WHERE UPPER(gf.genre) = 'TRAVEL') AS number_of_travel_movies,
	   COUNT(*) FILTER (WHERE UPPER(gf.genre) = 'DOCUMENTARY') AS number_of_documentary_movies
FROM genre_films gf
GROUP BY gf.release_year
ORDER BY gf.release_year DESC;


----------------------------------------
-- solution 3: Subquery
----------------------------------------
/*
Advantages:
	1. Self-contained and portable across SQL environments.
Disadvantages:
	2. Inline subquery makes the outer SELECT list visually busy.
*/

SELECT genres.release_year,
	   COUNT(*) FILTER (WHERE UPPER(genres.genre) = 'DRAMA') AS number_of_drama_movies,
	   COUNT(*) FILTER (WHERE UPPER(genres.genre) = 'TRAVEL') AS number_of_travel_movies,
	   COUNT(*) FILTER (WHERE UPPER(genres.genre) = 'DOCUMENTARY') AS number_of_documentary_movies
FROM (
	  SELECT c.name AS genre,
	  		 f.release_year
	  FROM public.film f
	  INNER JOIN public.film_category fc ON f.film_id = fc.film_id
	  INNER JOIN public.category c ON fc.category_id = c.category_id
	  WHERE UPPER(c.name) IN ('DRAMA', 'TRAVEL', 'DOCUMENTARY')	  
) AS genres
GROUP BY genres.release_year
ORDER BY genres.release_year DESC;



-------------------------------------------------------

-- Part 2: Solve the following problems using SQL


/*
Task 2.1
The HR department aims to reward top-performing employees in 2017 with bonuses to recognize 
their contribution to stores revenue. Show which three employees generated the most revenue in 2017? 

Assumptions: 
	1. staff could work in several stores in a year, please indicate which store the staff worked in (the last one);
	2. if staff processed the payment then he works in the same store; 
	3. take into account only payment_date
	4. If a staff member has multiple payments at the exact same max timestamp, 
	   MIN(store_id) is used as an arbitrary tiebreaker.

staff -> store(store_id) -> inventory(inventory_id) -> rental(rental_id) -> payment(payment_id)

a JOINs-only solution is not possible for this task:
The task requires two conflicting aggregations over the same table in
the same query:
	1. SUM(amount) - needs ALL payment rows for a staff member in 2017.
	2. Last store  - needs ONLY the single latest payment row per staff.
	
A JOIN-only approach can identify the latest row using the anti-join
pattern (LEFT JOIN payment on a later date WHERE later.payment_id IS NULL),
but applying that filter retains only the latest row per staff, which
breaks SUM(amount) because all other rows are excluded by the WHERE clause.
Without a subquery or CTE to pre-aggregate one of these two concerns
before the other is applied, it is not possible to satisfy both
requirements in a single standards-compliant JOIN query.

Production choice: solution 1
*/

----------------------------------------
-- solution 1: CTE
----------------------------------------
/*
Advantages:
   1. Three named steps (revenue, last payment, last store) are each
	  independently verifiable, easiest to maintain or extend.
Disadvantages:
   1. Three CTEs each scan the payment table. may be slower than a single
	  pass on very large datasets without covering indexes.
Additional comment on JOIN condition:
	p.payment_date = lp.last_payment_date is a non-FK condition.
	This is a necessary exception to identify only the payment rows
	matching the latest timestamp per staff member.
*/

WITH revenue_2017 AS (
	SELECT p.staff_id,
		   SUM(p.amount) AS total_revenue
	FROM public.payment p
	WHERE p.payment_date >= '2017-01-01' AND
		  p.payment_date < '2018-01-01'
	GROUP BY p.staff_id
),
last_payment AS (
	SELECT p.staff_id,
		   MAX(p.payment_date) AS last_payment_date
	FROM public.payment p 
	WHERE p.payment_date >= '2017-01-01' AND
		  p.payment_date < '2018-01-01'
	GROUP BY p.staff_id
),
last_store AS (
	SELECT p.staff_id,
		   MIN(i.store_id) AS store_id
	FROM public.payment p
	INNER JOIN last_payment lp ON p.staff_id = lp.staff_id AND
								  p.payment_date = lp.last_payment_date
	INNER JOIN public.rental r ON p.rental_id = r.rental_id
	INNER JOIN public.inventory i ON r.inventory_id = i.inventory_id
	WHERE p.payment_date >= '2017-01-01' AND
		  p.payment_date < '2018-01-01'
	GROUP BY p.staff_id
)
SELECT st.first_name,
	   st.last_name,
	   rev.total_revenue,
	   ls.store_id AS last_store_id
FROM revenue_2017 rev
INNER JOIN public.staff st ON rev.staff_id = st.staff_id
INNER JOIN last_store ls ON rev.staff_id = ls.staff_id
ORDER BY rev.total_revenue DESC
LIMIT 3;


----------------------------------------
-- solution 2: Subquery
----------------------------------------
/*
Advantages:
	1. No CTE dependency.
Disadvantages:
	1. The MAX subquery recalculates for each outer row, can be expensive
	   on large payment tables without a covering index on (staff_id, payment_date).
	2. Harder to read and test in isolation compared to the CTE version.
Additional comment on JOIN condition:
	pay_agg.max_payment_date = last_pay.payment_date is a non-FK join condition.
	This is a necessary exception, we need to match the pre-aggregated max date
	from pay_agg to the exact payment row in last_pay to identify the correct
	last store. There is no FK relationship that can express this.
*/
 
SELECT st.first_name,
	   st.last_name,
	   pay_agg.total_revenue,
	   last_pay.store_id AS last_store_id
FROM public.staff st
INNER JOIN (
	SELECT p.staff_id,
		   SUM(p.amount) AS total_revenue,
		   MAX(p.payment_date) AS max_payment_date
	FROM public.payment p
	WHERE p.payment_date >= '2017-01-01' AND
		  p.payment_date < '2018-01-01'
	GROUP BY p.staff_id
) AS pay_agg ON st.staff_id = pay_agg.staff_id
INNER JOIN (
	SELECT p.staff_id,
		   p.payment_date,
		   MIN(i.store_id) AS store_id
	FROM public.payment p
	INNER JOIN public.rental r ON p.rental_id = r.rental_id
	INNER JOIN public.inventory i ON r.inventory_id = i.inventory_id
	WHERE p.payment_date >= '2017-01-01' AND
		  p.payment_date < '2018-01-01'
	GROUP BY p.staff_id,
			 p.payment_date
) AS last_pay ON st.staff_id = last_pay.staff_id AND
				 pay_agg.max_payment_date = last_pay.payment_date
ORDER BY pay_agg.total_revenue DESC
LIMIT 3;



/*
Task 2.2
The management team wants to identify the most popular movies and their target audience 
age groups to optimize marketing efforts. Show which 5 movies were rented more than 
others (number of rentals), and what's the expected age of the audience for these movies? 
To determine expected age please use 'Motion Picture Association film rating system'
 
how i saw business logic:
film(film_id) -> inventory(inventory_id) -> rental(rental_id)
 
Assumptions:
	1. "rented more than others" = highest COUNT(rental_id) per film.
	2. MPA rating -> expected age mapping:
		G -> All ages (0+)
		PG -> 0+ (parental guidance suggested)
		PG-13 -> 13+
		R -> 17+
		NC-17 -> 18+
	3. A film can have multiple inventory copies, each rental row counts as
	   one rental event regardless of which copy was rented.
 
Production choice: solution 1
*/
 
----------------------------------------
-- solution 1: JOINs only
----------------------------------------
/*
INNER JOIN inventory: films with no inventory copies are excluded.
INNER JOIN rental: inventory items never rented are excluded

Advantages:
	1. Minimal complexity.
Disadvantages:
	1. Inline CASE for expected_age is coupled to the main SELECT, adding a
	   new rating requires editing the SELECT list.
*/
 
SELECT f.title,
	   COUNT(r.rental_id) AS rental_count,
	   f.rating,
	   CASE f.rating
		   WHEN 'G'     THEN 'All ages (0+)'
		   WHEN 'PG'    THEN '0+ (parental guidance)'
		   WHEN 'PG-13' THEN '13+'
		   WHEN 'R'     THEN '17+'
		   WHEN 'NC-17' THEN '18+'
	   END AS expected_age
FROM public.film f
INNER JOIN public.inventory i ON f.film_id = i.film_id
INNER JOIN public.rental r ON i.inventory_id = r.inventory_id
GROUP BY f.film_id,
		 f.title,
		 f.rating
ORDER BY rental_count DESC,
		 f.title ASC
LIMIT 5;
 
 
----------------------------------------
-- solution 2: CTE
----------------------------------------
/*
Advantages:
	1. Rental-count aggregation is isolated from the MPA rating mapping;
	   each step is independently testable and easy to maintain.
Disadvantages:
	1. Slight materialisation overhead before LIMIT is applied.
*/
 
WITH rental_counts AS (
	SELECT i.film_id,
		   COUNT(r.rental_id) AS rental_count
	FROM public.inventory i
	INNER JOIN public.rental r ON i.inventory_id = r.inventory_id
	GROUP BY i.film_id
)
SELECT f.title,
	   rc.rental_count,
	   f.rating,
	   CASE f.rating
		   WHEN 'G'     THEN 'All ages (0+)'
		   WHEN 'PG'    THEN '0+ (parental guidance)'
		   WHEN 'PG-13' THEN '13+'
		   WHEN 'R'     THEN '17+'
		   WHEN 'NC-17' THEN '18+'
	   END AS expected_age
FROM rental_counts rc
INNER JOIN public.film f ON rc.film_id = f.film_id
ORDER BY rc.rental_count DESC,
		 f.title ASC
LIMIT 5;

 
----------------------------------------
-- solution 3: Subquery
----------------------------------------
/*
Advantages:
	1. Self-contained derived table; portable across SQL environments.
Disadvantages:
	1. CASE block must live in the outer query, increasing overall length;
	   the inner query cannot be tested independently.
*/
 
SELECT f.title,
	   film_rentals.rental_count,
	   f.rating,
	   CASE f.rating
		   WHEN 'G'     THEN 'All ages (0+)'
		   WHEN 'PG'    THEN '0+ (parental guidance)'
		   WHEN 'PG-13' THEN '13+'
		   WHEN 'R'     THEN '17+'
		   WHEN 'NC-17' THEN '18+'
	   END AS expected_age
FROM public.film f
INNER JOIN (
	SELECT i.film_id,
		   COUNT(r.rental_id) AS rental_count
	FROM public.inventory i
	INNER JOIN public.rental r ON i.inventory_id = r.inventory_id
	GROUP BY i.film_id
) AS film_rentals ON f.film_id = film_rentals.film_id
ORDER BY film_rentals.rental_count DESC,
		 f.title ASC
LIMIT 5;

 
 
 
-------------------------------------------------------
 
-- Part 3: Which actors/actresses didn't act for a longer period of time than the others? 
 

/*
Task 3 - V1
The stores' marketing team wants to analyze actors' inactivity periods to select those 
with notable career breaks for targeted promotional campaigns, highlighting their comebacks or 
consistent appearances to engage customers with nostalgic or reliable film stars.
V1: gap between the latest release_year and current year per each actor;
 
how i saw business logic:
actor(actor_id) -> film_actor(actor_id, film_id) -> film(film_id)
 
Assumptions:
	1. Gap = EXTRACT(YEAR FROM CURRENT_DATE) - MAX(release_year) per actor.
	2. Actors with no film credits are excluded (INNER JOIN is intentional).
	3. A larger gap means the actor has not appeared in a film for longer.
	4. "didn't act for a longer period than others" is interpreted as the
	   top 5 actors with the longest inactivity gap. The task does not specify
	   a cutoff, so TOP 5 was chosen as a reasonable default for a promotional
	   campaign use-case.
 
Production choice: solution 1
*/
 
----------------------------------------
-- solution 1: JOINs only
----------------------------------------
/*
INNER JOIN film_actor / film: actors with no film credits are excluded.
 
Advantages:
	1. Single-pass aggregation, no subqueries or CTEs needed.
Disadvantages:
	1. All filtering and aggregation is done in a single block. harder
	   to extend if additional columns or filters are needed compared
	   to the named CTE steps.
*/
 
SELECT a.first_name,
	   a.last_name,
	   MAX(f.release_year) AS latest_release_year,
	   EXTRACT(YEAR FROM CURRENT_DATE)::INT - MAX(f.release_year) AS years_inactive
FROM public.actor a
INNER JOIN public.film_actor fa ON a.actor_id = fa.actor_id
INNER JOIN public.film f ON fa.film_id = f.film_id
GROUP BY a.actor_id,
		 a.first_name,
		 a.last_name
ORDER BY years_inactive DESC
LIMIT 5;
 
 
----------------------------------------
-- solution 2: CTE
----------------------------------------
/*
Advantages:
	1. The "last active year per actor" step is named and reusable within
	   the same query. easy to add filters or extra columns.
Disadvantages:
	1. Extra materialisation for a straightforward single-level aggregate.
*/
 
WITH actor_last_year AS (
	SELECT a.actor_id,
		   a.first_name,
		   a.last_name,
		   MAX(f.release_year) AS latest_release_year
	FROM public.actor a
	INNER JOIN public.film_actor fa ON a.actor_id = fa.actor_id
	INNER JOIN public.film f ON fa.film_id = f.film_id
	GROUP BY a.actor_id,
			 a.first_name,
			 a.last_name
)
SELECT aly.first_name,
	   aly.last_name,
	   aly.latest_release_year,
	   EXTRACT(YEAR FROM CURRENT_DATE)::INT - aly.latest_release_year AS years_inactive
FROM actor_last_year aly
ORDER BY years_inactive DESC
LIMIT 5;
 
 
----------------------------------------
-- solution 3: Subquery
----------------------------------------
/*
Advantages:
	1. Entirely self-contained; no CTE dependency.
Disadvantages:
	1. Outer/inner separation adds verbosity without additional clarity for
	   this straightforward single-level aggregation.
*/
 
SELECT actors_year.first_name,
	   actors_year.last_name,
	   actors_year.latest_release_year,
	   EXTRACT(YEAR FROM CURRENT_DATE)::INT - actors_year.latest_release_year AS years_inactive
FROM (
	SELECT a.actor_id,
		   a.first_name,
		   a.last_name,
		   MAX(f.release_year) AS latest_release_year
	FROM public.actor a
	INNER JOIN public.film_actor fa ON a.actor_id = fa.actor_id
	INNER JOIN public.film f ON fa.film_id = f.film_id
	GROUP BY a.actor_id,
			 a.first_name,
			 a.last_name
) AS actors_year
ORDER BY years_inactive DESC
LIMIT 5;
 
 
 
/*
Task 3 - V2
V2: gaps between sequential films per each actor;
 
how i saw business logic:
For each actor, find pairs (from_year, to_year) where to_year is the
immediately next year the same actor appeared in. No window functions used.
 
Strategy:
	1. Build distinct (actor_id, release_year) pairs.
	2. Self-join to find, for each (actor, from_year), the minimum
	   release_year strictly greater than from_year for the same actor
	   -> that is the next consecutive appearance year.
	3. gap_years = to_year - from_year.
	4. MAX(gap_years) per actor = longest single career break.
 
Assumptions:
	1. Only consecutive appearance pairs are considered (no year in between).
	2. Actors who appeared in only one distinct release year are excluded
	   (INNER JOIN; no consecutive pair exists, no gap to measure).
	3. NOTE: the standard dvdrental dataset has release_year = 2006 for all
	   films, so no consecutive pairs exist and all queries return 0 rows.
	   The logic is correct for real multi-year data.
	4. "didn't act for a longer period than others" is interpreted as the
	   top 5 actors by maximum gap between any two consecutive appearances.
	   Same reasoning as V1: top 5 is a sensible default for a promotional
	   campaign; the threshold should be confirmed with the stakeholder.
 
Production choice: solution 2
*/
 
----------------------------------------
-- solution 1: JOINs only
----------------------------------------
/*
A JOINs-only solution isnt possible to do correctly for Part 3 V2.

The logic requires two levels of aggregation:
   1. GROUP BY (actor, from_year) to find MIN(next_year) and compute
      the gap to the immediately next appearance year.
   2. GROUP BY actor to find MAX(gap) across all from_year pairs.

Standard SQL does not allow nested aggregate functions like MAX(MIN(...))
within a single GROUP BY. The inner aggregation result must first be
materialised as a derived table before the outer aggregation can be
applied, which requires either a subquery or a CTE.

*/
 
 
----------------------------------------
-- solution 2: CTE
----------------------------------------
/*
Advantages:
	1. Three named steps mirror the natural reasoning:
	   (1) actor_years - distinct (actor, year) pairs,
	   (2) consec_gaps - find next year per (actor, from_year) and compute gap,
	   (3) final SELECT - aggregate and rank.
	2. Each step is independently testable; easiest to extend (e.g., expose
	   from_year and to_year columns for reporting).
Disadvantages:
	1. actor_years is materialised and then self-joined; on very large film
	   tables this doubles the memory footprint vs. a single scan.
Additional comment on JOIN condition:
	ay_next.release_year > ay_curr.release_year is a non-FK range condition
	in the ON clause. This is a necessary exception, we need to find all
	later years for the same actor, which cannot be expressed through FK
	columns alone. MIN() then narrows this down to the immediately next year.
*/
 
WITH actor_years AS (
	SELECT DISTINCT fa.actor_id,
					f.release_year
	FROM public.film_actor fa
	INNER JOIN public.film f ON fa.film_id = f.film_id
),
consec_gaps AS (
	SELECT ay_curr.actor_id,
		   ay_curr.release_year AS from_year,
		   MIN(ay_next.release_year) AS to_year,
		   MIN(ay_next.release_year) - ay_curr.release_year AS gap_years
	FROM actor_years ay_curr
	INNER JOIN actor_years ay_next ON ay_curr.actor_id = ay_next.actor_id AND
									  ay_next.release_year > ay_curr.release_year
	GROUP BY ay_curr.actor_id,
			 ay_curr.release_year
)
SELECT a.first_name,
	   a.last_name,
	   MAX(cg.gap_years) AS max_gap_years
FROM public.actor a
INNER JOIN consec_gaps cg ON a.actor_id = cg.actor_id
GROUP BY a.actor_id,
		 a.first_name,
		 a.last_name
ORDER BY max_gap_years DESC
LIMIT 5;
 
 
----------------------------------------
-- solution 3: Subquery
----------------------------------------
/*
Advantages:
	1. No CTE dependency; portable to older SQL environments.
Disadvantages:
	1. The distinct (actor, year) logic must be written twice as separate
	   inline subqueries (ay_curr and ay_next), making the query verbose.
	2. Inner derived tables cannot be queried or tested independently.
Additional comment on JOIN condition:
	ay_next.release_year > ay_curr.release_year is a non-FK range condition
	in the ON clause. This is a necessary exception, we need to find all
	later years for the same actor, which cannot be expressed through FK
	columns alone. MIN() then narrows this down to the immediately next year.
*/
 
SELECT a.first_name,
	   a.last_name,
	   MAX(gap_data.gap_years) AS max_gap_years
FROM public.actor a
INNER JOIN (
	SELECT ay_curr.actor_id,
		   MIN(ay_next.release_year) - ay_curr.release_year AS gap_years
	FROM (
		SELECT DISTINCT fa.actor_id,
						f.release_year
		FROM public.film_actor fa
		INNER JOIN public.film f ON fa.film_id = f.film_id
	) AS ay_curr
	INNER JOIN (
		SELECT DISTINCT fa.actor_id,
						f.release_year
		FROM public.film_actor fa
		INNER JOIN public.film f ON fa.film_id = f.film_id
	) AS ay_next ON ay_curr.actor_id = ay_next.actor_id AND
					ay_next.release_year > ay_curr.release_year
	GROUP BY ay_curr.actor_id,
			 ay_curr.release_year
) AS gap_data ON a.actor_id = gap_data.actor_id
GROUP BY a.actor_id,
		 a.first_name,
		 a.last_name
ORDER BY max_gap_years DESC
LIMIT 5;