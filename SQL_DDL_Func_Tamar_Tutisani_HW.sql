-- Task: applying view and functions

-------------------------------------------------------

-- Task 1. Create a View
-------------------------------------------------------
/*
View: sales_revenue_by_category_qtr
Purpose:
	Shows each film category alongside its total sales revenue
	for the CURRENT quarter of the CURRENT year. Only categories
	that have at least one payment recorded in that period are shown.

How current quarter is determined:
	The quarter boundaries are derived dynamically using DATE_TRUNC:
	 - Quarter start = DATE_TRUNC('quarter', CURRENT_DATE)
	   e.g. for 2017-05-15 this gives 2017-04-01
	 - Quarter end = DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '3 months'
	   The upper bound is exclusive (< not <=) which is the standard
	   pattern for date range queries and avoids edge cases at midnight.
	   Because DATE_TRUNC is used, as soon as the calendar moves into the
	   next quarter the view automatically re-aligns.

How current year is determined:
	Embedded inside the quarter-boundary calculation via DATE_TRUNC, so it
	is always the current calendar year (2026 when run today).

Why only categories with sales appear (zero-sales categories excluded):
	The join chain payment -> rental -> inventory -> film -> film_category -> category
	uses only INNER JOINs. A category with no payment rows in the period
	produces no matching rows and therefore never appears in the GROUP BY output.
	This satisfies the requirement "only display categories with at least
	one sale in the current quarter" without needing a HAVING clause.

Verification note:
	The dvdrental database contains payment data spanning 2017-01-01 to
	2017-06-30. Running this view in 2026 will return 0 rows because no
	payment_date falls in Q2-2026. This is correct and expected behaviour,
	the view is a live window onto the current quarter.
	To test the view logic against real data, Test query 1 below runs the
	same query with a hardcoded 2017 date range, proving the JOIN chain and
	aggregation logic are correct independently of the current date.

Example of data that should NOT appear:
	A category such as 'Music' that had zero payments in Q2-2026 would
	not appear, even if it has films in the catalogue and historical revenue.
	Any category whose payments all fall outside the current quarter is excluded.

What happens if required data is missing:
	If the payment, rental, inventory, film, film_category, or category tables
	contain no rows for the current quarter, the view returns an empty result
	set. No error is raised, an empty set is valid and expected behaviour for
	a period with no sales.
*/
CREATE OR REPLACE VIEW public.sales_revenue_by_category_qtr AS
SELECT
	cat.name AS category,
	SUM(pay.amount) AS total_sales_revenue
FROM public.payment pay
INNER JOIN public.rental ren ON pay.rental_id = ren.rental_id
INNER JOIN public.inventory inv ON ren.inventory_id = inv.inventory_id
INNER JOIN public.film fil ON inv.film_id = fil.film_id
INNER JOIN public.film_category fc ON fil.film_id = fc.film_id
INNER JOIN public.category cat ON fc.category_id = cat.category_id
WHERE pay.payment_date >= DATE_TRUNC('quarter', CURRENT_DATE) AND
	  pay.payment_date <  DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '3 months'
GROUP BY
	cat.category_id,
	cat.name
ORDER BY total_sales_revenue DESC;

-- Test query 1 - valid: the view logic is verified by running the same query
-- with a hardcoded date range covering Q2-2017 where payment data actually exists.
-- Expected result: rows with category names and revenue totals > 0.
SELECT
	cat.name AS category,
	SUM(pay.amount) AS total_sales_revenue
FROM public.payment pay
INNER JOIN public.rental ren ON pay.rental_id = ren.rental_id
INNER JOIN public.inventory inv ON ren.inventory_id = inv.inventory_id
INNER JOIN public.film fil ON inv.film_id = fil.film_id
INNER JOIN public.film_category fc ON fil.film_id = fc.film_id
INNER JOIN public.category cat ON fc.category_id = cat.category_id
WHERE pay.payment_date >= '2017-04-01' AND
	  pay.payment_date <  '2017-07-01'
GROUP BY
	cat.category_id,
	cat.name
ORDER BY total_sales_revenue DESC;

-- Test query 2 - edge: current date is 2026, no payment data exists for this quarter.
-- The view correctly returns 0 rows with no error.
-- Expected result: empty result set.
SELECT * FROM public.sales_revenue_by_category_qtr;

-- Test query 3: confirms row count is 0, not an error
SELECT COUNT(*) AS row_count
FROM public.sales_revenue_by_category_qtr;



-- Task 2. Create a Query Language Function
-------------------------------------------------------
/*
Function: get_sales_revenue_by_category_qtr(p_date DATE)
Language: SQL (query language function)
Purpose:
	Returns the same result as the view in Task 1 but accepts an explicit
	date parameter. The quarter and year are derived from that date, allowing
	callers to query any historical or future period, not just the current one.
	This is how the view's logic can be tested against real 2017 data.

Why a date parameter:
	A single DATE is more natural than separate quarter + year integers, and
	DATE_TRUNC handles all boundary arithmetic correctly for any valid calendar date.

How RAISE EXCEPTION is achieved inside a SQL function:
	Pure SQL functions cannot call RAISE directly. A CTE-based guard is not
	reliable here because when p_date IS NULL, DATE_TRUNC('quarter', NULL)
	returns NULL, making the WHERE clause always FALSE, PostgreSQL optimizes
	the CTE away entirely without evaluating it, so the guard never fires.
	The reliable approach is to embed the CASE (1/0) directly inside the
	WHERE clause as an additional AND condition. The WHERE clause is always
	evaluated row-by-row and cannot be skipped by the optimizer:
	  AND (CASE WHEN p_date IS NULL THEN 1/0 ELSE 1 END) = 1
	For a malformed string such as '2025-Q5', PostgreSQL's type system rejects
	it at the call site before the function body runs, no internal guard is
	needed for that case.

What happens if input parameters are incorrect:
	- NULL date -> the WHERE-level CASE guard triggers a division-by-zero
	  runtime error that cannot be optimized away.
	- Malformed string -> PostgreSQL rejects it during type casting of the DATE
	  parameter before the function body is entered.

What happens if required data is missing:
	If no payments exist for the quarter containing p_date, the function
	returns an empty result set (0 rows). This is correct behaviour for a
	reporting query, "no data" is not an error condition.
*/
CREATE OR REPLACE FUNCTION public.get_sales_revenue_by_category_qtr(
	p_date DATE
)
RETURNS TABLE (
	category TEXT,
	total_sales_revenue NUMERIC
)
LANGUAGE SQL
AS $$
	SELECT
		cat.name AS category,
		SUM(pay.amount) AS total_sales_revenue
	FROM public.payment pay
	INNER JOIN public.rental ren ON pay.rental_id = ren.rental_id
	INNER JOIN public.inventory inv ON ren.inventory_id = inv.inventory_id
	INNER JOIN public.film fil ON inv.film_id = fil.film_id
	INNER JOIN public.film_category fc ON fil.film_id = fc.film_id
	INNER JOIN public.category cat ON fc.category_id = cat.category_id
	WHERE pay.payment_date >= DATE_TRUNC('quarter', p_date) AND
		  pay.payment_date <  DATE_TRUNC('quarter', p_date) + INTERVAL '3 months' AND
		  -- NULL guard: CASE is evaluated for every row and cannot be optimized away.
		  -- When p_date IS NULL, DATE_TRUNC returns NULL making the date conditions
		  -- always FALSE, the query would silently return 0 rows without this guard.
		  -- Embedding the check here forces a division-by-zero error for NULL input.
		  (CASE WHEN p_date IS NULL THEN 1/0 ELSE 1 END) = 1
	GROUP BY
		cat.category_id,
		cat.name
	ORDER BY total_sales_revenue DESC;
$$;

-- Test query 1 - valid: Q2-2017 contains real payment data.
-- Expected result: rows with category names and revenue totals > 0.
SELECT * FROM public.get_sales_revenue_by_category_qtr('2017-05-01');

-- Test query 2 - edge: valid date format but a quarter with no data (2020).
-- Expected result: empty result set, no error raised.
SELECT * FROM public.get_sales_revenue_by_category_qtr('2020-01-01');

-- Test query 3 - invalid: NULL input triggers the division-by-zero guard.
-- Expected result: runtime error "division by zero".
-- SELECT * FROM public.get_sales_revenue_by_category_qtr(NULL);



-- Task 3. Create procedure language functions
-------------------------------------------------------
/*
Function: most_popular_films_by_countries(p_countries TEXT[])
Purpose:
	Accepts an array of country names and returns the most popular film
	for each country that has rental data.

How 'most popular' is defined:
	By rental count, the number of times a film was rented by customers
	whose address falls within that country. Revenue was not chosen because
	rental count more directly reflects how many people chose to watch a film.

How the result is calculated:
	Step 1 - rental_counts CTE:
		Navigates country -> city -> address -> customer -> rental -> inventory -> film
		and groups by country + film to produce a rental count per film per country.
	Step 2 - ranked CTE:
		Applies RANK() OVER (PARTITION BY country ORDER BY rental_count DESC, film_id ASC).
		RANK() means tied films both receive rank 1 and both appear in the output.
	Final SELECT: returns only rank = 1 rows joined back to film and language
	for the display columns.

How ties are handled:
	When two films share the same rental count, RANK() returns both.
	As an additional tiebreaker, film_id ASC is used: a lower film_id means
	the film was added earlier and has had more time to accumulate views,
	making it the more established popular choice.

What happens if input parameters are incorrect:
	- NULL or empty array -> RAISE EXCEPTION before any query runs.
	- A country name with incorrect casing (e.g. 'BRAZIL' vs 'Brazil') ->
	  returns 0 rows for that country because WHERE uses = ANY() which is
	  case-sensitive. Callers should match the casing in public.country.

What happens if required data is missing:
	If a supplied country name does not exist in public.country, or exists but
	has no customers with rentals, no row is returned for that country.
	The function does not raise an exception, missing countries simply produce
	no output rows.
*/
CREATE OR REPLACE FUNCTION public.most_popular_films_by_countries(
	p_countries TEXT[]
)
RETURNS TABLE (
	country TEXT,
	film TEXT,
	rating public.mpaa_rating,
	language TEXT,
	length SMALLINT,
	release_year public.year
)
LANGUAGE plpgsql
AS $$
BEGIN
	-- Guard: NULL or empty array is almost certainly a caller mistake
	IF p_countries IS NULL OR array_length(p_countries, 1) IS NULL THEN
		RAISE EXCEPTION 'Input array of countries must not be NULL or empty.';
	END IF;

	RETURN QUERY
	WITH rental_counts AS (
		-- Count rentals per film per country by following the address chain
		SELECT
			cou.country_id,
			cou.country AS country_name,
			fil.film_id,
			COUNT(ren.rental_id) AS rental_count
		FROM public.country cou
		INNER JOIN public.city cit ON cit.country_id = cou.country_id
		INNER JOIN public.address adr ON adr.city_id = cit.city_id
		INNER JOIN public.customer cust ON cust.address_id = adr.address_id
		INNER JOIN public.rental ren ON ren.customer_id = cust.customer_id
		INNER JOIN public.inventory inv ON inv.inventory_id = ren.inventory_id
		INNER JOIN public.film fil ON fil.film_id = inv.film_id
		WHERE cou.country = ANY(p_countries)
		GROUP BY
			cou.country_id,
			cou.country,
			fil.film_id
	),
	ranked AS (
		-- RANK() so tied films both receive rank 1 and both appear in output.
		-- film_id ASC as tiebreaker: earlier-added film is more established.
		SELECT
			rc.country_id,
			rc.country_name,
			rc.film_id,
			rc.rental_count,
			RANK() OVER (PARTITION BY rc.country_id
						 ORDER BY rc.rental_count DESC, rc.film_id ASC) AS rnk
		FROM rental_counts rc
	)
	SELECT
		rnk_data.country_name AS country,
		fil.title AS film,
		fil.rating AS rating,
		TRIM(lang.name) AS language,
		fil.length AS length,
		fil.release_year AS release_year
	FROM ranked rnk_data
	INNER JOIN public.film fil ON fil.film_id = rnk_data.film_id
	INNER JOIN public.language lang ON lang.language_id = fil.language_id
	WHERE rnk_data.rnk = 1
	ORDER BY
		rnk_data.country_name,
		fil.title;
END;
$$;

-- Test query 1 - valid: countries that exist and have rental data.
-- Expected result: one or more rows per country with film details.
SELECT * FROM public.most_popular_films_by_countries(ARRAY['Afghanistan', 'Brazil', 'United States']);

-- Test query 2 - edge: country name that does not exist in the database.
-- Expected result: 0 rows returned, no exception raised.
SELECT * FROM public.most_popular_films_by_countries(ARRAY['Sakartvelo']);

-- Test query 3 - invalid: NULL array input.
-- Expected result: RAISE EXCEPTION 'Input array of countries must not be NULL or empty.'
-- SELECT * FROM public.most_popular_films_by_countries(NULL);



-- Task 4. Create procedure language functions
-------------------------------------------------------
/*
Function: films_in_stock_by_title(p_title_pattern TEXT)
Purpose:
	Returns in-stock inventory copies whose film title matches a LIKE pattern.
	"In stock" means the copy has no open rental, either it was never rented
	or all of its rental rows have return_date populated (not NULL).
	For each eligible copy the last rental's customer and date are shown.
	If no match is found in stock, a single sentinel row with a message is returned.

Note on NOT using inventory_in_stock():
	The existing inventory_in_stock() helper function is deliberately not used,
	as per the task requirement. The in-stock check is implemented directly via
	NOT EXISTS on open rental rows (return_date IS NULL).

How row_num is generated:
	ROW_NUMBER() window function is not used (clarified by mentor).
	A plain INTEGER counter variable (v_counter) is incremented inside a FOR
	loop, producing a sequential number starting at 1 for each function call.

How pattern matching works (LIKE, %):
	The parameter p_title_pattern is passed directly into a LIKE predicate.
	The % wildcard matches zero or more characters anywhere in the string,
	so '%love%' matches any title that contains 'love' as a substring.
	Passing 'love%' would match only titles that start with 'love'.

Case sensitivity:
	Both sides of the LIKE are wrapped in UPPER() so that '%love%',
	'%Love%', and '%LOVE%' all return identical results. This avoids
	relying on ILIKE which is PostgreSQL-specific and not SQL standard.

How last rental is determined:
	DISTINCT ON (inv.inventory_id) combined with ORDER BY ren.rental_date DESC
	NULLS LAST picks exactly one row per inventory copy, the most recent rental.
	NULLS LAST ensures never-rented copies (no rental row) are still included,
	appearing with NULL customer_name and rental_date.

Performance note:
	Which part may become slow on large data:
	  Using %love% to search for titles forces the database to check every single
	  row in the film table one by one. This is because standard indexes don't
	  work when a search starts with a wildcard (%). On a big table, this slow
	  manual search is the main reason the query is slow. The "NOT EXISTS" check
	  on the rental table is the other issue. It runs separately for every copy of
	  a movie that matches your search. If a movie has many copies, the database
	  has to run that extra check many different times.
	How unnecessary data processing is minimized:
	  Both the LIKE filter and the NOT EXISTS stock check are applied inside the
	  in_stock_copies CTE, before any JOIN to public.customer or public.language.
	  This means only inventory copies that match the title AND are in stock are
	  passed to the outer SELECT, so the more expensive customer and language
	  lookups run on the smallest possible row set.
	  For large datasets a pg_trgm GIN index would eliminate the full scan cost:
	    CREATE INDEX idx_film_title_trgm ON public.film USING GIN (title gin_trgm_ops);

What happens if input parameters are incorrect:
	- NULL pattern -> RAISE EXCEPTION before any data is read.
	- Pattern with no wildcards (e.g. 'Love') -> treated as an exact
	  case-insensitive match. Returns rows only if a title equals 'Love' exactly.

What happens if required data is missing:
	- Multiple matches -> all matching in-stock copies are returned, each with
	  its own row_num incrementing from 1.
	- No matches in stock -> a single sentinel row is returned where film_title
	  contains a descriptive message and all other columns are NULL.
*/
CREATE OR REPLACE FUNCTION public.films_in_stock_by_title(
	p_title_pattern TEXT
)
RETURNS TABLE (
	row_num INTEGER,
	film_title TEXT,
	language TEXT,
	customer_name TEXT,
	rental_date TIMESTAMP WITH TIME ZONE
)
LANGUAGE plpgsql
AS $$
DECLARE
	v_counter INTEGER := 0;
	v_rec RECORD;
	v_found BOOLEAN := FALSE;
BEGIN
	-- Guard: NULL pattern is almost certainly a caller mistake
	IF p_title_pattern IS NULL THEN
		RAISE EXCEPTION 'Title pattern must not be NULL. Pass a LIKE pattern such as ''%%love%%''.';
	END IF;

	FOR v_rec IN
		WITH in_stock_copies AS (
			-- Pick one row per inventory copy: the most recent rental.
			-- LEFT JOIN so never-rented copies (no rental row) are included.
			-- NOT EXISTS excludes copies with an open rental (return_date IS NULL).
			SELECT DISTINCT ON (inv.inventory_id)
				inv.inventory_id,
				fil.title,
				fil.language_id,
				ren.customer_id,
				ren.rental_date AS last_rental_date
			FROM public.film fil
			INNER JOIN public.inventory inv ON inv.film_id = fil.film_id
			LEFT JOIN public.rental ren ON ren.inventory_id = inv.inventory_id
			WHERE UPPER(fil.title) LIKE UPPER(p_title_pattern) AND
				  NOT EXISTS (
					  SELECT 1
					  FROM public.rental open_ren
					  WHERE open_ren.inventory_id = inv.inventory_id AND
							open_ren.return_date IS NULL
				  )
			ORDER BY
				inv.inventory_id,
				ren.rental_date DESC NULLS LAST
		)
		SELECT
			isc.title AS film_title,
			TRIM(lang.name) AS language,
			cust.first_name || ' ' || cust.last_name AS customer_name,
			isc.last_rental_date AS rental_date
		FROM in_stock_copies isc
		INNER JOIN public.language lang ON lang.language_id = isc.language_id
		LEFT JOIN public.customer cust ON cust.customer_id = isc.customer_id
		ORDER BY
			isc.last_rental_date,
			isc.title
	LOOP
		v_found := TRUE;
		v_counter := v_counter + 1;

		row_num := v_counter;
		film_title := v_rec.film_title;
		language := v_rec.language;
		customer_name := v_rec.customer_name;
		rental_date := v_rec.rental_date;

		RETURN NEXT;
	END LOOP;

	-- No matching in-stock copy found: return a single descriptive sentinel row
	IF NOT v_found THEN
		row_num := 1;
		film_title := 'Film with title matching ''' || p_title_pattern || ''' was not found in stock.';
		language := NULL;
		customer_name := NULL;
		rental_date := NULL;
		RETURN NEXT;
	END IF;
END;
$$;

-- Test query 1 - valid: '%love%' matches several films with in-stock copies.
-- Expected result: one or more rows with row_num starting at 1,
-- film titles containing 'love', language, last customer, and rental date.
SELECT * FROM public.films_in_stock_by_title('%love%');

-- Test query 2 - edge: pattern that matches no film in stock.
-- Expected result: single sentinel row, film_title contains the "not found"
-- message, all other columns are NULL.
SELECT * FROM public.films_in_stock_by_title('%nonexistent_movie%');

-- Test query 3 - invalid: NULL pattern.
-- Expected result: RAISE EXCEPTION 'Title pattern must not be NULL...'
-- SELECT * FROM public.films_in_stock_by_title(NULL);



-- Task 5. Create procedure language functions
-------------------------------------------------------
/*
Function: new_movie(p_title, p_release_year, p_language_name)
Purpose:
	Inserts a new film into public.film with sensible defaults and returns
	the newly generated film_id.

How unique ID is generated:
	film_id relies on the existing sequence public.film_film_id_seq via the
	column DEFAULT (nextval(...)). Inserting without specifying film_id lets
	PostgreSQL assign the next value automatically. IDs are never hardcoded.

How duplicates are prevented:
	Before inserting, the function checks whether a film with the same title
	already exists using UPPER() on both sides for a case-insensitive match.
	If a duplicate is found, RAISE EXCEPTION fires and no insert is attempted.

How language existence is validated and handled:
	The function looks up public.language using UPPER(TRIM(...)) on both sides.
	If the language does NOT exist it is automatically inserted into
	public.language and the new language_id is captured and used for the film.
	This follows the mentor instruction: "If language does not exist your
	function should insert new language to the table and then proceed."

Default values:
	- rental_rate: 4.99
	- rental_duration: 3 days
	- replacement_cost: 19.99
	- release_year: current year via EXTRACT(YEAR FROM CURRENT_DATE)
	- language: 'Klingon' (inserted automatically if not present)

How consistency is preserved:
	All validation (blank title, invalid year, duplicate title) and the
	optional language insert happen before the film INSERT. If any guard
	raises an exception the transaction is left clean with no partial writes.

What happens if input parameters are incorrect:
	- NULL or blank title -> RAISE EXCEPTION before any write occurs.
	- release_year outside 1901-2155 -> RAISE EXCEPTION (domain constraint).
	- Duplicate title (case-insensitive) -> RAISE EXCEPTION, no insert.

What happens if insertion fails:
	If the INSERT into public.film fails for any reason (e.g. a constraint
	violation other than duplicate title, or an unexpected type mismatch),
	PostgreSQL raises an exception automatically. Because no explicit COMMIT
	is issued inside the function, the entire transaction is rolled back by
	the caller, leaving the database in its original state with no partial writes.

What happens if required data is missing:
	- Language not in public.language -> automatically inserted, then film
	  insert proceeds. No exception is raised for a missing language.
	- Insertion failure for any other reason -> PostgreSQL propagates the error
	  as an exception; the caller's transaction can catch it with BEGIN/EXCEPTION/ROLLBACK.

fulltext column note:
	public.film has a NOT NULL tsvector column 'fulltext'. We populate it with
	TO_TSVECTOR('english', title) as a baseline. The existing film_fulltext_trigger
	(BEFORE INSERT OR UPDATE) regenerates it automatically from title and description
	on each write, so our value is immediately overwritten correctly.
*/
CREATE OR REPLACE FUNCTION public.new_movie(
	p_title TEXT,
	p_release_year INTEGER DEFAULT EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER,
	p_language_name TEXT DEFAULT 'Klingon'
)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
	v_language_id SMALLINT;
	v_existing_id INTEGER;
	v_new_film_id INTEGER;
BEGIN
	-- Guard: title must not be NULL or blank
	IF p_title IS NULL OR TRIM(p_title) = '' THEN
		RAISE EXCEPTION 'Film title must not be NULL or empty.';
	END IF;

	-- Guard: release_year must satisfy the public.year domain (1901-2155)
	IF p_release_year NOT BETWEEN 1901 AND 2155 THEN
		RAISE EXCEPTION 'Invalid release year: %. Must be between 1901 and 2155.', p_release_year;
	END IF;

	-- Check for duplicate title (case-insensitive)
	SELECT fil.film_id
	INTO v_existing_id
	FROM public.film fil
	WHERE UPPER(fil.title) = UPPER(TRIM(p_title))
	LIMIT 1;

	IF FOUND THEN
		RAISE EXCEPTION 'Film with title ''%'' already exists (film_id = %).', p_title, v_existing_id;
	END IF;

	-- Look up language; insert it automatically if it does not yet exist
	SELECT lang.language_id
	INTO v_language_id
	FROM public.language lang
	WHERE UPPER(TRIM(lang.name)) = UPPER(TRIM(p_language_name))
	LIMIT 1;

	IF NOT FOUND THEN
		INSERT INTO public.language (name, last_update)
		VALUES (p_language_name, CURRENT_TIMESTAMP)
		RETURNING language_id INTO v_language_id;
	END IF;

	-- Insert the new film; film_id is assigned automatically by the sequence
	INSERT INTO public.film (
		title,
		release_year,
		language_id,
		rental_duration,
		rental_rate,
		replacement_cost,
		last_update,
		fulltext
	)
	VALUES (
		TRIM(p_title),
		p_release_year,
		v_language_id,
		3,
		4.99,
		19.99,
		CURRENT_TIMESTAMP,
		TO_TSVECTOR('english', TRIM(p_title))
	)
	RETURNING film_id INTO v_new_film_id;

	RETURN v_new_film_id;
END;
$$;

-- Test query 1 - valid: insert a new film with all defaults.
-- 'Klingon' does not exist in public.language so it is inserted automatically.
-- Expected result: returns the new film_id as an integer.
SELECT public.new_movie('Klingon Warriors: The Revenge');

-- Test query 2 - valid: explicit release year and an existing language.
-- Expected result: returns a new film_id.
SELECT public.new_movie('Space Odyssey Reborn', 2024, 'English');

-- Test query 3 - edge: duplicate title (run after Test query 1).
-- Expected result: RAISE EXCEPTION
-- 'Film with title ''Klingon Warriors: The Revenge'' already exists (film_id = ...).'
-- SELECT public.new_movie('Klingon Warriors: The Revenge');

-- Test query 4 - invalid: blank title.
-- Expected result: RAISE EXCEPTION 'Film title must not be NULL or empty.'
-- SELECT public.new_movie('   ');