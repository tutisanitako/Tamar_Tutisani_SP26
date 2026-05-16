-- Tasks: writing queries using window functions
----------------------------------------------------------------------------

----------------------------------------------------------------------------
-- TASK 1
----------------------------------------------------------------------------
/*
Create a query to produce a sales report highlighting the top customers
with the highest sales across different sales channels.

APPROACH:
1. Aggregate first (CTE channel_sales): sum amount_sold per
   (channel, customer) pair. Aggregating before applying window
   functions avoids processing duplicate transaction rows inside
   the window, making the logic cleaner.

2. Window functions applied on the aggregated CTE:
   - RANK() OVER (PARTITION BY channel_desc ORDER BY cust_amount DESC)
     is used instead of ROW_NUMBER() so that tied customers at rank 5
     both appear in the output rather than one being dropped
     arbitrarily. This is a deliberate choice: if two customers share
     the same total sales at position 5, excluding either one would
     misrepresent the ranking. The trade-off is that a channel may
     return more than 5 rows when ties exist at the boundary.
   - SUM() OVER (PARTITION BY channel_desc) computes the channel-level
     grand total directly from the aggregated CTE, eliminating the need
     for a separate self-join or subquery.

3. The outer SELECT filters rank <= 5, formats amounts and percentages,
   and orders results per channel in descending sales order.
*/

WITH channel_sales AS (
	SELECT
		ch.channel_desc,
		cu.cust_id,
		cu.cust_last_name,
		cu.cust_first_name,
		SUM(sa.amount_sold) AS cust_amount
	FROM sh.sales sa
	INNER JOIN sh.channels ch ON ch.channel_id = sa.channel_id
	INNER JOIN sh.customers cu ON cu.cust_id = sa.cust_id
	GROUP BY
		ch.channel_desc,
		cu.cust_id,
		cu.cust_last_name,
		cu.cust_first_name
),
ranked AS (
	SELECT
		channel_desc,
		cust_id,
		cust_last_name,
		cust_first_name,
		cust_amount,
		RANK() OVER (
			PARTITION BY channel_desc
			ORDER BY cust_amount DESC
		) AS cust_rank,
		SUM(cust_amount) OVER (PARTITION BY channel_desc) AS channel_total
	FROM channel_sales
)
SELECT
	channel_desc,
	cust_id,
	cust_last_name,
	cust_first_name,
	TO_CHAR(cust_amount, 'FM999,999,990.00') AS amount_sold,
	TO_CHAR(
		ROUND(cust_amount / channel_total * 100, 4),
		'FM990.0000'
	) || ' %' AS sales_percentage
FROM ranked
WHERE cust_rank <= 5
ORDER BY
	channel_desc,
	cust_amount DESC;


----------------------------------------------------------------------------
-- TASK 2
----------------------------------------------------------------------------
/*
Create a query to retrieve data for a report that displays the total sales
for all products in the Photo category in the Asian region for the year 2000. 

APPROACH:
1. The crosstab() function from the tablefunc extension is used to pivot
   quarterly rows into columns (q1..q4), producing one row per product.

2. Source query (first argument to crosstab):
   - Filters: prod_category = 'Photo', country_region = 'Asia',
     calendar_year = 2000.
   - Groups by prod_name + calendar_quarter_number to get one
     aggregated row per (product, quarter).
   - Must be ordered by prod_name first, then quarter_number, because
     crosstab() relies on this ordering to assign values to the correct
     pivot column.

3. The second argument to crosstab explicitly lists the category values
   (1, 2, 3, 4). This guarantees that all four quarter columns always
   appear in the correct order, even if a product had no sales in a
   particular quarter (which would otherwise cause crosstab to shift
   values into the wrong column).

4. COALESCE(qN, 0) in the outer SELECT converts NULL (no sales that
   quarter) to 0 so that YEAR_SUM is always arithmetically correct.

5. YEAR_SUM is computed as the simple sum of the four pivot columns.
   A window function is not used here because crosstab already
   collapses the data to exactly one row per product, there are no
   remaining raw rows to aggregate over.
*/

-- Ensure the extension is available
CREATE EXTENSION IF NOT EXISTS tablefunc;

SELECT
	prod_name AS product_name,
	ROUND(COALESCE(q1, 0), 2) AS q1,
	ROUND(COALESCE(q2, 0), 2) AS q2,
	ROUND(COALESCE(q3, 0), 2) AS q3,
	ROUND(COALESCE(q4, 0), 2) AS q4,
	ROUND(
		COALESCE(q1, 0) + COALESCE(q2, 0) +
		COALESCE(q3, 0) + COALESCE(q4, 0),
		2
	) AS year_sum
FROM crosstab(
	$$
	SELECT
		pr.prod_name,
		ti.calendar_quarter_number,
		SUM(sa.amount_sold)
	FROM sh.sales sa
	INNER JOIN sh.products pr ON pr.prod_id = sa.prod_id
	INNER JOIN sh.times ti ON ti.time_id = sa.time_id
	INNER JOIN sh.customers cu ON cu.cust_id = sa.cust_id
	INNER JOIN sh.countries co ON co.country_id = cu.country_id
	WHERE
		pr.prod_category = 'Photo' AND
		co.country_region = 'Asia' AND 
		ti.calendar_year = 2000
	GROUP BY
		pr.prod_name,
		ti.calendar_quarter_number
	ORDER BY
		pr.prod_name,
		ti.calendar_quarter_number
	$$,
	$$ VALUES (1),(2),(3),(4) $$
) AS pivot_table (
	prod_name VARCHAR(50),
	q1 NUMERIC,
	q2 NUMERIC,
	q3 NUMERIC,
	q4 NUMERIC
)
ORDER BY year_sum DESC;


----------------------------------------------------------------------------
-- TASK 3
----------------------------------------------------------------------------
/*
Create a query to generate a sales report for customers ranked in the top 300
based on total sales in the years 1998, 1999, and 2001. 

APPROACH:
1. yearly_channel_sales CTE: aggregate amount_sold per
   (year, channel, customer) for 1998, 1999, and 2001 only.
   This produces one row per (year, channel, customer) combination.

2. ranked_per_year CTE: apply RANK() OVER (PARTITION BY sale_year,
   channel_desc ORDER BY cust_year_sales DESC) to rank each customer
   within their channel for each year independently.
   RANK() is used instead of ROW_NUMBER() so that customers with
   identical sales totals at position 300 are both retained rather than
   one being dropped arbitrarily.

3. top300_all_years CTE: filter to rank <= 300, then GROUP BY
   (channel, customer) and require COUNT(DISTINCT sale_year) = 3.
   This guarantees the customer was top-300 in that channel in ALL
   three years, not just one or two of them.

4. final_sales CTE: re-join the qualifying (channel, customer) pairs
   back to the raw sales table to sum their actual purchases on that
   channel across the three target years. The year filter is repeated
   here to keep the totals consistent with the ranking scope and to
   exclude any sales from other years.
*/

WITH yearly_channel_sales AS (
	SELECT
		ti.calendar_year AS sale_year,
		ch.channel_desc,
		sa.cust_id,
		SUM(sa.amount_sold) AS cust_year_sales
	FROM sh.sales sa
	INNER JOIN sh.times ti ON ti.time_id = sa.time_id
	INNER JOIN sh.channels ch ON ch.channel_id = sa.channel_id
	WHERE ti.calendar_year IN (1998, 1999, 2001)
	GROUP BY
		ti.calendar_year,
		ch.channel_desc,
		sa.cust_id
),
ranked_per_year AS (
	SELECT
		sale_year,
		channel_desc,
		cust_id,
		cust_year_sales,
		RANK() OVER (
			PARTITION BY sale_year, channel_desc
			ORDER BY cust_year_sales DESC
		) AS rnk
	FROM yearly_channel_sales
),
top300_all_years AS (
	-- Retain only (channel, customer) pairs where the customer placed
	-- in the top 300 within that channel in every one of the three years.
	SELECT
		channel_desc,
		cust_id
	FROM ranked_per_year
	WHERE rnk <= 300
	GROUP BY
		channel_desc,
		cust_id
	HAVING COUNT(DISTINCT sale_year) = 3
),
final_sales AS (
	SELECT
		ch.channel_desc,
		sa.cust_id,
		SUM(sa.amount_sold) AS total_amount_sold
	FROM sh.sales sa
	INNER JOIN sh.channels ch ON ch.channel_id = sa.channel_id
	INNER JOIN sh.times ti ON ti.time_id = sa.time_id
	INNER JOIN top300_all_years t3 ON t3.cust_id = sa.cust_id AND 
									  t3.channel_desc = ch.channel_desc
	WHERE ti.calendar_year IN (1998, 1999, 2001)
	GROUP BY
		ch.channel_desc,
		sa.cust_id
)
SELECT
	fs.channel_desc,
	fs.cust_id,
	cu.cust_last_name,
	cu.cust_first_name,
	TO_CHAR(fs.total_amount_sold, 'FM999,999,990.00') AS amount_sold
FROM final_sales fs
INNER JOIN sh.customers cu ON cu.cust_id = fs.cust_id
ORDER BY
	fs.channel_desc,
	fs.total_amount_sold DESC;


----------------------------------------------------------------------------
-- TASK 4
----------------------------------------------------------------------------
/*
Create a query to generate a sales report for January 2000, February 2000, and
March 2000 specifically for the Europe and Americas regions.

APPROACH:
1. raw_sales CTE: join sales, times, products, customers, and countries.
   Filter to Q1 2000 (calendar_month_desc IN '2000-01'..'2000-03') and
   the two target regions (Americas, Europe).

2. windowed CTE: apply SUM() OVER (PARTITION BY calendar_month_desc,
   prod_category, country_region) to calculate the regional total for
   each (month, category, region) group.
   Using a window function here, rather than a plain GROUP BY. it keeps
   every underlying row intact, which is what the subsequent pivot step
   requires: the outer GROUP BY + CASE WHEN construct needs to see both
   an Americas row and a Europe row for the same (month, category) group
   in order to pick them into separate columns via MAX(CASE WHEN ...).
   A prior GROUP BY would collapse the data to one row per partition and
   make the pivot impossible without a self-join.

3. Outer SELECT: GROUP BY (month, category) and use
   MAX(CASE WHEN country_region = 'Americas' THEN region_cat_month_total
            ELSE 0 END)
   to pivot the two region totals into side-by-side columns.
   MAX() is safe here because every row within a (month, category,
   region) group carries the identical pre-computed window total, so
   MAX() simply picks that one value.

4. Results are ordered chronologically by month number, then
   alphabetically by product category as required.
*/

WITH raw_sales AS (
	SELECT
		ti.calendar_month_desc,
		ti.calendar_month_number,
		pr.prod_category,
		co.country_region,
		sa.amount_sold
	FROM sh.sales sa
	INNER JOIN sh.times ti ON ti.time_id = sa.time_id
	INNER JOIN sh.products pr ON pr.prod_id = sa.prod_id
	INNER JOIN sh.customers cu ON cu.cust_id = sa.cust_id
	INNER JOIN sh.countries co ON co.country_id = cu.country_id
	WHERE
		ti.calendar_month_desc IN ('2000-01', '2000-02', '2000-03')
		AND co.country_region IN ('Europe', 'Americas')
),
windowed AS (
	SELECT
		calendar_month_desc,
		calendar_month_number,
		prod_category,
		country_region,
		SUM(amount_sold) OVER (
			PARTITION BY calendar_month_desc, prod_category, country_region
		) AS region_cat_month_total
	FROM raw_sales
)
SELECT
	calendar_month_desc,
	prod_category,
	TO_CHAR(
		ROUND(MAX(CASE WHEN country_region = 'Americas'
		               THEN region_cat_month_total ELSE 0 END), 0),
		'FM999,999,990'
	) AS "Americas SALES",
	TO_CHAR(
		ROUND(MAX(CASE WHEN country_region = 'Europe'
		               THEN region_cat_month_total ELSE 0 END), 0),
		'FM999,999,990'
	) AS "Europe SALES"
FROM windowed
GROUP BY
	calendar_month_desc,
	calendar_month_number,
	prod_category
ORDER BY
	calendar_month_number,
	prod_category;