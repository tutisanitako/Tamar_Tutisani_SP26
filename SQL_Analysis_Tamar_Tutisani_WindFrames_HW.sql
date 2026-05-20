-- Tasks: writing queries using window frames
----------------------------------------------------------------------------

----------------------------------------------------------------------------
-- TASK 1
----------------------------------------------------------------------------
/*
Create a query for analyzing the annual sales data for the years 1999 to
2001, focusing on different sales channels and regions: 'Americas', 'Asia',
and 'Europe'.
*/

WITH base_sales AS (
	SELECT
		co.country_region,
		ti.calendar_year,
		ch.channel_desc,
		SUM(sa.amount_sold) AS amount_sold
	FROM sh.sales sa
	INNER JOIN sh.channels ch ON ch.channel_id = sa.channel_id
	INNER JOIN sh.customers cu ON cu.cust_id = sa.cust_id
	INNER JOIN sh.countries co ON co.country_id = cu.country_id
	INNER JOIN sh.times ti ON ti.time_id = sa.time_id
	WHERE
		LOWER(co.country_region) IN ('americas', 'asia', 'europe') AND
		ti.calendar_year IN (1998, 1999, 2000, 2001)
	GROUP BY
		co.country_region,
		ti.calendar_year,
		ch.channel_desc
),
with_pct AS (
	SELECT
		country_region,
		calendar_year,
		channel_desc,
		amount_sold,
		ROUND(
			amount_sold /
			SUM(amount_sold) OVER (
				PARTITION BY country_region, calendar_year
			) * 100,
			2
		) AS pct_by_channels
	FROM base_sales
),
with_prev AS (
	SELECT
		country_region,
		calendar_year,
		channel_desc,
		amount_sold,
		pct_by_channels,
		LAG(pct_by_channels) OVER (
			PARTITION BY country_region, channel_desc
			ORDER BY calendar_year
		) AS pct_previous_period
	FROM with_pct
)
SELECT
	country_region,
	calendar_year,
	channel_desc,
	TO_CHAR(amount_sold, 'FM999,999,999,990.00') || ' $' AS amount_sold,
	TO_CHAR(pct_by_channels, 'FM990.00') || ' %' AS "% BY CHANNELS",
	TO_CHAR(pct_previous_period, 'FM990.00') || ' %' AS "% PREVIOUS PERIOD",
	CASE
		WHEN pct_previous_period IS NULL THEN NULL
		ELSE TO_CHAR(
			ROUND(pct_by_channels - pct_previous_period, 2),
			'FM990.00'
		) || ' %'
	END AS "% DIFF"
FROM with_prev
WHERE calendar_year IN (1999, 2000, 2001)
ORDER BY
	country_region ASC,
	calendar_year ASC,
	channel_desc ASC;


----------------------------------------------------------------------------
-- TASK 2
----------------------------------------------------------------------------
/*
Generate a sales report for weeks 49, 50, and 51 of calendar year 1999.
*/

WITH daily_sales AS (
	SELECT
		ti.calendar_week_number,
		ti.time_id,
		ti.day_name,
		EXTRACT(ISODOW FROM ti.time_id)::int AS day_of_week,
		SUM(sa.amount_sold) AS sales
	FROM sh.sales sa
	INNER JOIN sh.times ti ON ti.time_id = sa.time_id
	WHERE
		ti.calendar_year = 1999 AND
		ti.calendar_week_number IN (48, 49, 50, 51, 52)
	GROUP BY
		ti.calendar_week_number,
		ti.time_id,
		ti.day_name,
		EXTRACT(ISODOW FROM ti.time_id)
),
with_neighbors AS (
	SELECT
		calendar_week_number,
		time_id,
		day_name,
		day_of_week,
		sales,
		SUM(sales) OVER (
			PARTITION BY calendar_week_number
			ORDER BY time_id
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS cum_sum,
		LAG(sales, 1) OVER (ORDER BY time_id) AS prev_1_sales,
		LAG(sales, 2) OVER (ORDER BY time_id) AS prev_2_sales,
		LEAD(sales, 1) OVER (ORDER BY time_id) AS next_1_sales,
		LEAD(sales, 2) OVER (ORDER BY time_id) AS next_2_sales
	FROM daily_sales
)
SELECT
	calendar_week_number,
	time_id,
	day_name,
	ROUND(sales, 2) AS sales,
	ROUND(cum_sum, 2) AS cum_sum,
	ROUND(
		CASE day_of_week
			WHEN 1 THEN  -- Monday: Sat + Sun + Mon + Tue = 4
				(prev_2_sales + prev_1_sales + sales + next_1_sales) / 4.0
			WHEN 5 THEN  -- Friday: Thu + Fri + Sat + Sun = 4
				(prev_1_sales + sales + next_1_sales + next_2_sales) / 4.0
			ELSE
				(prev_1_sales + sales + next_1_sales) / 3.0
		END,
		2
	) AS centered_3_day_avg
FROM with_neighbors
WHERE calendar_week_number IN (49, 50, 51)
ORDER BY
	time_id ASC;


----------------------------------------------------------------------------
-- TASK 3
----------------------------------------------------------------------------
/*
provide 3 instances of utilizing window functions that include a frame clause, using RANGE, ROWS, and GROUPS modes. 


EXAMPLE 1: ROWS mode
	Use case: 7-day rolling average of daily sales for a specific product
	category ('Electronics') in 1999, ordered by date.

Why ROWS:
	We want exactly 6 preceding rows + current row = 7 calendar days of
	data. Because multiple rows can share the same time_id (different
	products on the same day), ROWS gives us a physically fixed row count
	which is what we need for "7 most recent data points". RANGE would
	instead include ALL rows with time_id values within a date-interval
	offset, which would expand the window unpredictably when there are
	multiple rows per date. GROUPS would count distinct peer groups
	(dates), which could include more than 7 rows if any date has
	multiple products.

Frame: ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
*/

WITH daily_category_sales AS (
	SELECT
		ti.time_id,
		ti.day_name,
		pr.prod_category,
		SUM(sa.amount_sold) AS daily_sales
	FROM sh.sales sa
	INNER JOIN sh.times ti ON ti.time_id = sa.time_id
	INNER JOIN sh.products pr ON pr.prod_id = sa.prod_id
	WHERE
		ti.calendar_year = 1999 AND
		pr.prod_category = 'Electronics'
	GROUP BY
		ti.time_id,
		ti.day_name,
		pr.prod_category
)
SELECT
	time_id,
	day_name,
	prod_category,
	ROUND(daily_sales, 2) AS daily_sales,
	ROUND(
		AVG(daily_sales) OVER (
			ORDER BY time_id
			ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
		),
		2
	) AS rolling_7day_avg
FROM daily_category_sales
ORDER BY time_id ASC
LIMIT 30;


/*
EXAMPLE 2: RANGE mode
	Use case: for each sale transaction, compute the total sales amount
	by all transactions in the same calendar year whose amount_sold falls
	within ±500 of the current row's amount_sold. This creates a
	"value-neighborhood" aggregation.

Why RANGE:
	RANGE operates on the logical values of the ORDER BY column, not on
	physical row offsets. Here we want to sum all rows whose amount_sold
	is within [current - 500, current + 500]. This value-based sliding
	window is exactly what RANGE BETWEEN <numeric offset> PRECEDING AND
	<numeric offset> FOLLOWING does. ROWS would give a fixed row count
	(not related to the actual amounts), and GROUPS would group by
	exact duplicate values (too granular for a ±500 range).

  Frame: RANGE BETWEEN 500 PRECEDING AND 500 FOLLOWING
  (requires numeric ORDER BY column, supported in PostgreSQL)
*/

SELECT
	sa.prod_id,
	ti.calendar_year,
	sa.amount_sold,
	ROUND(
		SUM(sa.amount_sold) OVER (
			PARTITION BY ti.calendar_year
			ORDER BY sa.amount_sold
			RANGE BETWEEN 500 PRECEDING AND 500 FOLLOWING
		),
		2
	) AS neighborhood_total_500
FROM sh.sales sa
INNER JOIN sh.times ti ON ti.time_id = sa.time_id
WHERE ti.calendar_year = 1999
ORDER BY
	ti.calendar_year ASC,
	sa.amount_sold ASC
LIMIT 30;


/*
EXAMPLE 3: GROUPS mode
	Use case: for each product, show a running sum of quarterly revenue
	where the window spans from the beginning of the year up to and
	including all rows that belong to the same calendar quarter (peer group)
	as the current row. This means every row within the same quarter sees
	the same cumulative total (the quarter is fully "closed" before moving
	on).

Why GROUPS:
	GROUPS counts distinct ORDER BY peer groups, not physical rows or
	value offsets. Two rows with the same calendar_quarter_number are
	in the same peer group. Using GROUPS BETWEEN UNBOUNDED PRECEDING
	AND CURRENT ROW means: include all groups from the start through
	the group that the current row belongs to. The key difference from
	ROWS: with ROWS, within a quarter each row would see a different
	partial cumulative (row by row). With GROUPS, all rows in the same
	quarter see the complete quarter total accumulated up to that group,
	giving a clean "quarter-closed" running total. RANGE would require
	a numeric/date offset, which does not directly map to "quarter
	boundaries" without extra arithmetic.

Frame: GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
*/

WITH product_quarter_sales AS (
	SELECT
		pr.prod_id,
		pr.prod_name,
		pr.prod_category,
		ti.calendar_year,
		ti.calendar_quarter_number,
		SUM(sa.amount_sold) AS quarter_sales
	FROM sh.sales sa
	INNER JOIN sh.times ti ON ti.time_id = sa.time_id
	INNER JOIN sh.products pr ON pr.prod_id = sa.prod_id
	WHERE
		ti.calendar_year = 1999 AND
		pr.prod_category = 'Electronics'
	GROUP BY
		pr.prod_id,
		pr.prod_name,
		pr.prod_category,
		ti.calendar_year,
		ti.calendar_quarter_number
)
SELECT
	prod_id,
	prod_name,
	prod_category,
	calendar_year,
	calendar_quarter_number,
	ROUND(quarter_sales, 2) AS quarter_sales,
	ROUND(
		SUM(quarter_sales) OVER (
			PARTITION BY prod_id, calendar_year
			ORDER BY calendar_quarter_number
			GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		),
		2
	) AS cumulative_ytd_sales
FROM product_quarter_sales
ORDER BY
	prod_id ASC,
	calendar_quarter_number ASC;