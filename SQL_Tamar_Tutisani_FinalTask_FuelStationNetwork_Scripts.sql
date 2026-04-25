-- Final Task
-----------------------------------------------------------------------------


-----------------------------------------------------------------------------
-- PART 0: DATABASE & SCHEMA SETUP
-----------------------------------------------------------------------------

--CREATE DATABASE fuel_station_network
--	ENCODING 'UTF8'
--	LC_COLLATE 'en_US.UTF-8'
--	LC_CTYPE 'en_US.UTF-8'
--	TEMPLATE template0;

-- Drop and recreate the schema to allow a clean re-run.
DROP SCHEMA IF EXISTS fuel_network CASCADE;
CREATE SCHEMA fuel_network;

COMMENT ON SCHEMA fuel_network IS
	'Physical schema for the Fuel Station Network domain. '
	'Contains all operational tables, functions, and views.';

SET search_path TO fuel_network;


-----------------------------------------------------------------------------
-- PART 1: DDL: TABLES & CONSTRAINTS
-----------------------------------------------------------------------------
/*
Creation order: parent tables before child tables to satisfy FK constraints.

Lookup tables (no FKs):
	1. regions
	2. fuel_types
	3. employee_roles
	4. suppliers

Master tables (FK -> lookups):
	5. stations (-> regions)
	6. customers (no FK, independent master)

Bridge / child tables (FK -> masters):
	7. station_fuel (-> stations, fuel_types)
	8. fuel_prices (-> stations, fuel_types)
	9. employees (-> stations, employee_roles)

Transaction tables (FK -> masters + bridge):
	10. deliveries (-> stations, suppliers)
	11. delivery_items (-> deliveries, fuel_types)
	12. fuel_sales (-> stations, fuel_types, employees, customers)
*/

-- -----------------------------------------------------------------------------
-- TABLE 1: regions
-- Type: Lookup
-- Purpose: Geographic regions that group stations, e.g. Tbilisi, Adjara.
-- -----------------------------------------------------------------------------
CREATE TABLE fuel_network.regions (
	region_id SERIAL NOT NULL,
	region_name VARCHAR(100) NOT NULL,

	CONSTRAINT PK_regions_region_id PRIMARY KEY (region_id),
	CONSTRAINT UQ_regions_region_name UNIQUE (region_name)
);


-- -----------------------------------------------------------------------------
-- TABLE 2: fuel_types
-- Type: Lookup
-- Purpose: Reference catalogue of fuel products sold across the network.
--			Kept as a separate table so adding a new product type (e.g. H2)
--			is one INSERT with no schema change.
-- -----------------------------------------------------------------------------
CREATE TABLE fuel_network.fuel_types (
	fuel_type_id SERIAL NOT NULL,
	fuel_type_name VARCHAR(50) NOT NULL,
	description VARCHAR(200),

	CONSTRAINT PK_fuel_types_fuel_type_id PRIMARY KEY (fuel_type_id),
	CONSTRAINT UQ_fuel_types_fuel_type_name UNIQUE (fuel_type_name)
);


-- -----------------------------------------------------------------------------
-- TABLE 3: employee_roles
-- Type: Lookup
-- Purpose: Allowed staff role categories across all stations.
--
-- Why VARCHAR + CHECK instead of ENUM:
--	Adding a new role only needs ALTER TABLE ... ADD CHECK, not ALTER TYPE
--	which requires a full redeploy in some environments.
--	The allowed values are also readable in plain SQL without querying pg_type.
-- -----------------------------------------------------------------------------
CREATE TABLE fuel_network.employee_roles (
	role_id SERIAL NOT NULL,
	role_name VARCHAR(50) NOT NULL,

	CONSTRAINT PK_employee_roles_role_id PRIMARY KEY (role_id),
	CONSTRAINT UQ_employee_roles_role_name UNIQUE (role_name),

	-- [CHECK CONSTRAINT 1/5]
	-- Restricts role_name to the five predefined organisational roles.
	-- Any insert or update with a value outside this list will be rejected.
	CONSTRAINT CHK_employee_roles_role_name
		CHECK (role_name IN ('Cashier', 'Manager', 'Technician', 'Security', 'Supervisor'))
);


-- -----------------------------------------------------------------------------
-- TABLE 4: suppliers
-- Type: Lookup / Master
-- Purpose: Fuel wholesalers and delivery companies.
-- -----------------------------------------------------------------------------
CREATE TABLE fuel_network.suppliers (
	supplier_id SERIAL NOT NULL,
	supplier_name VARCHAR(150) NOT NULL,
	contact_phone VARCHAR(20),
	contact_email VARCHAR(150),
	-- Defaults to Georgia because the network operates primarily in Georgia.
	-- Override for foreign suppliers (e.g. AzPetrol).
	country VARCHAR(100) NOT NULL DEFAULT 'Georgia',

	CONSTRAINT PK_suppliers_supplier_id PRIMARY KEY (supplier_id),
	CONSTRAINT UQ_suppliers_supplier_name UNIQUE (supplier_name),

	-- Basic email sanity check: must contain @ with at least one dot after it.
	-- A full regex is avoided here because email validation belongs in the app layer.
	CONSTRAINT CHK_suppliers_contact_email
		CHECK (contact_email IS NULL OR contact_email LIKE '%@%.%'),

	-- Phone must start with + to enforce international dialling format.
	CONSTRAINT CHK_suppliers_contact_phone
		CHECK (contact_phone IS NULL OR contact_phone LIKE '+%')
);


-- -----------------------------------------------------------------------------
-- TABLE 5: stations
-- Type: Master
-- Purpose: Individual physical fuel stations in the network.
--			Parent of station_fuel, fuel_prices, employees, deliveries, fuel_sales.
-- -----------------------------------------------------------------------------
CREATE TABLE fuel_network.stations (
	station_id SERIAL NOT NULL,
	station_name VARCHAR(150) NOT NULL,
	address VARCHAR(250) NOT NULL,
	city VARCHAR(100) NOT NULL,
	region_id INTEGER NOT NULL,
	phone VARCHAR(20),
	-- Operational lifecycle status. Defaults to Active on creation.
	station_status VARCHAR(20) NOT NULL DEFAULT 'Active',
	opened_date DATE NOT NULL,

	CONSTRAINT PK_stations_station_id PRIMARY KEY (station_id),
	CONSTRAINT UQ_stations_station_name UNIQUE (station_name),

	CONSTRAINT FK_stations_region_id
		FOREIGN KEY (region_id) REFERENCES fuel_network.regions (region_id),

	-- [CHECK CONSTRAINT 2/5]
	-- Limits station_status to the three defined operational states.
	-- Under Maintenance is used for stations temporarily out of service
	-- without removing them from the network (e.g. FuelNet Rustavi).
	CONSTRAINT CHK_stations_station_status
		CHECK (station_status IN ('Active', 'Inactive', 'Under Maintenance')),

	-- A station cannot predate the company founding year.
	CONSTRAINT CHK_stations_opened_date
		CHECK (opened_date >= DATE '2000-01-01'),

	CONSTRAINT CHK_stations_phone
		CHECK (phone IS NULL OR phone LIKE '+%')
);


-- -----------------------------------------------------------------------------
-- TABLE 6: customers
-- Type: Master
-- Purpose: Registered loyalty-card customers.
--			Walk-in / anonymous sales reference no customer row (NULL FK in fuel_sales).
-- -----------------------------------------------------------------------------
CREATE TABLE fuel_network.customers (
	customer_id SERIAL NOT NULL,
	first_name VARCHAR(80) NOT NULL,
	last_name VARCHAR(80) NOT NULL,
	phone VARCHAR(20),
	email VARCHAR(150),
	-- Optional. Unique when present. Format enforced: LCD-XXXXXXXXXX.
	loyalty_card_number VARCHAR(20),
	registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

	CONSTRAINT PK_customers_customer_id PRIMARY KEY (customer_id),
	CONSTRAINT UQ_customers_loyalty_card_number UNIQUE (loyalty_card_number),

	CONSTRAINT CHK_customers_email
		CHECK (email IS NULL OR email LIKE '%@%.%'),

	-- [CHECK CONSTRAINT 3/5]
	-- Registration must be on or after 2026-01-01 because sample data covers
	-- only the last three months per task requirements, and no legacy customers
	-- are in scope for this submission.
	CONSTRAINT CHK_customers_registered_at
		CHECK (registered_at >= TIMESTAMPTZ '2026-01-01 00:00:00+00'),

	CONSTRAINT CHK_customers_phone
		CHECK (phone IS NULL OR phone LIKE '+%'),

	-- Loyalty card format: LCD- prefix followed by exactly 10 digits.
	-- This matches the physical card numbering format used by the network.
	CONSTRAINT CHK_customers_loyalty_card_format
		CHECK (
			loyalty_card_number IS NULL OR
			loyalty_card_number ~ '^LCD-[0-9]{10}$'
		)
);


-- -----------------------------------------------------------------------------
-- TABLE 7: station_fuel
-- Type: Bridge (M:N - stations <-> fuel_types)
-- Purpose: Records which fuel types a station carries and its current stock.
--
-- Why surrogate PK + composite UNIQUE instead of composite PK:
--	A surrogate PK is simpler to reference if a child table ever needs it.
--	The composite UNIQUE (station_id, fuel_type_id) is what actually prevents
--	inserting the same station-fuel pair twice. A surrogate PK alone does not.
-- -----------------------------------------------------------------------------
CREATE TABLE fuel_network.station_fuel (
	station_fuel_id SERIAL NOT NULL,
	station_id INTEGER NOT NULL,
	fuel_type_id INTEGER NOT NULL,
	-- Physical tank capacity in litres for this fuel type at this station.
	tank_capacity_l NUMERIC(10, 2) NOT NULL,
	-- Current stock in litres. Updated after each delivery and sale.
	current_stock_l NUMERIC(10, 2) NOT NULL DEFAULT 0,

	CONSTRAINT PK_station_fuel_station_fuel_id PRIMARY KEY (station_fuel_id),

	-- Composite UNIQUE prevents a station from listing the same fuel type twice.
	CONSTRAINT UQ_station_fuel_station_fuel
		UNIQUE (station_id, fuel_type_id),

	CONSTRAINT FK_station_fuel_station_id
		FOREIGN KEY (station_id) REFERENCES fuel_network.stations (station_id),
	CONSTRAINT FK_station_fuel_fuel_type_id
		FOREIGN KEY (fuel_type_id) REFERENCES fuel_network.fuel_types (fuel_type_id),

	-- [CHECK CONSTRAINT 4/5]
	-- Tank capacity must be a positive number.
	-- Zero would mean the station has no tank, which is not a valid state.
	CONSTRAINT CHK_station_fuel_tank_capacity_l
		CHECK (tank_capacity_l > 0),

	-- Stock cannot be negative or exceed the physical tank capacity.
	-- A delivery that would overflow the tank should be rejected at the app level,
	-- but this constraint is a last line of defence at the DB level.
	CONSTRAINT CHK_station_fuel_current_stock_l
		CHECK (current_stock_l >= 0 AND current_stock_l <= tank_capacity_l)
);


-- -----------------------------------------------------------------------------
-- TABLE 8: fuel_prices
-- Type: History (append-only)
-- Purpose: Pricing history per station per fuel type.
--			A new row is inserted whenever the price changes.
--			Current price = MAX(effective_from) for a given (station, fuel_type).
--
-- Why no is_current flag:
--	Storing the current state in two places (a flag here and the latest row)
--	creates a dual-write risk: if the flag update fails, the two facts disagree
--	silently. Deriving the current price from MAX(effective_from) keeps one
--	source of truth and is fast with an index on (station_id, fuel_type_id,
--	effective_from).
--
-- NOTE on referential integrity with station_fuel:
--	This table does not have a composite FK to station_fuel(station_id,
--	fuel_type_id) because PostgreSQL FKs can only reference tables, not
--	unique constraints on a subset of columns in a different table without
--	a dedicated unique key on that combination. The UNIQUE constraint
--	UQ_station_fuel_station_fuel on station_fuel does serve this purpose
--	in principle, but to keep the schema portable and straightforward a
--	composite FK is not added here. Application and function logic is
--	responsible for ensuring prices are only inserted for stocked fuels.
-- -----------------------------------------------------------------------------
CREATE TABLE fuel_network.fuel_prices (
	price_id SERIAL NOT NULL,
	station_id INTEGER NOT NULL,
	fuel_type_id INTEGER NOT NULL,
	-- Standard retail price per litre in GEL.
	regular_price NUMERIC(6, 3) NOT NULL,
	-- Optional discount price. NULL means no active promotion at this station.
	discounted_price NUMERIC(6, 3),
	effective_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),

	CONSTRAINT PK_fuel_prices_price_id PRIMARY KEY (price_id),

	-- Prevents two price records for the same station + fuel at the exact same moment.
	CONSTRAINT UQ_fuel_prices_station_fuel_time
		UNIQUE (station_id, fuel_type_id, effective_from),

	CONSTRAINT FK_fuel_prices_station_id
		FOREIGN KEY (station_id) REFERENCES fuel_network.stations (station_id),
	CONSTRAINT FK_fuel_prices_fuel_type_id
		FOREIGN KEY (fuel_type_id) REFERENCES fuel_network.fuel_types (fuel_type_id),

	CONSTRAINT CHK_fuel_prices_regular_price
		CHECK (regular_price > 0),

	-- Discounted price, when present, must be positive AND strictly below regular.
	-- If discounted_price >= regular_price it would not actually be a discount.
	CONSTRAINT CHK_fuel_prices_discounted_price
		CHECK (
			discounted_price IS NULL OR
			(discounted_price > 0 AND discounted_price < regular_price)
		),

	-- [CHECK CONSTRAINT 5/5]
	-- Price records must not be backdated before 2026-01-01.
	-- This aligns with the task requirement that all data covers the last 3 months.
	CONSTRAINT CHK_fuel_prices_effective_from
		CHECK (effective_from >= TIMESTAMPTZ '2026-01-01 00:00:00+00')
);


-- -----------------------------------------------------------------------------
-- TABLE 9: employees
-- Type: Master
-- Purpose: Staff members assigned to stations.
--
-- Why GENERATED ALWAYS AS IDENTITY instead of SERIAL:
--	GENERATED ALWAYS AS IDENTITY is the SQL-standard syntax introduced in
--	PostgreSQL 10. SERIAL is a legacy shorthand that leaks sequence ownership
--	and permissions details. IDENTITY columns are owned by the table, not the
--	sequence, which simplifies copying and dropping tables.
--
-- Why is_active instead of DELETE:
--	Employees are soft-deleted because fuel_sales references employee_id.
--	Hard-deleting a terminated employee would orphan historical sale records
--	or require cascading nullification, which destroys the audit trail.
--	is_active = FALSE keeps the row for history while excluding the employee
--	from active queries.
-- -----------------------------------------------------------------------------
CREATE TABLE fuel_network.employees (
	employee_id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY,
	first_name VARCHAR(80) NOT NULL,
	last_name VARCHAR(80) NOT NULL,
	role_id INTEGER NOT NULL,
	station_id INTEGER NOT NULL,
	hire_date DATE NOT NULL,
	phone VARCHAR(20),
	email VARCHAR(150),
	-- Monthly salary in GEL. 0 is allowed for interns or probation periods.
	salary NUMERIC(8, 2) NOT NULL,
	-- FALSE = terminated. Row is kept for audit history (soft-delete).
	is_active BOOLEAN NOT NULL DEFAULT TRUE,

	CONSTRAINT PK_employees_employee_id PRIMARY KEY (employee_id),

	CONSTRAINT FK_employees_role_id
		FOREIGN KEY (role_id) REFERENCES fuel_network.employee_roles (role_id),
	CONSTRAINT FK_employees_station_id
		FOREIGN KEY (station_id) REFERENCES fuel_network.stations (station_id),

	-- Salary cannot be negative. Zero is allowed for probation / interns.
	CONSTRAINT CHK_employees_salary
		CHECK (salary >= 0),

	-- Hire date cannot predate company founding year.
	CONSTRAINT CHK_employees_hire_date
		CHECK (hire_date >= DATE '2000-01-01'),

	CONSTRAINT CHK_employees_email
		CHECK (email IS NULL OR email LIKE '%@%.%'),

	CONSTRAINT CHK_employees_phone
		CHECK (phone IS NULL OR phone LIKE '+%')
);


-- -----------------------------------------------------------------------------
-- TABLE 10: deliveries
-- Type: Transaction (header)
-- Purpose: Fuel replenishment delivery header. One row per truck arrival.
--			Line items (quantities per fuel type) live in delivery_items.
--			Splitting header from line items avoids repeating supplier and
--			station data for every fuel type in a multi-product delivery.
-- -----------------------------------------------------------------------------
CREATE TABLE fuel_network.deliveries (
	delivery_id SERIAL NOT NULL,
	station_id INTEGER NOT NULL,
	supplier_id INTEGER NOT NULL,
	delivery_date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	-- Optional supplier-side waybill number for cross-referencing paper records.
	reference_no VARCHAR(50),

	CONSTRAINT PK_deliveries_delivery_id PRIMARY KEY (delivery_id),

	CONSTRAINT FK_deliveries_station_id
		FOREIGN KEY (station_id) REFERENCES fuel_network.stations (station_id),
	CONSTRAINT FK_deliveries_supplier_id
		FOREIGN KEY (supplier_id) REFERENCES fuel_network.suppliers (supplier_id),

	CONSTRAINT CHK_deliveries_delivery_date
		CHECK (delivery_date >= TIMESTAMPTZ '2026-01-01 00:00:00+00')
);


-- -----------------------------------------------------------------------------
-- TABLE 11: delivery_items
-- Type: Transaction (line item)
-- Purpose: Individual fuel quantities and wholesale costs per delivery.
--			One delivery can contain multiple fuel types as separate line items.
-- -----------------------------------------------------------------------------
CREATE TABLE fuel_network.delivery_items (
	delivery_item_id SERIAL NOT NULL,
	delivery_id INTEGER NOT NULL,
	fuel_type_id INTEGER NOT NULL,
	-- Litres of this fuel type received in this delivery.
	quantity_l NUMERIC(10, 2) NOT NULL,
	-- Wholesale price per litre charged by the supplier for this delivery.
	-- Stored here rather than on deliveries because different fuels in the
	-- same delivery can have different wholesale rates.
	unit_cost NUMERIC(6, 3) NOT NULL,

	CONSTRAINT PK_delivery_items_delivery_item_id PRIMARY KEY (delivery_item_id),

	-- A single delivery cannot list the same fuel type twice.
	CONSTRAINT UQ_delivery_items_delivery_fuel
		UNIQUE (delivery_id, fuel_type_id),

	CONSTRAINT FK_delivery_items_delivery_id
		FOREIGN KEY (delivery_id) REFERENCES fuel_network.deliveries (delivery_id),
	CONSTRAINT FK_delivery_items_fuel_type_id
		FOREIGN KEY (fuel_type_id) REFERENCES fuel_network.fuel_types (fuel_type_id),

	CONSTRAINT CHK_delivery_items_quantity_l
		CHECK (quantity_l > 0),

	CONSTRAINT CHK_delivery_items_unit_cost
		CHECK (unit_cost > 0)
);


-- -----------------------------------------------------------------------------
-- TABLE 12: fuel_sales <- PRIMARY TRANSACTION TABLE
-- Type: Transaction
-- Purpose: One row per individual fuel pump transaction.
--
-- Why total_amount is GENERATED ALWAYS AS STORED:
--   Storing total_amount as a computed column guarantees that it is always
--   equal to ROUND(quantity_l * price_per_litre, 2). If it were a plain
--   column, an application bug or a direct UPDATE could make the stored total
--   disagree with the quantity and price on the same row. STORED means the
--   value is written to disk at INSERT time (not recomputed on every read),
--   so it has the same query performance as a regular column.
--
-- Why customer_id is nullable:
--   Not every pump transaction belongs to a registered loyalty customer.
--   Walk-in / cash sales are a normal part of the business. Using NULL is
--   correct here. A dummy "anonymous customer" row would pollute the customers
--   table and make customer-level analytics meaningless.
-- -----------------------------------------------------------------------------
CREATE TABLE fuel_network.fuel_sales (
	sale_id SERIAL NOT NULL,
	station_id INTEGER NOT NULL,
	fuel_type_id INTEGER NOT NULL,
	employee_id INTEGER NOT NULL,
	-- NULL for anonymous (walk-in / cash) customers.
	customer_id INTEGER,
	sale_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	-- Volume dispensed in litres.
	quantity_l NUMERIC(8, 3) NOT NULL,
	-- Retail price per litre at the exact moment of the sale.
	-- Stored here because fuel_prices can change after the sale is recorded.
	price_per_litre NUMERIC(6, 3) NOT NULL,
	-- Computed from quantity_l and price_per_litre. Cannot be set manually.
	total_amount NUMERIC(10, 2)
		GENERATED ALWAYS AS (ROUND(quantity_l * price_per_litre, 2)) STORED,
	payment_method VARCHAR(20) NOT NULL,

	CONSTRAINT PK_fuel_sales_sale_id PRIMARY KEY (sale_id),

	CONSTRAINT FK_fuel_sales_station_id
		FOREIGN KEY (station_id) REFERENCES fuel_network.stations (station_id),
	CONSTRAINT FK_fuel_sales_fuel_type_id
		FOREIGN KEY (fuel_type_id) REFERENCES fuel_network.fuel_types (fuel_type_id),
	CONSTRAINT FK_fuel_sales_employee_id
		FOREIGN KEY (employee_id) REFERENCES fuel_network.employees (employee_id),
	-- Optional FK: NULL is allowed for anonymous customers.
	CONSTRAINT FK_fuel_sales_customer_id
		FOREIGN KEY (customer_id) REFERENCES fuel_network.customers (customer_id),

	-- Payment method is constrained to the four accepted values.
	-- Loyalty means the customer paid via their loyalty card balance.
	CONSTRAINT CHK_fuel_sales_payment_method
		CHECK (payment_method IN ('Cash', 'Card', 'Mobile', 'Loyalty')),

	CONSTRAINT CHK_fuel_sales_quantity_l
		CHECK (quantity_l > 0),

	CONSTRAINT CHK_fuel_sales_price_per_litre
		CHECK (price_per_litre > 0),

	CONSTRAINT CHK_fuel_sales_sale_timestamp
		CHECK (sale_timestamp >= TIMESTAMPTZ '2026-01-01 00:00:00+00')
);


-----------------------------------------------------------------------------
-- PART 2: DML: SAMPLE DATA (Jan-Apr 2026)
-----------------------------------------------------------------------------
/*
Rules applied throughout:
	- No surrogate keys hardcoded. All IDs resolved via JOIN / subquery on
	  natural keys so the script works after any re-run that resets sequences.
	- WHERE NOT EXISTS or ON CONFLICT DO NOTHING on every INSERT to prevent
	  duplicates on re-run.
	- DEFAULT column values are omitted in the column list where the column
	  default is intentionally used (e.g. registered_at, sale_timestamp).
	- All timestamps fall within 2026-01-01 to 2026-04-23.
	- Tables with >= 6 rows each; total across all tables >= 36 rows.
	- RETURNING is used on all inserts to confirm generated IDs and inserted values.
*/


-- -----------------------------------------------------------------------------
-- Lookup: regions
-- -----------------------------------------------------------------------------
INSERT INTO fuel_network.regions (region_name)
SELECT src.region_name
FROM (
	VALUES
	('Tbilisi'),
	('Adjara'),
	('Imereti'),
	('Shida Kartli'),
	('Kakheti'),
	('Kvemo Kartli'),
	('Samegrelo')
) AS src (region_name)
WHERE NOT EXISTS (
	SELECT 1
	FROM fuel_network.regions r
	WHERE lower(r.region_name) = lower(src.region_name)
)
RETURNING region_id, region_name;


-- -----------------------------------------------------------------------------
-- Lookup: fuel_types
-- -----------------------------------------------------------------------------
INSERT INTO fuel_network.fuel_types (fuel_type_name, description)
SELECT src.fuel_type_name, src.description
FROM (
	VALUES
	('Regular 92', 'Standard unleaded petrol, RON 92'),
	('Premium 95', 'Premium unleaded petrol, RON 95'),
	('Super 98', 'Super premium unleaded petrol, RON 98'),
	('Diesel', 'Standard automotive diesel fuel'),
	('Premium Diesel', 'Low-sulphur premium diesel for modern engines'),
	('LPG', 'Liquefied petroleum gas for converted vehicles')
) AS src (fuel_type_name, description)
WHERE NOT EXISTS (
	SELECT 1
	FROM fuel_network.fuel_types ft
	WHERE lower(ft.fuel_type_name) = lower(src.fuel_type_name)
)
RETURNING fuel_type_id, fuel_type_name;


-- -----------------------------------------------------------------------------
-- Lookup: employee_roles
-- Values are constrained by CHK_employee_roles_role_name.
-- ON CONFLICT is used here because role_name has a UNIQUE constraint,
-- making it safe and more concise than WHERE NOT EXISTS.
-- -----------------------------------------------------------------------------
INSERT INTO fuel_network.employee_roles (role_name)
SELECT src.role_name
FROM (
	VALUES
	('Cashier'),
	('Manager'),
	('Technician'),
	('Security'),
	('Supervisor')
) AS src (role_name)
ON CONFLICT (role_name) DO NOTHING
RETURNING role_id, role_name;


-- -----------------------------------------------------------------------------
-- Master: suppliers
-- -----------------------------------------------------------------------------
INSERT INTO fuel_network.suppliers (supplier_name, contact_phone, contact_email, country)
SELECT src.supplier_name, src.contact_phone, src.contact_email, src.country
FROM (
	VALUES
	('Georgian Oil Company', '+99532100200', 'supply@goc.ge', 'Georgia'),
	('Socar Georgia Petroleum', '+99532200300', 'orders@socar.ge', 'Georgia'),
	('Lukoil Georgia LLC', '+99532300400', 'georgia@lukoil.com', 'Georgia'),
	('Wissol Petroleum', '+99532400500', 'logistics@wissol.ge', 'Georgia'),
	('Gulf Energy GE', '+99532500600', 'operations@gulf.ge', 'Georgia'),
	('AzPetrol International', '+994121234567', 'supply@azpetrol.az', 'Azerbaijan')
) AS src (supplier_name, contact_phone, contact_email, country)
WHERE NOT EXISTS (
	SELECT 1
	FROM fuel_network.suppliers s
	WHERE lower(s.supplier_name) = lower(src.supplier_name)
)
RETURNING supplier_id, supplier_name, country;


-- -----------------------------------------------------------------------------
-- Master: stations
-- region_id is resolved by name to avoid hardcoding surrogate IDs.
-- -----------------------------------------------------------------------------
INSERT INTO fuel_network.stations (
	station_name, address, city, region_id, phone, station_status, opened_date
)
SELECT
	src.station_name,
	src.address,
	src.city,
	r.region_id,
	src.phone,
	src.station_status,
	src.opened_date
FROM (
	VALUES
	('FuelNet Vake', '14 Chavchavadze Ave', 'Tbilisi', 'Tbilisi', '+99532700100', 'Active', DATE '2010-03-15'),
	('FuelNet Saburtalo', '7 Tsinamdzgvrishvili', 'Tbilisi', 'Tbilisi', '+99532700200', 'Active', DATE '2012-06-20'),
	('FuelNet Batumi South', '3 Ninoshvili St', 'Batumi', 'Adjara', '+99532700300', 'Active', DATE '2014-09-10'),
	('FuelNet Kutaisi', '22 King Tamar Ave', 'Kutaisi', 'Imereti', '+99532700400', 'Active', DATE '2015-02-01'),
	('FuelNet Gori', '5 Stalin Ave', 'Gori', 'Shida Kartli', '+99532700500', 'Active', DATE '2016-11-05'),
	('FuelNet Rustavi', '11 Kostava St', 'Rustavi', 'Kvemo Kartli', '+99532700600', 'Under Maintenance', DATE '2018-04-18'),
	('FuelNet Telavi', '8 Alazani Blvd', 'Telavi', 'Kakheti', '+99532700700', 'Active', DATE '2019-07-22'),
	('FuelNet Zugdidi', '2 Zviad Gamsakhurdia', 'Zugdidi', 'Samegrelo', '+99532700800', 'Active', DATE '2020-01-30')
) AS src (station_name, address, city, region_name, phone, station_status, opened_date)
INNER JOIN fuel_network.regions r ON lower(r.region_name) = lower(src.region_name)
WHERE NOT EXISTS (
	SELECT 1
	FROM fuel_network.stations st
	WHERE lower(st.station_name) = lower(src.station_name)
)
RETURNING station_id, station_name, city, station_status;


-- -----------------------------------------------------------------------------
-- Master: customers
-- -----------------------------------------------------------------------------
INSERT INTO fuel_network.customers (
	first_name, last_name, phone, email, loyalty_card_number, registered_at
)
SELECT
	src.first_name,
	src.last_name,
	src.phone,
	src.email,
	src.loyalty_card_number,
	src.registered_at
FROM (
	VALUES
	('Nino', 'Lomidze', '+99599100001', 'nino.lomidze@mail.ge', 'LCD-1234567890', TIMESTAMPTZ '2026-01-05 10:30:00+04'),
	('Giorgi', 'Beridze', '+99599200002', 'giorgi.beridze@gmail.com', 'LCD-2345678901', TIMESTAMPTZ '2026-01-12 14:15:00+04'),
	('Tamara', 'Kvaratskhelia', '+99599300003', 'tamara.k@yahoo.com', 'LCD-3456789012', TIMESTAMPTZ '2026-01-20 09:00:00+04'),
	('Davit', 'Jishkariani', '+99599400004', 'davit.j@outlook.com', 'LCD-4567890123', TIMESTAMPTZ '2026-02-03 11:45:00+04'),
	('Mariam', 'Tsiklauri', '+99599500005', 'mariam.ts@mail.ge', 'LCD-5678901234', TIMESTAMPTZ '2026-02-14 16:00:00+04'),
	('Sandro', 'Gabrichidze', '+99599600006', 'sandro.g@gmail.com', 'LCD-6789012345', TIMESTAMPTZ '2026-03-01 08:30:00+04'),
	('Ekaterine', 'Nakashidze', '+99599700007', 'ekaterine.n@mail.ge', 'LCD-7890123456', TIMESTAMPTZ '2026-03-10 13:20:00+04'),
	('Levan', 'Suladze', '+99599800008', 'levan.s@gmail.com', 'LCD-8901234567', TIMESTAMPTZ '2026-04-01 10:00:00+04')
) AS src (first_name, last_name, phone, email, loyalty_card_number, registered_at)
WHERE NOT EXISTS (
	SELECT 1
	FROM fuel_network.customers c
	WHERE c.loyalty_card_number = src.loyalty_card_number
)
RETURNING customer_id, first_name, last_name, loyalty_card_number;


-- -----------------------------------------------------------------------------
-- Bridge: station_fuel (M:N - stations <-> fuel_types)
-- station_id and fuel_type_id are resolved by name.
-- 8 stations x 3 fuel types each = 24 rows.
-- ON CONFLICT on the composite UNIQUE is the cleanest duplicate guard here.
-- -----------------------------------------------------------------------------
INSERT INTO fuel_network.station_fuel (
	station_id, fuel_type_id, tank_capacity_l, current_stock_l
)
SELECT
	st.station_id,
	ft.fuel_type_id,
	src.tank_capacity_l,
	src.current_stock_l
FROM (
	VALUES
	('FuelNet Vake', 'Regular 92', 20000.00, 8500.00),
	('FuelNet Vake', 'Premium 95', 15000.00, 6200.00),
	('FuelNet Vake', 'Diesel', 25000.00, 12000.00),
	('FuelNet Saburtalo', 'Regular 92', 20000.00, 9000.00),
	('FuelNet Saburtalo', 'Premium 95', 15000.00, 7000.00),
	('FuelNet Saburtalo', 'LPG', 10000.00, 4500.00),
	('FuelNet Batumi South', 'Regular 92', 18000.00, 7500.00),
	('FuelNet Batumi South', 'Diesel', 22000.00, 10000.00),
	('FuelNet Batumi South', 'Super 98', 12000.00, 5000.00),
	('FuelNet Kutaisi', 'Regular 92', 20000.00, 8000.00),
	('FuelNet Kutaisi', 'Premium 95', 15000.00, 6500.00),
	('FuelNet Kutaisi', 'Diesel', 25000.00, 11000.00),
	('FuelNet Gori', 'Regular 92', 18000.00, 7200.00),
	('FuelNet Gori', 'Premium 95', 12000.00, 5000.00),
	('FuelNet Gori', 'LPG', 8000.00, 3500.00),
	('FuelNet Rustavi', 'Regular 92', 20000.00, 6000.00),
	('FuelNet Rustavi', 'Diesel', 22000.00, 8000.00),
	('FuelNet Rustavi', 'Premium Diesel', 10000.00, 4000.00),
	('FuelNet Telavi', 'Regular 92', 15000.00, 5500.00),
	('FuelNet Telavi', 'Premium 95', 10000.00, 4000.00),
	('FuelNet Telavi', 'Diesel', 18000.00, 7000.00),
	('FuelNet Zugdidi', 'Regular 92', 15000.00, 6000.00),
	('FuelNet Zugdidi', 'Premium 95', 10000.00, 4500.00),
	('FuelNet Zugdidi', 'Diesel', 18000.00, 8000.00)
) AS src (station_name, fuel_type_name, tank_capacity_l, current_stock_l)
INNER JOIN fuel_network.stations st ON lower(st.station_name) = lower(src.station_name)
INNER JOIN fuel_network.fuel_types ft ON lower(ft.fuel_type_name) = lower(src.fuel_type_name)
ON CONFLICT (station_id, fuel_type_id) DO NOTHING
RETURNING station_fuel_id, station_id, fuel_type_id, tank_capacity_l, current_stock_l;


-- -----------------------------------------------------------------------------
-- History: fuel_prices
-- Inserts initial prices (January 2026) and a mid-quarter price update
-- (February 2026) for FuelNet Vake to demonstrate history tracking.
-- Current price = MAX(effective_from) per (station_id, fuel_type_id).
-- TIMESTAMPTZ is used because the network operates in Georgia (UTC+4).
-- -----------------------------------------------------------------------------
INSERT INTO fuel_network.fuel_prices (
	station_id, fuel_type_id, regular_price, discounted_price, effective_from
)
SELECT
	st.station_id,
	ft.fuel_type_id,
	src.regular_price,
	src.discounted_price,
	src.effective_from
FROM (
	VALUES
	-- FuelNet Vake initial prices - January 2026
	('FuelNet Vake', 'Regular 92', 2.450::NUMERIC(6,3), NULL::NUMERIC(6,3), TIMESTAMPTZ '2026-01-01 06:00:00+04'),
	('FuelNet Vake', 'Premium 95', 2.750::NUMERIC(6,3), 2.650::NUMERIC(6,3), TIMESTAMPTZ '2026-01-01 06:00:00+04'),
	('FuelNet Vake', 'Diesel', 2.650::NUMERIC(6,3), NULL::NUMERIC(6,3), TIMESTAMPTZ '2026-01-01 06:00:00+04'),
	-- FuelNet Vake price update - February 2026
	('FuelNet Vake', 'Regular 92', 2.480::NUMERIC(6,3), NULL::NUMERIC(6,3), TIMESTAMPTZ '2026-02-01 06:00:00+04'),
	('FuelNet Vake', 'Diesel', 2.700::NUMERIC(6,3), 2.600::NUMERIC(6,3), TIMESTAMPTZ '2026-02-01 06:00:00+04'),
	-- FuelNet Saburtalo
	('FuelNet Saburtalo', 'Regular 92', 2.450::NUMERIC(6,3), NULL::NUMERIC(6,3), TIMESTAMPTZ '2026-01-01 06:00:00+04'),
	('FuelNet Saburtalo', 'Premium 95', 2.750::NUMERIC(6,3), 2.680::NUMERIC(6,3), TIMESTAMPTZ '2026-01-01 06:00:00+04'),
	('FuelNet Saburtalo', 'LPG', 1.200::NUMERIC(6,3), NULL::NUMERIC(6,3), TIMESTAMPTZ '2026-01-01 06:00:00+04'),
	-- FuelNet Batumi South
	('FuelNet Batumi South', 'Regular 92', 2.460::NUMERIC(6,3), NULL::NUMERIC(6,3), TIMESTAMPTZ '2026-01-01 06:00:00+04'),
	('FuelNet Batumi South', 'Diesel', 2.660::NUMERIC(6,3), NULL::NUMERIC(6,3), TIMESTAMPTZ '2026-01-01 06:00:00+04'),
	('FuelNet Batumi South', 'Super 98', 3.100::NUMERIC(6,3), 3.000::NUMERIC(6,3), TIMESTAMPTZ '2026-01-01 06:00:00+04'),
	-- FuelNet Kutaisi
	('FuelNet Kutaisi', 'Regular 92', 2.440::NUMERIC(6,3), NULL::NUMERIC(6,3), TIMESTAMPTZ '2026-01-01 06:00:00+04'),
	('FuelNet Kutaisi', 'Premium 95', 2.740::NUMERIC(6,3), NULL::NUMERIC(6,3), TIMESTAMPTZ '2026-01-01 06:00:00+04'),
	('FuelNet Kutaisi', 'Diesel', 2.640::NUMERIC(6,3), 2.590::NUMERIC(6,3), TIMESTAMPTZ '2026-01-01 06:00:00+04'),
	-- FuelNet Gori
	('FuelNet Gori', 'Regular 92', 2.450::NUMERIC(6,3), NULL::NUMERIC(6,3), TIMESTAMPTZ '2026-01-01 06:00:00+04'),
	('FuelNet Gori', 'Premium 95', 2.750::NUMERIC(6,3), NULL::NUMERIC(6,3), TIMESTAMPTZ '2026-01-01 06:00:00+04'),
	('FuelNet Gori', 'LPG', 1.190::NUMERIC(6,3), NULL::NUMERIC(6,3), TIMESTAMPTZ '2026-01-01 06:00:00+04'),
	-- FuelNet Rustavi
	('FuelNet Rustavi', 'Regular 92', 2.430::NUMERIC(6,3), NULL::NUMERIC(6,3), TIMESTAMPTZ '2026-01-01 06:00:00+04'),
	('FuelNet Rustavi', 'Diesel', 2.630::NUMERIC(6,3), NULL::NUMERIC(6,3), TIMESTAMPTZ '2026-01-01 06:00:00+04'),
	('FuelNet Rustavi', 'Premium Diesel', 2.900::NUMERIC(6,3), 2.800::NUMERIC(6,3), TIMESTAMPTZ '2026-01-01 06:00:00+04'),
	-- FuelNet Telavi
	('FuelNet Telavi', 'Regular 92', 2.460::NUMERIC(6,3), NULL::NUMERIC(6,3), TIMESTAMPTZ '2026-01-01 06:00:00+04'),
	('FuelNet Telavi', 'Premium 95', 2.760::NUMERIC(6,3), NULL::NUMERIC(6,3), TIMESTAMPTZ '2026-01-01 06:00:00+04'),
	('FuelNet Telavi', 'Diesel', 2.660::NUMERIC(6,3), 2.610::NUMERIC(6,3), TIMESTAMPTZ '2026-01-01 06:00:00+04'),
	-- FuelNet Zugdidi
	('FuelNet Zugdidi', 'Regular 92', 2.450::NUMERIC(6,3), NULL::NUMERIC(6,3), TIMESTAMPTZ '2026-01-01 06:00:00+04'),
	('FuelNet Zugdidi', 'Premium 95', 2.750::NUMERIC(6,3), 2.700::NUMERIC(6,3), TIMESTAMPTZ '2026-01-01 06:00:00+04'),
	('FuelNet Zugdidi', 'Diesel', 2.650::NUMERIC(6,3), NULL::NUMERIC(6,3), TIMESTAMPTZ '2026-01-01 06:00:00+04')
) AS src (station_name, fuel_type_name, regular_price, discounted_price, effective_from)
INNER JOIN fuel_network.stations st ON lower(st.station_name) = lower(src.station_name)
INNER JOIN fuel_network.fuel_types ft ON lower(ft.fuel_type_name) = lower(src.fuel_type_name)
ON CONFLICT (station_id, fuel_type_id, effective_from) DO NOTHING
RETURNING price_id, station_id, fuel_type_id, regular_price, discounted_price, effective_from;


-- -----------------------------------------------------------------------------
-- Master: employees
-- role_id and station_id resolved by name.
-- WHERE NOT EXISTS checks first + last name + station to avoid inserting
-- duplicates if the same person works at two stations in future data.
-- -----------------------------------------------------------------------------
INSERT INTO fuel_network.employees (
	first_name, last_name, role_id, station_id, hire_date, phone, email, salary
)
SELECT
	src.first_name,
	src.last_name,
	er.role_id,
	st.station_id,
	src.hire_date,
	src.phone,
	src.email,
	src.salary
FROM (
	VALUES
	('Ana', 'Kvariani', 'Manager', 'FuelNet Vake', DATE '2018-03-01', '+99599111001', 'ana.k@fuelnet.ge', 3500.00::NUMERIC(8,2)),
	('Irakli', 'Dolidze', 'Cashier', 'FuelNet Vake', DATE '2020-06-15', '+99599111002', 'irakli.d@fuelnet.ge', 1800.00::NUMERIC(8,2)),
	('Salome', 'Maisuradze', 'Cashier', 'FuelNet Saburtalo', DATE '2021-01-10', '+99599111003', 'salome.m@fuelnet.ge', 1800.00::NUMERIC(8,2)),
	('Nikoloz', 'Chitashvili', 'Manager', 'FuelNet Saburtalo', DATE '2017-09-20', '+99599111004', 'nikoloz.c@fuelnet.ge', 3500.00::NUMERIC(8,2)),
	('Tinatin', 'Apakidze', 'Cashier', 'FuelNet Batumi South', DATE '2022-04-05', '+99599111005', 'tinatin.a@fuelnet.ge', 1800.00::NUMERIC(8,2)),
	('Zurab', 'Tsikarishvili', 'Manager', 'FuelNet Batumi South', DATE '2016-12-01', '+99599111006', 'zurab.t@fuelnet.ge', 3500.00::NUMERIC(8,2)),
	('Khatia', 'Gobejishvili', 'Cashier', 'FuelNet Kutaisi', DATE '2023-02-14', '+99599111007', 'khatia.g@fuelnet.ge', 1800.00::NUMERIC(8,2)),
	('Lasha', 'Bezhiashvili', 'Supervisor', 'FuelNet Kutaisi', DATE '2019-07-01', '+99599111008', 'lasha.b@fuelnet.ge', 2800.00::NUMERIC(8,2)),
	('Manana', 'Vardanidze', 'Cashier', 'FuelNet Gori', DATE '2021-11-20', '+99599111009', 'manana.v@fuelnet.ge', 1800.00::NUMERIC(8,2)),
	('Giga', 'Pirtskhalava', 'Manager', 'FuelNet Gori', DATE '2018-05-10', '+99599111010', 'giga.p@fuelnet.ge', 3500.00::NUMERIC(8,2)),
	('Tatia', 'Gelashvili', 'Technician', 'FuelNet Rustavi', DATE '2020-08-30', '+99599111011', 'tatia.g@fuelnet.ge', 2200.00::NUMERIC(8,2)),
	('Giorgi', 'Khutsishvili', 'Manager', 'FuelNet Rustavi', DATE '2015-03-25', '+99599111012', 'giorgi.k@fuelnet.ge', 3500.00::NUMERIC(8,2)),
	('Nana', 'Elizbarashvili', 'Cashier', 'FuelNet Telavi', DATE '2022-09-01', '+99599111013', 'nana.e@fuelnet.ge', 1800.00::NUMERIC(8,2)),
	('Tornike', 'Basilashvili', 'Manager', 'FuelNet Telavi', DATE '2019-01-15', '+99599111014', 'tornike.b@fuelnet.ge', 3500.00::NUMERIC(8,2)),
	('Mari', 'Javakhishvili', 'Cashier', 'FuelNet Zugdidi', DATE '2023-06-01', '+99599111015', 'mari.j@fuelnet.ge', 1800.00::NUMERIC(8,2)),
	('Nika', 'Mchelidze', 'Security', 'FuelNet Zugdidi', DATE '2021-04-10', '+99599111016', 'nika.m@fuelnet.ge', 1600.00::NUMERIC(8,2))
) AS src (first_name, last_name, role_name, station_name, hire_date, phone, email, salary)
INNER JOIN fuel_network.employee_roles er ON lower(er.role_name) = lower(src.role_name)
INNER JOIN fuel_network.stations st ON lower(st.station_name) = lower(src.station_name)
WHERE NOT EXISTS (
	SELECT 1
	FROM fuel_network.employees e
	WHERE lower(e.first_name) = lower(src.first_name)
	  AND lower(e.last_name) = lower(src.last_name)
	  AND e.station_id = st.station_id
)
RETURNING employee_id, first_name, last_name, station_id, role_id, salary;


-- -----------------------------------------------------------------------------
-- Transaction: deliveries
-- reference_no is used in WHERE NOT EXISTS because it is the natural
-- business key for a delivery document.
-- -----------------------------------------------------------------------------
INSERT INTO fuel_network.deliveries (
	station_id, supplier_id, delivery_date, reference_no
)
SELECT
	st.station_id,
	sup.supplier_id,
	src.delivery_date,
	src.reference_no
FROM (
	VALUES
	('FuelNet Vake', 'Georgian Oil Company', TIMESTAMPTZ '2026-01-08 07:00:00+04', 'WB-2026-0001'),
	('FuelNet Vake', 'Socar Georgia Petroleum', TIMESTAMPTZ '2026-02-10 07:30:00+04', 'WB-2026-0012'),
	('FuelNet Saburtalo', 'Georgian Oil Company', TIMESTAMPTZ '2026-01-15 08:00:00+04', 'WB-2026-0002'),
	('FuelNet Saburtalo', 'Wissol Petroleum', TIMESTAMPTZ '2026-03-05 07:00:00+04', 'WB-2026-0020'),
	('FuelNet Batumi South', 'Lukoil Georgia LLC', TIMESTAMPTZ '2026-01-20 06:30:00+04', 'WB-2026-0003'),
	('FuelNet Batumi South', 'Lukoil Georgia LLC', TIMESTAMPTZ '2026-03-18 07:00:00+04', 'WB-2026-0025'),
	('FuelNet Kutaisi', 'Wissol Petroleum', TIMESTAMPTZ '2026-01-22 08:30:00+04', 'WB-2026-0004'),
	('FuelNet Kutaisi', 'Georgian Oil Company', TIMESTAMPTZ '2026-04-02 07:00:00+04', 'WB-2026-0030'),
	('FuelNet Gori', 'Gulf Energy GE', TIMESTAMPTZ '2026-02-01 07:00:00+04', 'WB-2026-0010'),
	('FuelNet Gori', 'Gulf Energy GE', TIMESTAMPTZ '2026-04-05 08:00:00+04', 'WB-2026-0033'),
	('FuelNet Rustavi', 'AzPetrol International', TIMESTAMPTZ '2026-02-15 09:00:00+04', 'WB-2026-0015'),
	('FuelNet Telavi', 'Wissol Petroleum', TIMESTAMPTZ '2026-03-01 07:30:00+04', 'WB-2026-0018'),
	('FuelNet Zugdidi', 'Georgian Oil Company', TIMESTAMPTZ '2026-03-20 08:00:00+04', 'WB-2026-0027'),
	('FuelNet Zugdidi', 'Socar Georgia Petroleum', TIMESTAMPTZ '2026-04-10 07:00:00+04', 'WB-2026-0035')
) AS src (station_name, supplier_name, delivery_date, reference_no)
INNER JOIN fuel_network.stations st ON lower(st.station_name) = lower(src.station_name)
INNER JOIN fuel_network.suppliers sup ON lower(sup.supplier_name) = lower(src.supplier_name)
WHERE NOT EXISTS (
	SELECT 1
	FROM fuel_network.deliveries d
	WHERE d.reference_no = src.reference_no
)
RETURNING delivery_id, reference_no, delivery_date, station_id, supplier_id;


-- -----------------------------------------------------------------------------
-- Transaction: delivery_items
-- delivery_id resolved by reference_no; fuel_type_id resolved by name.
-- ON CONFLICT on the composite UNIQUE (delivery_id, fuel_type_id) is safe
-- because a waybill for the same fuel type should never change after the fact.
-- -----------------------------------------------------------------------------
INSERT INTO fuel_network.delivery_items (
	delivery_id, fuel_type_id, quantity_l, unit_cost
)
SELECT
	d.delivery_id,
	ft.fuel_type_id,
	src.quantity_l,
	src.unit_cost
FROM (
	VALUES
	('WB-2026-0001', 'Regular 92', 8000.00::NUMERIC(10,2), 2.10::NUMERIC(6,3)),
	('WB-2026-0001', 'Diesel', 10000.00::NUMERIC(10,2), 2.25::NUMERIC(6,3)),
	('WB-2026-0012', 'Premium 95', 6000.00::NUMERIC(10,2), 2.30::NUMERIC(6,3)),
	('WB-2026-0012', 'Diesel', 8000.00::NUMERIC(10,2), 2.28::NUMERIC(6,3)),
	('WB-2026-0002', 'Regular 92', 7000.00::NUMERIC(10,2), 2.10::NUMERIC(6,3)),
	('WB-2026-0002', 'LPG', 5000.00::NUMERIC(10,2), 0.90::NUMERIC(6,3)),
	('WB-2026-0020', 'Premium 95', 6000.00::NUMERIC(10,2), 2.32::NUMERIC(6,3)),
	('WB-2026-0020', 'LPG', 4000.00::NUMERIC(10,2), 0.92::NUMERIC(6,3)),
	('WB-2026-0003', 'Regular 92', 6000.00::NUMERIC(10,2), 2.12::NUMERIC(6,3)),
	('WB-2026-0003', 'Super 98', 4000.00::NUMERIC(10,2), 2.65::NUMERIC(6,3)),
	('WB-2026-0025', 'Diesel', 8000.00::NUMERIC(10,2), 2.27::NUMERIC(6,3)),
	('WB-2026-0025', 'Super 98', 3000.00::NUMERIC(10,2), 2.68::NUMERIC(6,3)),
	('WB-2026-0004', 'Regular 92', 7000.00::NUMERIC(10,2), 2.10::NUMERIC(6,3)),
	('WB-2026-0004', 'Diesel', 9000.00::NUMERIC(10,2), 2.24::NUMERIC(6,3)),
	('WB-2026-0030', 'Premium 95', 5000.00::NUMERIC(10,2), 2.31::NUMERIC(6,3)),
	('WB-2026-0010', 'Regular 92', 6000.00::NUMERIC(10,2), 2.11::NUMERIC(6,3)),
	('WB-2026-0010', 'LPG', 3000.00::NUMERIC(10,2), 0.91::NUMERIC(6,3)),
	('WB-2026-0033', 'Premium 95', 5000.00::NUMERIC(10,2), 2.33::NUMERIC(6,3)),
	('WB-2026-0015', 'Regular 92', 5000.00::NUMERIC(10,2), 2.09::NUMERIC(6,3)),
	('WB-2026-0015', 'Premium Diesel', 4000.00::NUMERIC(10,2), 2.50::NUMERIC(6,3)),
	('WB-2026-0018', 'Regular 92', 5000.00::NUMERIC(10,2), 2.11::NUMERIC(6,3)),
	('WB-2026-0018', 'Diesel', 6000.00::NUMERIC(10,2), 2.26::NUMERIC(6,3)),
	('WB-2026-0027', 'Regular 92', 5000.00::NUMERIC(10,2), 2.10::NUMERIC(6,3)),
	('WB-2026-0027', 'Diesel', 7000.00::NUMERIC(10,2), 2.25::NUMERIC(6,3)),
	('WB-2026-0035', 'Premium 95', 4000.00::NUMERIC(10,2), 2.30::NUMERIC(6,3)),
	('WB-2026-0035', 'Diesel', 6000.00::NUMERIC(10,2), 2.27::NUMERIC(6,3))
) AS src (reference_no, fuel_type_name, quantity_l, unit_cost)
INNER JOIN fuel_network.deliveries d ON d.reference_no = src.reference_no
INNER JOIN fuel_network.fuel_types ft ON lower(ft.fuel_type_name) = lower(src.fuel_type_name)
ON CONFLICT (delivery_id, fuel_type_id) DO NOTHING
RETURNING delivery_item_id, delivery_id, fuel_type_id, quantity_l, unit_cost;


-- -----------------------------------------------------------------------------
-- Transaction: fuel_sales
-- All FKs (station, fuel type, employee, customer) resolved by natural keys.
-- NULL in cust_first/cust_last = anonymous / walk-in sale; customer_id = NULL.
--
-- Why LEFT JOIN on customers (not INNER JOIN):
--   A LEFT JOIN allows rows where cust_first/cust_last IS NULL to produce
--   a NULL customer_id, which is correct for anonymous sales. An INNER JOIN
--   would silently drop all anonymous sale rows because NULL does not match
--   any row in customers. The WHERE guard at the end of the query ensures
--   that if a customer name IS provided but no matching row is found
--   (e.g. a typo in the name), that row is skipped rather than silently
--   inserted with a NULL customer_id, which would misrepresent a named
--   customer as an anonymous sale.
-- -----------------------------------------------------------------------------
INSERT INTO fuel_network.fuel_sales (
	station_id, fuel_type_id, employee_id, customer_id,
	sale_timestamp, quantity_l, price_per_litre, payment_method
)
SELECT
	st.station_id,
	ft.fuel_type_id,
	e.employee_id,
	c.customer_id,
	src.sale_timestamp,
	src.quantity_l,
	src.price_per_litre,
	src.payment_method
FROM (
	VALUES
	-- FuelNet Vake - January 2026
	('FuelNet Vake', 'Regular 92', 'Irakli', 'Dolidze', 'Nino', 'Lomidze', TIMESTAMPTZ '2026-01-10 08:15:00+04', 40.000::NUMERIC(8,3), 2.450::NUMERIC(6,3), 'Loyalty'),
	('FuelNet Vake', 'Premium 95', 'Irakli', 'Dolidze', 'Giorgi', 'Beridze', TIMESTAMPTZ '2026-01-11 12:30:00+04', 30.000::NUMERIC(8,3), 2.750::NUMERIC(6,3), 'Card'),
	('FuelNet Vake', 'Diesel', 'Irakli', 'Dolidze', NULL, NULL, TIMESTAMPTZ '2026-01-13 17:00:00+04', 60.000::NUMERIC(8,3), 2.650::NUMERIC(6,3), 'Cash'),
	('FuelNet Vake', 'Regular 92', 'Irakli', 'Dolidze', 'Tamara', 'Kvaratskhelia', TIMESTAMPTZ '2026-01-18 09:45:00+04', 35.000::NUMERIC(8,3), 2.450::NUMERIC(6,3), 'Mobile'),
	('FuelNet Vake', 'Premium 95', 'Irakli', 'Dolidze', NULL, NULL, TIMESTAMPTZ '2026-01-22 14:20:00+04', 25.000::NUMERIC(8,3), 2.750::NUMERIC(6,3), 'Cash'),
	('FuelNet Vake', 'Diesel', 'Irakli', 'Dolidze', 'Davit', 'Jishkariani', TIMESTAMPTZ '2026-01-28 16:10:00+04', 80.000::NUMERIC(8,3), 2.650::NUMERIC(6,3), 'Card'),
	-- FuelNet Vake - February 2026 (price update row in fuel_prices applies from Feb 1)
	('FuelNet Vake', 'Regular 92', 'Irakli', 'Dolidze', NULL, NULL, TIMESTAMPTZ '2026-02-05 08:00:00+04', 45.000::NUMERIC(8,3), 2.480::NUMERIC(6,3), 'Cash'),
	('FuelNet Vake', 'Diesel', 'Irakli', 'Dolidze', 'Mariam', 'Tsiklauri', TIMESTAMPTZ '2026-02-12 11:30:00+04', 55.000::NUMERIC(8,3), 2.700::NUMERIC(6,3), 'Loyalty'),
	('FuelNet Vake', 'Premium 95', 'Irakli', 'Dolidze', NULL, NULL, TIMESTAMPTZ '2026-02-20 15:45:00+04', 28.000::NUMERIC(8,3), 2.750::NUMERIC(6,3), 'Card'),
	-- FuelNet Vake - March 2026
	('FuelNet Vake', 'Regular 92', 'Irakli', 'Dolidze', 'Sandro', 'Gabrichidze', TIMESTAMPTZ '2026-03-03 09:20:00+04', 38.000::NUMERIC(8,3), 2.480::NUMERIC(6,3), 'Loyalty'),
	('FuelNet Vake', 'Diesel', 'Irakli', 'Dolidze', NULL, NULL, TIMESTAMPTZ '2026-03-15 13:00:00+04', 70.000::NUMERIC(8,3), 2.700::NUMERIC(6,3), 'Cash'),
	('FuelNet Vake', 'Premium 95', 'Irakli', 'Dolidze', 'Ekaterine', 'Nakashidze', TIMESTAMPTZ '2026-03-25 17:30:00+04', 22.000::NUMERIC(8,3), 2.750::NUMERIC(6,3), 'Mobile'),
	-- FuelNet Saburtalo - January + February + March 2026
	('FuelNet Saburtalo', 'Regular 92', 'Salome', 'Maisuradze', 'Nino', 'Lomidze', TIMESTAMPTZ '2026-01-09 10:00:00+04', 42.000::NUMERIC(8,3), 2.450::NUMERIC(6,3), 'Loyalty'),
	('FuelNet Saburtalo', 'Premium 95', 'Salome', 'Maisuradze', 'Giorgi', 'Beridze', TIMESTAMPTZ '2026-01-14 13:15:00+04', 32.000::NUMERIC(8,3), 2.750::NUMERIC(6,3), 'Card'),
	('FuelNet Saburtalo', 'LPG', 'Salome', 'Maisuradze', NULL, NULL, TIMESTAMPTZ '2026-01-19 08:30:00+04', 20.000::NUMERIC(8,3), 1.200::NUMERIC(6,3), 'Cash'),
	('FuelNet Saburtalo', 'Regular 92', 'Salome', 'Maisuradze', 'Levan', 'Suladze', TIMESTAMPTZ '2026-02-08 11:00:00+04', 38.000::NUMERIC(8,3), 2.450::NUMERIC(6,3), 'Loyalty'),
	('FuelNet Saburtalo', 'LPG', 'Salome', 'Maisuradze', NULL, NULL, TIMESTAMPTZ '2026-02-18 14:30:00+04', 25.000::NUMERIC(8,3), 1.200::NUMERIC(6,3), 'Cash'),
	('FuelNet Saburtalo', 'Premium 95', 'Salome', 'Maisuradze', 'Tamara', 'Kvaratskhelia', TIMESTAMPTZ '2026-03-08 10:45:00+04', 28.000::NUMERIC(8,3), 2.750::NUMERIC(6,3), 'Mobile'),
	-- FuelNet Batumi South - February + March 2026
	('FuelNet Batumi South', 'Regular 92', 'Tinatin', 'Apakidze', NULL, NULL, TIMESTAMPTZ '2026-02-03 09:00:00+04', 50.000::NUMERIC(8,3), 2.460::NUMERIC(6,3), 'Cash'),
	('FuelNet Batumi South', 'Super 98', 'Tinatin', 'Apakidze', 'Davit', 'Jishkariani', TIMESTAMPTZ '2026-02-14 12:00:00+04', 20.000::NUMERIC(8,3), 3.100::NUMERIC(6,3), 'Card'),
	('FuelNet Batumi South', 'Diesel', 'Tinatin', 'Apakidze', NULL, NULL, TIMESTAMPTZ '2026-03-10 16:00:00+04', 65.000::NUMERIC(8,3), 2.660::NUMERIC(6,3), 'Cash'),
	('FuelNet Batumi South', 'Regular 92', 'Tinatin', 'Apakidze', 'Mariam', 'Tsiklauri', TIMESTAMPTZ '2026-03-22 10:30:00+04', 40.000::NUMERIC(8,3), 2.460::NUMERIC(6,3), 'Loyalty'),
	-- FuelNet Kutaisi - January + April 2026
	('FuelNet Kutaisi', 'Regular 92', 'Khatia', 'Gobejishvili', NULL, NULL, TIMESTAMPTZ '2026-01-25 08:00:00+04', 44.000::NUMERIC(8,3), 2.440::NUMERIC(6,3), 'Cash'),
	('FuelNet Kutaisi', 'Diesel', 'Khatia', 'Gobejishvili', 'Sandro', 'Gabrichidze', TIMESTAMPTZ '2026-01-30 15:00:00+04', 75.000::NUMERIC(8,3), 2.640::NUMERIC(6,3), 'Card'),
	('FuelNet Kutaisi', 'Premium 95', 'Khatia', 'Gobejishvili', 'Ekaterine', 'Nakashidze', TIMESTAMPTZ '2026-04-03 11:00:00+04', 30.000::NUMERIC(8,3), 2.740::NUMERIC(6,3), 'Loyalty'),
	('FuelNet Kutaisi', 'Regular 92', 'Khatia', 'Gobejishvili', NULL, NULL, TIMESTAMPTZ '2026-04-08 13:30:00+04', 48.000::NUMERIC(8,3), 2.440::NUMERIC(6,3), 'Cash'),
	-- FuelNet Gori - February + April 2026
	('FuelNet Gori', 'Regular 92', 'Manana', 'Vardanidze', NULL, NULL, TIMESTAMPTZ '2026-02-06 09:30:00+04', 36.000::NUMERIC(8,3), 2.450::NUMERIC(6,3), 'Cash'),
	('FuelNet Gori', 'LPG', 'Manana', 'Vardanidze', 'Levan', 'Suladze', TIMESTAMPTZ '2026-02-20 14:00:00+04', 18.000::NUMERIC(8,3), 1.190::NUMERIC(6,3), 'Loyalty'),
	('FuelNet Gori', 'Premium 95', 'Manana', 'Vardanidze', 'Nino', 'Lomidze', TIMESTAMPTZ '2026-04-06 10:15:00+04', 26.000::NUMERIC(8,3), 2.750::NUMERIC(6,3), 'Mobile'),
	('FuelNet Gori', 'Regular 92', 'Manana', 'Vardanidze', NULL, NULL, TIMESTAMPTZ '2026-04-12 15:30:00+04', 40.000::NUMERIC(8,3), 2.450::NUMERIC(6,3), 'Cash'),
	-- FuelNet Telavi - March + April 2026
	('FuelNet Telavi', 'Regular 92', 'Nana', 'Elizbarashvili', NULL, NULL, TIMESTAMPTZ '2026-03-04 08:45:00+04', 35.000::NUMERIC(8,3), 2.460::NUMERIC(6,3), 'Cash'),
	('FuelNet Telavi', 'Diesel', 'Nana', 'Elizbarashvili', 'Giorgi', 'Beridze', TIMESTAMPTZ '2026-03-16 12:00:00+04', 60.000::NUMERIC(8,3), 2.660::NUMERIC(6,3), 'Card'),
	('FuelNet Telavi', 'Premium 95', 'Nana', 'Elizbarashvili', 'Tamara', 'Kvaratskhelia', TIMESTAMPTZ '2026-04-09 11:30:00+04', 24.000::NUMERIC(8,3), 2.760::NUMERIC(6,3), 'Loyalty'),
	('FuelNet Telavi', 'Regular 92', 'Nana', 'Elizbarashvili', NULL, NULL, TIMESTAMPTZ '2026-04-15 16:45:00+04', 42.000::NUMERIC(8,3), 2.460::NUMERIC(6,3), 'Cash'),
	-- FuelNet Zugdidi - March + April 2026
	('FuelNet Zugdidi', 'Regular 92', 'Mari', 'Javakhishvili', NULL, NULL, TIMESTAMPTZ '2026-03-21 09:00:00+04', 38.000::NUMERIC(8,3), 2.450::NUMERIC(6,3), 'Cash'),
	('FuelNet Zugdidi', 'Diesel', 'Mari', 'Javakhishvili', 'Davit', 'Jishkariani', TIMESTAMPTZ '2026-04-01 13:00:00+04', 55.000::NUMERIC(8,3), 2.650::NUMERIC(6,3), 'Card'),
	('FuelNet Zugdidi', 'Premium 95', 'Mari', 'Javakhishvili', 'Sandro', 'Gabrichidze', TIMESTAMPTZ '2026-04-11 10:00:00+04', 27.000::NUMERIC(8,3), 2.750::NUMERIC(6,3), 'Loyalty'),
	('FuelNet Zugdidi', 'Regular 92', 'Mari', 'Javakhishvili', NULL, NULL, TIMESTAMPTZ '2026-04-18 15:00:00+04', 43.000::NUMERIC(8,3), 2.450::NUMERIC(6,3), 'Cash')
) AS src (
	station_name, fuel_type_name,
	emp_first, emp_last,
	cust_first, cust_last,
	sale_timestamp, quantity_l, price_per_litre, payment_method
)
INNER JOIN fuel_network.stations st ON lower(st.station_name) = lower(src.station_name)
INNER JOIN fuel_network.fuel_types ft ON lower(ft.fuel_type_name) = lower(src.fuel_type_name)
INNER JOIN fuel_network.employees e ON lower(e.first_name) = lower(src.emp_first) AND
									   lower(e.last_name) = lower(src.emp_last) AND
									   e.station_id = st.station_id
LEFT JOIN fuel_network.customers c ON lower(c.first_name) = lower(src.cust_first) AND
									  lower(c.last_name) = lower(src.cust_last)
-- Skip rows where a customer name was provided but no matching row was found.
-- This prevents silently inserting NULL for a customer that should exist.
-- Rows where cust_first IS NULL are anonymous sales and are always included.
WHERE (src.cust_first IS NULL OR c.customer_id IS NOT NULL)
RETURNING sale_id, station_id, fuel_type_id, sale_timestamp, total_amount, payment_method;


-----------------------------------------------------------------------------
-- PART 3: FUNCTIONS
-----------------------------------------------------------------------------


-----------------------------------------------------------------------------
-- FUNCTION 5.1: update_fuel_sale_column
/*
Updates a single column in fuel_sales for a given sale_id.
	Input: sale_id, column name, new value (as text).
	Output: TEXT confirmation message.

Why dynamic SQL with a whitelist:
	The column name cannot be passed as a bind parameter in PostgreSQL.
	To avoid SQL injection via p_column_name, the function checks it against
	an explicit allowed list before building the query with format().
	The new value is always passed as a bind parameter via USING, so it is
	safe regardless of its content.

total_amount is excluded from the allowed list because it is a GENERATED
column and will raise an error if you try to update it manually.
*/

CREATE OR REPLACE FUNCTION fuel_network.update_fuel_sale_column(
	p_sale_id INTEGER,
	p_column_name TEXT,
	p_new_value TEXT
)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
	v_allowed_columns TEXT[] := ARRAY[
		'payment_method',
		'quantity_l',
		'price_per_litre',
		'sale_timestamp'
	];
	v_query TEXT;
BEGIN
	-- Reject column names not in the allowed list before touching the DB.
	IF p_column_name != ALL(v_allowed_columns) THEN
		RAISE EXCEPTION
			'Column "%" is not updatable via this function. Allowed columns: %',
			p_column_name,
			array_to_string(v_allowed_columns, ', ');
	END IF;

	-- Confirm the target row exists before attempting the update.
	IF NOT EXISTS (
		SELECT 1
		FROM fuel_network.fuel_sales
		WHERE sale_id = p_sale_id
	) THEN
		RAISE EXCEPTION 'No fuel_sale found with sale_id = %', p_sale_id;
	END IF;

	-- Build the dynamic UPDATE. %I quotes the column name safely.
	-- The cast ensures the text value is converted to the correct column type.
	v_query := format(
		'UPDATE fuel_network.fuel_sales
		 SET %I = $1::text::%s
		 WHERE sale_id = $2',
		p_column_name,
		CASE p_column_name
			WHEN 'quantity_l' THEN 'NUMERIC(8,3)'
			WHEN 'price_per_litre' THEN 'NUMERIC(6,3)'
			WHEN 'sale_timestamp' THEN 'TIMESTAMPTZ'
			ELSE 'TEXT'
		END
	);

	EXECUTE v_query USING p_new_value, p_sale_id;

	RETURN format(
		'SUCCESS: sale_id %s - column "%s" updated to "%s".',
		p_sale_id,
		p_column_name,
		p_new_value
	);
END;
$$;


-----------------------------------------------------------------------------
-- FUNCTION 5.2: add_fuel_sale
/*
Inserts a new row into fuel_sales. All FK values are resolved from natural
keys (names) inside the function so the caller never needs surrogate IDs.

Input:
	p_station_name     - station natural key
	p_fuel_type_name   - fuel type natural key
	p_employee_first   - cashier first name
	p_employee_last    - cashier last name
	p_customer_loyalty - loyalty card number; pass NULL for anonymous sales
	p_sale_timestamp   - timestamp of the sale
	p_quantity_l       - litres dispensed
	p_price_per_litre  - retail price at the time of the sale
	p_payment_method   - Cash | Card | Mobile | Loyalty

Output: TEXT confirmation message with the generated sale_id.

Why the employee check includes station_id:
	Two employees at different stations can share the same name. Scoping
	the lookup to the station prevents ambiguity.
*/

CREATE OR REPLACE FUNCTION fuel_network.add_fuel_sale(
	p_station_name TEXT,
	p_fuel_type_name TEXT,
	p_employee_first TEXT,
	p_employee_last TEXT,
	p_customer_loyalty TEXT,
	p_sale_timestamp TIMESTAMPTZ,
	p_quantity_l NUMERIC(8, 3),
	p_price_per_litre NUMERIC(6, 3),
	p_payment_method TEXT
)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
	v_station_id INTEGER;
	v_fuel_type_id INTEGER;
	v_employee_id INTEGER;
	v_customer_id INTEGER;
	v_new_sale_id INTEGER;
BEGIN
	-- Resolve station_id.
	SELECT station_id
	INTO v_station_id
	FROM fuel_network.stations
	WHERE lower(station_name) = lower(p_station_name);

	IF v_station_id IS NULL THEN
		RAISE EXCEPTION 'Station "%" not found.', p_station_name;
	END IF;

	-- Resolve fuel_type_id.
	SELECT fuel_type_id
	INTO v_fuel_type_id
	FROM fuel_network.fuel_types
	WHERE lower(fuel_type_name) = lower(p_fuel_type_name);

	IF v_fuel_type_id IS NULL THEN
		RAISE EXCEPTION 'Fuel type "%" not found.', p_fuel_type_name;
	END IF;

	-- Resolve employee_id. Scoped to the station to avoid name collisions.
	SELECT employee_id
	INTO v_employee_id
	FROM fuel_network.employees
	WHERE lower(first_name) = lower(p_employee_first)
	  AND lower(last_name) = lower(p_employee_last)
	  AND station_id = v_station_id
	  AND is_active = TRUE;

	IF v_employee_id IS NULL THEN
		RAISE EXCEPTION
			'Active employee "% %" not found at station "%".',
			p_employee_first, p_employee_last, p_station_name;
	END IF;

	-- Resolve customer_id. NULL is valid for anonymous / walk-in sales.
	IF p_customer_loyalty IS NOT NULL THEN
		SELECT customer_id
		INTO v_customer_id
		FROM fuel_network.customers
		WHERE loyalty_card_number = p_customer_loyalty;

		IF v_customer_id IS NULL THEN
			RAISE EXCEPTION
				'Customer with loyalty card "%" not found.',
				p_customer_loyalty;
		END IF;
	ELSE
		v_customer_id := NULL;
	END IF;

	-- Insert the new sale and capture the generated sale_id.
	INSERT INTO fuel_network.fuel_sales (
		station_id,
		fuel_type_id,
		employee_id,
		customer_id,
		sale_timestamp,
		quantity_l,
		price_per_litre,
		payment_method
	)
	VALUES (
		v_station_id,
		v_fuel_type_id,
		v_employee_id,
		v_customer_id,
		p_sale_timestamp,
		p_quantity_l,
		p_price_per_litre,
		p_payment_method
	)
	RETURNING sale_id INTO v_new_sale_id;

	RETURN format(
		'SUCCESS: sale_id = %s | station = "%s" | fuel = "%s" | '
		'litres = %s | total = %s GEL | method = "%s".',
		v_new_sale_id,
		p_station_name,
		p_fuel_type_name,
		p_quantity_l,
		ROUND(p_quantity_l * p_price_per_litre, 2),
		p_payment_method
	);
END;
$$;

-----------------------------------------------------------------------------
-- PART 4: ANALYTICS VIEW
-----------------------------------------------------------------------------
/*
Shows aggregated sales data for the most recently added calendar quarter.

Why the quarter boundary is derived dynamically:
	Hardcoding a date range would require updating the view whenever new data
	is added. DATE_TRUNC('quarter', MAX(sale_timestamp)) always resolves to
	the correct Q-start regardless of when the view is queried.

What is excluded:
	Surrogate keys (sale_id, station_id, etc.) and individual sale rows.
	The view aggregates to station + fuel type level.

What is included:
	Region, station, city, fuel type, sale count, total litres, total revenue,
	average price per litre, and a breakdown of sales by payment method.
*/

CREATE OR REPLACE VIEW fuel_network.v_quarterly_sales_analytics AS
WITH latest_quarter AS (
	-- Find the calendar quarter that contains the most recent sale.
	SELECT
		DATE_TRUNC('quarter', MAX(sale_timestamp)) AS q_start,
		DATE_TRUNC('quarter', MAX(sale_timestamp)) + INTERVAL '3 months' AS q_end
	FROM fuel_network.fuel_sales
),
sales_in_quarter AS (
	SELECT
		r.region_name,
		st.station_name,
		st.city,
		ft.fuel_type_name,
		fs.quantity_l,
		fs.total_amount,
		fs.payment_method
	FROM fuel_network.fuel_sales fs
	CROSS JOIN latest_quarter lq
	INNER JOIN fuel_network.stations st ON fs.station_id = st.station_id
	INNER JOIN fuel_network.regions r ON st.region_id = r.region_id
	INNER JOIN fuel_network.fuel_types ft ON fs.fuel_type_id = ft.fuel_type_id
	-- Filter to the latest quarter only.
	WHERE fs.sale_timestamp >= lq.q_start
	  AND fs.sale_timestamp < lq.q_end
)
SELECT
	(SELECT q_start FROM latest_quarter) AS quarter_start,
	(SELECT q_end FROM latest_quarter) - INTERVAL '1 day' AS quarter_end,
	region_name,
	station_name,
	city,
	fuel_type_name,
	COUNT(*) AS sale_count,
	ROUND(SUM(quantity_l), 2) AS total_litres_sold,
	ROUND(SUM(total_amount), 2) AS total_revenue_gel,
	-- NULLIF guard is defensive; quantity_l has a CHECK > 0 so division by
	-- zero cannot occur in practice, but the guard keeps the expression safe
	-- if the constraint is ever relaxed or data is loaded via a different path.
	ROUND(AVG(total_amount / NULLIF(quantity_l, 0)), 3) AS avg_price_per_litre,
	COUNT(*) FILTER (WHERE payment_method = 'Cash') AS cash_sales,
	COUNT(*) FILTER (WHERE payment_method = 'Card') AS card_sales,
	COUNT(*) FILTER (WHERE payment_method = 'Mobile') AS mobile_sales,
	COUNT(*) FILTER (WHERE payment_method = 'Loyalty') AS loyalty_sales
FROM sales_in_quarter
GROUP BY
	region_name,
	station_name,
	city,
	fuel_type_name
ORDER BY
	total_revenue_gel DESC,
	station_name,
	fuel_type_name;


-----------------------------------------------------------------------------
-- PART 5: READ-ONLY ROLE FOR MANAGER
-----------------------------------------------------------------------------
/*
Security best practices applied:

	- NOSUPERUSER, NOCREATEDB, NOCREATEROLE: role cannot escalate its own
	  privileges or create other roles.
	- NOINHERIT: the role does not automatically inherit permissions from any
	  group it might be added to in future. Permissions are only what is
	  explicitly granted here.
	- CONNECTION LIMIT 5: prevents a misconfigured client from exhausting
	  all available connections.
	- Password is a placeholder. It must be changed before production use:
	  ALTER ROLE manager_readonly PASSWORD 'new_secure_password';
	- GRANT USAGE on schema is required in addition to table-level SELECT.
	  Without it, the role can see the schema exists but cannot access objects.
	- ALTER DEFAULT PRIVILEGES ensures tables created after this script is run
	  are also automatically accessible to the role.
	- Explicit GRANT SELECT on the view is included because in some PostgreSQL
	  configurations GRANT SELECT ON ALL TABLES does not cover views that are
	  created in the same transaction or session. Granting it explicitly
	  guarantees access regardless of execution order.
*/

DO $$
BEGIN
	IF NOT EXISTS (
		SELECT 1 FROM pg_roles WHERE rolname = 'manager_readonly'
	) THEN
		CREATE ROLE manager_readonly
			LOGIN
			PASSWORD 'ChangeMe_Before_Production!'
			NOSUPERUSER
			NOCREATEDB
			NOCREATEROLE
			NOINHERIT
			CONNECTION LIMIT 5;
	END IF;
END;
$$;

-- Schema access is a prerequisite for any object-level access in PostgreSQL.
GRANT USAGE ON SCHEMA fuel_network TO manager_readonly;

-- Grant SELECT on all current tables.
GRANT SELECT ON ALL TABLES IN SCHEMA fuel_network TO manager_readonly;

-- GRANT SELECT ON ALL TABLES covers views in most cases, but an explicit
-- grant ensures the view is accessible regardless of execution context.
GRANT SELECT ON fuel_network.v_quarterly_sales_analytics TO manager_readonly;

-- Ensure future tables created in this schema also get SELECT automatically.
ALTER DEFAULT PRIVILEGES IN SCHEMA fuel_network
	GRANT SELECT ON TABLES TO manager_readonly;