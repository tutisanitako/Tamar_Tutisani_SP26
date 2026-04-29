-- Task: create a physical DB

-------------------------------------------------------
/*
MODEL CHANGES FROM LOGICAL VERSION:
1. I decided to drop the regions table. I just moved region_name directly
   into the countries table as a VARCHAR(100). It keeps the schema cleaner
   and reduces the table count to 15, but because the hierarchy (Country -> City)
   is still solid, we aren't losing any structural integrity or breaking 3NF.
2. I also got rid of the candidate_preferred_locations bridge table. Having a
   full Many-to-Many setup just to track where someone wants to work was adding
   unnecessary complexity. Instead, I added a nullable preferred_city_id foreign
   key directly to the candidates table. It hits the business requirement perfectly
   without the extra baggage of a junction table.
*/

/*
DDL ORDER
Parent tables must be created before child tables that reference them via FK.
If a child table is created first, PostgreSQL will raise:
ERROR: relation "<parent_table>" does not exist
because the FK constraint cannot reference a table that has not yet been
created. The correct order follows the dependency graph:
countries -> cities -> candidates, jobs, interviews
companies -> company_representatives, jobs
candidates -> candidate_skills, work_experience, candidate_services, applications
jobs -> applications
applications -> application_status_history, interviews, placements
services -> candidate_services
skills -> candidate_skills
company_representatives -> interviews
*/

----------------------------------------------------------------------------
-- STEP 0: DATABASE AND SCHEMA SETUP

-- Create database:
-- CREATE DATABASE recruitment_agency;

-- Create schema. IF NOT EXISTS makes the script rerunnable.
CREATE SCHEMA IF NOT EXISTS recruitment;

-- Set search path so all objects are created inside the correct schema.
SET search_path TO recruitment;

----------------------------------------------------------------------------
-- STEP 1: DROP TABLES (reverse dependency order for rerunnability)
-- Dropping in reverse FK order ensures no FK violation errors on re-run.

DROP TABLE IF EXISTS recruitment.placements CASCADE;
DROP TABLE IF EXISTS recruitment.interviews CASCADE;
DROP TABLE IF EXISTS recruitment.application_status_history CASCADE;
DROP TABLE IF EXISTS recruitment.applications CASCADE;
DROP TABLE IF EXISTS recruitment.candidate_services CASCADE;
DROP TABLE IF EXISTS recruitment.work_experience CASCADE;
DROP TABLE IF EXISTS recruitment.candidate_skills CASCADE;
DROP TABLE IF EXISTS recruitment.jobs CASCADE;
DROP TABLE IF EXISTS recruitment.company_representatives CASCADE;
DROP TABLE IF EXISTS recruitment.companies CASCADE;
DROP TABLE IF EXISTS recruitment.candidates CASCADE;
DROP TABLE IF EXISTS recruitment.skills CASCADE;
DROP TABLE IF EXISTS recruitment.services CASCADE;
DROP TABLE IF EXISTS recruitment.cities CASCADE;
DROP TABLE IF EXISTS recruitment.countries CASCADE;

----------------------------------------------------------------------------
-- STEP 2: CREATE PARENT TABLES (no FK dependencies)

----------------------------------------------------------------------------
-- TABLE: countries

CREATE TABLE recruitment.countries (
	country_id SERIAL NOT NULL,
	country_name VARCHAR(100) NOT NULL,
	region_name VARCHAR(100) NOT NULL,

	CONSTRAINT PK_countries_country_id PRIMARY KEY (country_id),

	-- CONSTRAINT: UNIQUE on UPPER(country_name)
	-- Prevents the same country being entered twice regardless of case
	-- (e.g. 'Poland' and 'poland' would be treated as duplicates).
	-- Without this, JOIN queries would return duplicate rows and aggregations
	-- would be inflated.
	CONSTRAINT UQ_countries_country_name UNIQUE (UPPER(country_name))
);


----------------------------------------------------------------------------
-- TABLE: cities

CREATE TABLE recruitment.cities (
	city_id SERIAL NOT NULL,
	city_name VARCHAR(100) NOT NULL,
	country_id INTEGER NOT NULL,

	CONSTRAINT PK_cities_city_id PRIMARY KEY (city_id),

	-- CONSTRAINT: UNIQUE(UPPER(city_name), country_id)
	-- Prevents the same city being entered twice within the same country
	-- regardless of case, while allowing "London, UK" and "London, Canada"
	-- as separate rows.
	-- Without this, duplicate city rows would cause ambiguous FK joins.
	CONSTRAINT UQ_cities_city_name_country_id UNIQUE (UPPER(city_name), country_id),

	-- FK: every city belongs to exactly one country.
	-- If FK is missing, a city could reference a non-existent country_id,
	-- making geographic hierarchy queries return NULL or wrong results.
	CONSTRAINT FK_cities_country_id
		FOREIGN KEY (country_id) REFERENCES recruitment.countries (country_id)
);


----------------------------------------------------------------------------
-- TABLE: skills

CREATE TABLE recruitment.skills (
	skill_id SERIAL NOT NULL,
	skill_name VARCHAR(100) NOT NULL,

	-- CONSTRAINT: category CHECK (specific values only)
	-- Prevents free-text garbage or NULL-equivalent strings.
	-- Without it, skill grouping queries would return inconsistent categories.
	category VARCHAR(50),

	CONSTRAINT PK_skills_skill_id PRIMARY KEY (skill_id),

	-- CONSTRAINT: UNIQUE on UPPER(skill_name)
	-- Prevents duplicate skills differing only by case (e.g. 'Python' vs 'python').
	CONSTRAINT UQ_skills_skill_name UNIQUE (UPPER(skill_name)),

	CONSTRAINT CHK_skills_category
		CHECK (category IN ('technical','soft','language','domain','other')
			   OR category IS NULL)
);


----------------------------------------------------------------------------
-- TABLE: services

CREATE TABLE recruitment.services (
	service_id SERIAL NOT NULL,
	service_name VARCHAR(150) NOT NULL,
	description TEXT,

	-- NUMERIC not FLOAT: monetary values must be exact. FLOAT uses binary
	-- floating point which cannot represent most decimal fractions exactly.
	-- Using FLOAT for price would cause rounding errors in billing and fee calculations.
	price NUMERIC(10,2) NOT NULL,

	CONSTRAINT PK_services_service_id PRIMARY KEY (service_id),

	-- CONSTRAINT: UNIQUE on UPPER(service_name)
	-- Prevents duplicate services differing only by case.
	CONSTRAINT UQ_services_service_name UNIQUE (UPPER(service_name)),

	-- CONSTRAINT: price cannot be negative
	-- Prevents a data entry mistake like price = -50.00.
	-- Without this, billing queries could subtract money from candidates.
	CONSTRAINT CHK_services_price CHECK (price >= 0)
);


----------------------------------------------------------------------------
-- TABLE: companies

CREATE TABLE recruitment.companies (
	company_id SERIAL NOT NULL,
	name VARCHAR(200) NOT NULL,
	industry VARCHAR(100),
	website VARCHAR(255),
	
	-- TIMESTAMPTZ not TIMESTAMP: stores timezone offset so records from
	-- different regions are always comparable in absolute time.
	-- Using plain TIMESTAMP would make "09:00 London" and "09:00 Warsaw"
	-- look identical even though they are 1 hour apart.
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

	CONSTRAINT PK_companies_company_id PRIMARY KEY (company_id),

	-- CONSTRAINT: UNIQUE on UPPER(name)
	-- Prevents one company being entered twice under different cases.
	-- Without this, jobs could be split across duplicate rows and reporting
	-- would double-count postings.
	CONSTRAINT UQ_companies_name UNIQUE (UPPER(name))
);


----------------------------------------------------------------------------
-- STEP 3: CREATE TABLES THAT DEPEND ON STEP-2 TABLES

----------------------------------------------------------------------------
-- TABLE: candidates

CREATE TABLE recruitment.candidates (
	candidate_id SERIAL NOT NULL,
	first_name VARCHAR(100) NOT NULL,
	last_name VARCHAR(100) NOT NULL,
	 
	-- GENERATED ALWAYS AS column.
	-- full_name is always derived from first_name || ' ' || last_name.
	-- Storing it as a plain column would risk it going out of sync with
	-- first_name/last_name and would violate 3NF (transitive dependency on
	-- non-key columns). GENERATED ALWAYS AS ensures it is always consistent
	-- without any application-level logic.
	full_name VARCHAR(201) GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED,
	
	email VARCHAR(255) NOT NULL,
	phone VARCHAR(30),
	
	-- DATE not TIMESTAMPTZ: a birthday is a calendar fact. Timezone conversion
	-- of a DATE would corrupt it (e.g. "1995-03-12" could shift to "1995-03-11"
	-- in a UTC-offset session).
	date_of_birth DATE,
	summary TEXT,
	
	-- FK to cities: nullable because remote candidates may have no preferred city.
	-- If FK is missing, preferred_city_id could reference a deleted or
	-- non-existent city, returning NULL on joins with no error raised.
	preferred_city_id INTEGER,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

	CONSTRAINT PK_candidates_candidate_id PRIMARY KEY (candidate_id),

	-- CONSTRAINT: UNIQUE on UPPER(email)
	-- Email is the natural business key. Duplicates (differing only by case)
	-- would split one person's application history across two profiles.
	CONSTRAINT UQ_candidates_email UNIQUE (UPPER(email)),

	CONSTRAINT FK_candidates_preferred_city_id
		FOREIGN KEY (preferred_city_id) REFERENCES recruitment.cities (city_id)
);


----------------------------------------------------------------------------
-- TABLE: company_representatives

CREATE TABLE recruitment.company_representatives (
	rep_id SERIAL NOT NULL,
	company_id INTEGER NOT NULL,
	first_name VARCHAR(100) NOT NULL,
	last_name VARCHAR(100) NOT NULL,
	email VARCHAR(255) NOT NULL,
	
	-- VARCHAR not INTEGER for phone: preserves leading zeros and country codes
	-- (+48 123 456 789). Storing as INTEGER would silently drop the leading zero
	-- and make international numbers unreadable.
	phone VARCHAR(30),
	position VARCHAR(150),
	is_active BOOLEAN NOT NULL DEFAULT TRUE,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

	CONSTRAINT PK_company_representatives_rep_id PRIMARY KEY (rep_id),

	-- CONSTRAINT: UNIQUE on UPPER(email)
	-- Prevents duplicate rep accounts differing only by email case.
	CONSTRAINT UQ_company_representatives_email UNIQUE (UPPER(email)),

	-- FK: every rep belongs to exactly one company.
	-- If FK is missing, a rep could be assigned to a non-existent company,
	-- making company-level interview reports return wrong or null results.
	CONSTRAINT FK_company_representatives_company_id
		FOREIGN KEY (company_id) REFERENCES recruitment.companies (company_id)
);


----------------------------------------------------------------------------
-- TABLE: jobs

CREATE TABLE recruitment.jobs (
	job_id SERIAL NOT NULL,
	company_id INTEGER NOT NULL,
	city_id INTEGER,
	title VARCHAR(200) NOT NULL,
	description TEXT,
	
	-- NUMERIC not FLOAT: same reasoning as services.price, monetary values
	-- require exact decimal representation. Wrong type risks rounding errors
	-- in salary range filtering and reporting.
	salary_min NUMERIC(12,2),
	salary_max NUMERIC(12,2),
	employment_type VARCHAR(20) NOT NULL,
	work_mode VARCHAR(20) NOT NULL,
	
	-- TIMESTAMPTZ: job postings are visible to users in multiple timezones.
	-- Plain TIMESTAMP would make "posted at 09:00" ambiguous across regions.
	posted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	closed_at TIMESTAMPTZ,
	is_active BOOLEAN NOT NULL DEFAULT TRUE,

	CONSTRAINT PK_jobs_job_id PRIMARY KEY (job_id),

	-- FK: every job must belong to a registered company.
	-- Without FK, jobs could reference deleted or fake company_ids, breaking
	-- company-level vacancy reports.
	CONSTRAINT FK_jobs_company_id
		FOREIGN KEY (company_id) REFERENCES recruitment.companies (company_id),

	-- FK: nullable for fully remote roles that have no physical office.
	-- If FK is missing, city_id could reference a deleted city with no error,
	-- silently returning NULL on location joins.
	CONSTRAINT FK_jobs_city_id
		FOREIGN KEY (city_id) REFERENCES recruitment.cities (city_id),

	-- CONSTRAINT: salary_min cannot be negative
	-- A negative minimum salary is not a valid business value and would corrupt
	-- salary range filtering queries.
	CONSTRAINT CHK_jobs_salary_min
		CHECK (salary_min IS NULL OR salary_min >= 0),

	-- CONSTRAINT: salary_max >= salary_min
	-- Prevents inverted ranges like min=120000, max=50000 which would cause
	-- salary-range filter queries to return zero results silently.
	CONSTRAINT CHK_jobs_salary_max
		CHECK (salary_max IS NULL OR salary_min IS NULL OR salary_max >= salary_min),

	-- CONSTRAINT: employment_type restricted to specific values
	-- Prevents free-text like "fulltime" or "FT" which breaks GROUP BY reports.
	CONSTRAINT CHK_jobs_employment_type
		CHECK (employment_type IN ('full_time', 'part_time', 'contract', 'internship')),

	-- CONSTRAINT: work_mode restricted to specific values
	-- Without this, filters like WHERE work_mode = 'remote' would miss rows
	-- entered as "Remote" or "WFH".
	CONSTRAINT CHK_jobs_work_mode
		CHECK (work_mode IN ('on_site', 'remote', 'hybrid')),

	-- CONSTRAINT: posted_at must be after 2000-01-01
	-- Prevents legacy import placeholder dates like 1900-01-01 corrupting
	-- "days since posted" calculations.
	CONSTRAINT CHK_jobs_posted_at
		CHECK (posted_at > TIMESTAMPTZ '2000-01-01 00:00:00+00')
);


----------------------------------------------------------------------------
-- STEP 4: BRIDGE AND CHILD TABLES

----------------------------------------------------------------------------
-- TABLE: candidate_skills

CREATE TABLE recruitment.candidate_skills (
	candidate_id INTEGER NOT NULL,
	skill_id INTEGER NOT NULL,
	
	-- CONSTRAINT: proficiency_level restricted to specific values
	-- Prevents entries like "good" or "5/10" which make skill-level filtering
	-- impossible and break any UI that displays proficiency badges.
	proficiency_level VARCHAR(50) NOT NULL,
	
	-- DATE not TIMESTAMPTZ: acquired_date is a calendar fact (which month/year
	-- a skill was learned), not a precise moment. TIMESTAMPTZ would imply false
	-- precision and timezone sensitivity that is not meaningful here.
	acquired_date DATE,

	CONSTRAINT PK_candidate_skills_candidate_id_skill_id
		PRIMARY KEY (candidate_id, skill_id),

	-- If FK on candidate_id is missing, skill rows could be orphaned,
	-- making candidate skill searches return incomplete results.
	CONSTRAINT FK_candidate_skills_candidate_id
		FOREIGN KEY (candidate_id) REFERENCES recruitment.candidates (candidate_id),

	-- If FK on skill_id is missing, a skill entry could reference a deleted
	-- skill, making skill-search queries silently miss those candidates.
	CONSTRAINT FK_candidate_skills_skill_id
		FOREIGN KEY (skill_id) REFERENCES recruitment.skills (skill_id),

	CONSTRAINT CHK_candidate_skills_proficiency_level
		CHECK (proficiency_level IN ('beginner', 'intermediate', 'advanced', 'expert'))
);


----------------------------------------------------------------------------
-- TABLE: work_experience

CREATE TABLE recruitment.work_experience (
	exp_id SERIAL NOT NULL,
	candidate_id INTEGER NOT NULL,
	company_name VARCHAR(200) NOT NULL,
	position VARCHAR(150) NOT NULL,
	start_date DATE NOT NULL,
	end_date DATE,
	description TEXT,
	is_current BOOLEAN NOT NULL DEFAULT FALSE,

	CONSTRAINT PK_work_experience_exp_id PRIMARY KEY (exp_id),

	-- If FK is missing, work experience rows could be orphaned from their
	-- candidate, making employment history queries return incomplete profiles.
	CONSTRAINT FK_work_experience_candidate_id
		FOREIGN KEY (candidate_id) REFERENCES recruitment.candidates (candidate_id),

	-- CONSTRAINT: end_date >= start_date
	-- Prevents logically impossible ranges (e.g. ended before starting).
	-- Without this, tenure calculations would return negative durations.
	CONSTRAINT CHK_work_experience_end_date
		CHECK (end_date IS NULL OR end_date >= start_date),

	-- CONSTRAINT: if is_current = true then end_date must be NULL
	-- Prevents a "current job" row that also has an end date, which would be
	-- contradictory and cause is_current filters to return stale records.
	CONSTRAINT CHK_work_experience_is_current
		CHECK (is_current = FALSE OR end_date IS NULL)
);


----------------------------------------------------------------------------
-- TABLE: candidate_services

CREATE TABLE recruitment.candidate_services (
	candidate_service_id SERIAL NOT NULL,
	candidate_id INTEGER NOT NULL,
	service_id INTEGER NOT NULL,
	status VARCHAR(20) NOT NULL DEFAULT 'assigned',
	active_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	active_to TIMESTAMPTZ,
	is_active BOOLEAN NOT NULL DEFAULT TRUE,
	notes TEXT,

	CONSTRAINT PK_candidate_services_candidate_service_id
		PRIMARY KEY (candidate_service_id),

	-- If FK on candidate_id is missing, service delivery rows could be orphaned,
	-- making it impossible to look up which candidate received the service.
	CONSTRAINT FK_candidate_services_candidate_id
		FOREIGN KEY (candidate_id) REFERENCES recruitment.candidates (candidate_id),

	-- If FK on service_id is missing, a delivery row could reference a deleted
	-- service, making it impossible to look up the service name or price.
	CONSTRAINT FK_candidate_services_service_id
		FOREIGN KEY (service_id) REFERENCES recruitment.services (service_id),

	-- CONSTRAINT: status restricted to specific values
	-- Prevents free-text statuses that break stage-funnel reports.
	CONSTRAINT CHK_candidate_services_status
		CHECK (status IN ('assigned', 'in_progress', 'completed', 'cancelled')),

	-- CONSTRAINT: active_to >= active_from
	-- Prevents inverted timestamps that would make duration calculations
	-- return negative values.
	CONSTRAINT CHK_candidate_services_active_range
		CHECK (active_to IS NULL OR active_to >= active_from)
);


----------------------------------------------------------------------------
-- TABLE: applications

CREATE TABLE recruitment.applications (
	application_id SERIAL NOT NULL,
	candidate_id INTEGER NOT NULL,
	job_id INTEGER NOT NULL,
	applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

	CONSTRAINT PK_applications_application_id PRIMARY KEY (application_id),

	-- CONSTRAINT: UNIQUE(candidate_id, job_id)
	-- Prevents the same candidate applying to the same job twice, which would
	-- create two parallel status histories and make placement attribution ambiguous.
	CONSTRAINT UQ_applications_candidate_id_job_id UNIQUE (candidate_id, job_id),

	-- If FK on candidate_id is missing, applications could be orphaned,
	-- making it impossible to identify who submitted the application.
	CONSTRAINT FK_applications_candidate_id
		FOREIGN KEY (candidate_id) REFERENCES recruitment.candidates (candidate_id),

	-- If FK on job_id is missing, applications could reference deleted jobs,
	-- making it impossible to display the vacancy details for an application.
	CONSTRAINT FK_applications_job_id
		FOREIGN KEY (job_id) REFERENCES recruitment.jobs (job_id),

	-- CONSTRAINT: applied_at after 2000-01-01
	-- Prevents placeholder import dates from corrupting application timelines.
	CONSTRAINT CHK_applications_applied_at
		CHECK (applied_at > TIMESTAMPTZ '2000-01-01 00:00:00+00')
);


----------------------------------------------------------------------------
-- STEP 5: TABLES THAT DEPEND ON applications

----------------------------------------------------------------------------
-- TABLE: application_status_history

CREATE TABLE recruitment.application_status_history (
	history_id SERIAL NOT NULL,
	application_id INTEGER NOT NULL,
	status VARCHAR(20) NOT NULL,
	changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	is_active BOOLEAN NOT NULL DEFAULT TRUE,
	notes TEXT,

	CONSTRAINT PK_application_status_history_history_id
		PRIMARY KEY (history_id),

	-- If FK is missing, history rows could be orphaned, making it impossible
	-- to trace which application a status change belongs to.
	CONSTRAINT FK_application_status_history_application_id
		FOREIGN KEY (application_id) REFERENCES recruitment.applications (application_id),

	-- CONSTRAINT: status restricted to specific values
	-- Prevents free-text statuses like "maybe" that break stage-funnel reports.
	CONSTRAINT CHK_application_status_history_status
		CHECK (status IN ('submitted', 'review', 'approved', 'rejected')),

	-- CONSTRAINT: changed_at after 2000-01-01
	-- Prevents placeholder import dates corrupting application timelines.
	CONSTRAINT CHK_application_status_history_changed_at
		CHECK (changed_at > TIMESTAMPTZ '2000-01-01 00:00:00+00')
);


----------------------------------------------------------------------------
-- TABLE: interviews

CREATE TABLE recruitment.interviews (
	interview_id SERIAL NOT NULL,
	application_id INTEGER NOT NULL,
	rep_id INTEGER NOT NULL,
	
	-- TIMESTAMPTZ essential: candidates and reps may be in different timezones.
	-- Plain TIMESTAMP would make a Tbilisi candidate and Madrid rep look at
	-- different absolute moments for "09:00", causing missed interviews.
	scheduled_at TIMESTAMPTZ NOT NULL,
	interview_type VARCHAR(20) NOT NULL,
	interview_city_id INTEGER,
	meeting_link VARCHAR(255),
	outcome VARCHAR(20),
	notes TEXT,

	CONSTRAINT PK_interviews_interview_id PRIMARY KEY (interview_id),

	-- If FK on application_id is missing, interviews could be orphaned,
	-- making it impossible to link an interview back to its candidate and job.
	CONSTRAINT FK_interviews_application_id
		FOREIGN KEY (application_id) REFERENCES recruitment.applications (application_id),

	-- If FK on rep_id is missing, interviews could reference departed reps
	-- whose rows were deleted, permanently losing interviewer attribution.
	CONSTRAINT FK_interviews_rep_id
		FOREIGN KEY (rep_id) REFERENCES recruitment.company_representatives (rep_id),

	CONSTRAINT FK_interviews_interview_city_id
		FOREIGN KEY (interview_city_id) REFERENCES recruitment.cities (city_id),

	-- CONSTRAINT: interview_type restricted to specific values
	-- Prevents free-text entries that break interview-round reporting.
	CONSTRAINT CHK_interviews_interview_type
		CHECK (interview_type IN ('phone', 'video', 'on_site', 'technical', 'final')),

	-- CONSTRAINT: outcome restricted to specific values when set
	-- Without this, outcome fields could contain "ok" or "nope", making
	-- pass-rate analytics unreliable.
	CONSTRAINT CHK_interviews_outcome
		CHECK (outcome IS NULL
			   OR outcome IN ('passed', 'failed', 'no_show', 'pending')),

	-- CONSTRAINT: at least one of city or meeting link must be provided
	-- Without this, an on-site interview could be created with no address and
	-- no video link, giving the candidate no way to attend.
	CONSTRAINT CHK_interviews_location_or_link
		CHECK (interview_city_id IS NOT NULL OR meeting_link IS NOT NULL),

	-- CONSTRAINT: scheduled_at after 2000-01-01
	-- Prevents placeholder import dates corrupting interview scheduling.
	CONSTRAINT CHK_interviews_scheduled_at
		CHECK (scheduled_at > TIMESTAMPTZ '2000-01-01 00:00:00+00')
);


----------------------------------------------------------------------------
-- TABLE: placements

CREATE TABLE recruitment.placements (
	placement_id SERIAL NOT NULL,
	application_id INTEGER NOT NULL,
	start_date DATE NOT NULL,
	end_date DATE,
	
	-- NUMERIC not FLOAT: monetary values must be exact. Wrong type risks
	-- rounding errors in salary reporting and agency fee calculations.
	salary_agreed NUMERIC(12,2) NOT NULL,
	placement_fee NUMERIC(12,2) NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

	CONSTRAINT PK_placements_placement_id PRIMARY KEY (placement_id),

	-- CONSTRAINT: UNIQUE on application_id
	-- Enforces 1:1 between applications and placements. Without this, two
	-- placement rows could reference the same application, producing two
	-- conflicting salary_agreed values for one hire event.
	CONSTRAINT UQ_placements_application_id UNIQUE (application_id),

	-- If FK is missing, a placement could reference a deleted application,
	-- making it impossible to look up the candidate or company for the hire.
	CONSTRAINT FK_placements_application_id
		FOREIGN KEY (application_id) REFERENCES recruitment.applications (application_id),

	-- CONSTRAINT: salary_agreed > 0
	-- A zero or negative agreed salary is not a valid hire outcome. Without
	-- this, billing calculations would produce nonsensical results.
	CONSTRAINT CHK_placements_salary_agreed CHECK (salary_agreed > 0),

	-- CONSTRAINT: placement_fee >= 0
	-- Fee can be 0 for pro-bono placements but never negative.
	CONSTRAINT CHK_placements_placement_fee CHECK (placement_fee >= 0),

	-- CONSTRAINT: end_date > start_date when set
	-- Prevents logically impossible employment ranges.
	CONSTRAINT CHK_placements_end_date
		CHECK (end_date IS NULL OR end_date > start_date),

	-- CONSTRAINT: start_date after 2000-01-01
	-- Prevents placeholder import dates corrupting employment timelines.
	CONSTRAINT CHK_placements_start_date
		CHECK (start_date > DATE '2000-01-01')
);


----------------------------------------------------------------------------
-- STEP 6: SAMPLE DATA
/*
HOW CONSISTENCY IS ENSURED:
All FK values are resolved using SELECT subqueries that reference parent
tables by their natural/unique key rather than hardcoded IDs. This means
the script works correctly even if SERIAL sequences produce different values
across environments or after a partial re-run. ON CONFLICT DO NOTHING and
WHERE NOT EXISTS prevent duplicates without raising errors. Insert order
follows the same parent-before-child dependency order as the DDL above.
*/

-- countries
INSERT INTO recruitment.countries (
	country_name,
	region_name
)
VALUES
	('United Kingdom', 'Europe'),
	('Poland', 'Europe'),
	('Germany', 'Europe'),
	('United States', 'North America')
ON CONFLICT (UPPER(country_name)) DO NOTHING;


-- cities
-- FIX: All four cities are inserted in a single query. Each city's country_id
-- is resolved inline via a correlated subquery, keeping the insert set-based
-- and avoiding four separate round-trips.
INSERT INTO recruitment.cities (
	city_name,
	country_id
)
VALUES
	('London', (SELECT country_id FROM recruitment.countries WHERE UPPER(country_name) = UPPER('United Kingdom'))),
	('Warsaw', (SELECT country_id FROM recruitment.countries WHERE UPPER(country_name) = UPPER('Poland'))),
	('Berlin', (SELECT country_id FROM recruitment.countries WHERE UPPER(country_name) = UPPER('Germany'))),
	('New York', (SELECT country_id FROM recruitment.countries WHERE UPPER(country_name) = UPPER('United States')))
ON CONFLICT (UPPER(city_name), country_id) DO NOTHING;


-- skills
INSERT INTO recruitment.skills (
	skill_name, 
	category
)
VALUES
	('Python', 'technical'),
	('SQL', 'technical'),
	('Communication', 'soft'),
	('Data Analysis', 'technical'),
	('English', 'language')
ON CONFLICT (UPPER(skill_name)) DO NOTHING;


-- services
INSERT INTO recruitment.services (
	service_name,
	description,
	price
)
VALUES
	('Resume Review', 'Expert CV rewrite and optimisation', 99.00),
	('Interview Coaching', 'Mock sessions with structured feedback', 149.00),
	('Skills Testing', 'Online assessment across technical domains', 49.00)
ON CONFLICT (UPPER(service_name)) DO NOTHING;


-- companies
INSERT INTO recruitment.companies (
	name,
	industry,
	website
)
VALUES
	('TechCorp Ltd', 'Software', 'https://techcorp.example.com'),
	('FinGroup AG', 'Banking', 'https://fingroup.example.com')
ON CONFLICT (UPPER(name)) DO NOTHING;


-- candidates
-- All three candidates are inserted in a single query. Each preferred_city_id
-- is resolved inline via a correlated subquery against the already-populated
-- cities and countries tables.
INSERT INTO recruitment.candidates (
	first_name,
	last_name,
	email,
	phone,
	date_of_birth,
	summary,
	preferred_city_id
)
VALUES
	(
		'Maria',
		'Kowalska',
		'maria.kowalska@email.com',
		'+48 501 234 567',
		DATE '2000-03-12',
		'Experienced data analyst with 5 years in fintech',
		(SELECT ci.city_id FROM recruitment.cities ci
		 JOIN recruitment.countries co ON ci.country_id = co.country_id
		 WHERE UPPER(ci.city_name) = UPPER('Warsaw') AND
			   UPPER(co.country_name) = UPPER('Poland'))
	),
	(
		'James',
		'Obi',
		'james.obi@email.com',
		'+44 7911 123456',
		DATE '2001-07-28',
		'Full stack developer seeking senior roles in product companies',
		(SELECT ci.city_id FROM recruitment.cities ci
		 JOIN recruitment.countries co ON ci.country_id = co.country_id
		 WHERE UPPER(ci.city_name) = UPPER('London') AND
			   UPPER(co.country_name) = UPPER('United Kingdom'))
	),
	(
		'Anna',
		'Schmidt',
		'anna.schmidt@email.com',
		'+49 30 1234567',
		DATE '2002-11-05',
		'Junior backend developer with 2 years Python experience',
		(SELECT ci.city_id FROM recruitment.cities ci
		 JOIN recruitment.countries co ON ci.country_id = co.country_id
		 WHERE UPPER(ci.city_name) = UPPER('Berlin') AND
			   UPPER(co.country_name) = UPPER('Germany'))
	)
ON CONFLICT (UPPER(email)) DO NOTHING;


-- company_representatives
-- Both reps are inserted in a single query. Each company_id is resolved
-- inline via a correlated subquery.
INSERT INTO recruitment.company_representatives (
	company_id,
	first_name,
	last_name,
	email,
	phone,
	position
)
VALUES
	(
		(SELECT company_id FROM recruitment.companies WHERE UPPER(name) = UPPER('TechCorp Ltd')),
		'Alice',
		'Walker',
		'alice.walker@techcorp.example',
		'+48 123 456 789',
		'HR Manager'
	),
	(
		(SELECT company_id FROM recruitment.companies WHERE UPPER(name) = UPPER('FinGroup AG')),
		'Bob',
		'Lee',
		'bob.lee@fingroup.example',
		'+49 30 987 654',
		'Talent Acquisition Lead'
	)
ON CONFLICT (UPPER(email)) DO NOTHING;


-- jobs
INSERT INTO recruitment.jobs (
	company_id,
	city_id,
	title,
	description,
	salary_min,
	salary_max,
	employment_type,
	work_mode
)
SELECT
	co.company_id,
	ci.city_id,
	'Python Backend Developer',
	'Build and maintain REST APIs for our core platform',
	80000.00,
	110000.00,
	'full_time',
	'hybrid'
FROM recruitment.companies co
JOIN recruitment.cities ci ON UPPER(ci.city_name) = UPPER('London')
WHERE UPPER(co.name) = UPPER('TechCorp Ltd') AND
	  NOT EXISTS (
		  SELECT 1
		  FROM recruitment.jobs j
		  WHERE UPPER(j.title) = UPPER('Python Backend Developer') AND
			    j.company_id = co.company_id
	  );

INSERT INTO recruitment.jobs (
	company_id,
	city_id,
	title,
	description,
	salary_min,
	salary_max,
	employment_type,
	work_mode
)
SELECT
	co.company_id,
	ci.city_id,
	'Financial Analyst',
	'Support quarterly reporting and budget forecasting',
	65000.00,
	90000.00,
	'full_time',
	'on_site'
FROM recruitment.companies co
JOIN recruitment.cities ci ON UPPER(ci.city_name) = UPPER('Warsaw')
WHERE UPPER(co.name) = UPPER('FinGroup AG') AND
	  NOT EXISTS (
		  SELECT 1
		  FROM recruitment.jobs j
		  WHERE UPPER(j.title) = UPPER('Financial Analyst') AND
			    j.company_id = co.company_id
	  );

INSERT INTO recruitment.jobs (
	company_id,
	city_id,
	title,
	description,
	salary_min,
	salary_max,
	employment_type,
	work_mode
)
SELECT
	co.company_id,
	NULL,
	'DevOps Engineer',
	'Manage CI/CD pipelines and cloud infrastructure',
	NULL,
	NULL,
	'contract',
	'remote'
FROM recruitment.companies co
WHERE UPPER(co.name) = UPPER('TechCorp Ltd') AND
	  NOT EXISTS (
		  SELECT 1
		  FROM recruitment.jobs j
		  WHERE UPPER(j.title) = UPPER('DevOps Engineer') AND
			    j.company_id = co.company_id
	  );


-- candidate_skills
INSERT INTO recruitment.candidate_skills (
	candidate_id,
	skill_id,
	proficiency_level,
	acquired_date
)
SELECT ca.candidate_id,
	   sk.skill_id,
	   'expert',
	   DATE '2020-06-01'
FROM recruitment.candidates ca, 
	 recruitment.skills sk
WHERE UPPER(ca.email) = UPPER('maria.kowalska@email.com') AND
	  UPPER(sk.skill_name) = UPPER('Python')
ON CONFLICT (candidate_id, skill_id) DO NOTHING;

INSERT INTO recruitment.candidate_skills (
	candidate_id,
	skill_id,
	proficiency_level,
	acquired_date
)
SELECT ca.candidate_id,
	   sk.skill_id,
	   'advanced',
	   DATE '2019-01-01'
FROM recruitment.candidates ca,
	 recruitment.skills sk
WHERE UPPER(ca.email) = UPPER('maria.kowalska@email.com') AND
	  UPPER(sk.skill_name) = UPPER('SQL')
ON CONFLICT (candidate_id, skill_id) DO NOTHING;

INSERT INTO recruitment.candidate_skills (
	candidate_id,
	skill_id,
	proficiency_level,
	acquired_date
)
SELECT ca.candidate_id, 
	   sk.skill_id,
	   'intermediate',
	   DATE '2022-03-15'
FROM recruitment.candidates ca,
	 recruitment.skills sk
WHERE UPPER(ca.email) = UPPER('james.obi@email.com') AND
	  UPPER(sk.skill_name) = UPPER('Python')
ON CONFLICT (candidate_id, skill_id) DO NOTHING;

INSERT INTO recruitment.candidate_skills (
	candidate_id,
	skill_id,
	proficiency_level,
	acquired_date
)
SELECT ca.candidate_id,
	   sk.skill_id,
	   'beginner',
	   DATE '2023-09-10'
FROM recruitment.candidates ca,
	 recruitment.skills sk
WHERE UPPER(ca.email) = UPPER('anna.schmidt@email.com') AND
	  UPPER(sk.skill_name) = UPPER('Python')
ON CONFLICT (candidate_id, skill_id) DO NOTHING;


-- work_experience
INSERT INTO recruitment.work_experience (
	candidate_id,
	company_name,
	position,
	start_date,
	end_date,
	description,
	is_current
)
SELECT
	candidate_id, 
	'DataSoft GmbH',
	'Data Analyst',
	DATE '2019-04-01',
	DATE '2022-12-31',
	'ETL pipelines and dashboard reporting', 
	FALSE
FROM recruitment.candidates 
WHERE UPPER(email) = UPPER('maria.kowalska@email.com') AND
	  NOT EXISTS (
		  SELECT 1 
		  FROM recruitment.work_experience we
		  JOIN recruitment.candidates ca ON ca.candidate_id = we.candidate_id
		  WHERE UPPER(ca.email) = UPPER('maria.kowalska@email.com') AND 
			    UPPER(we.company_name) = UPPER('DataSoft GmbH')
	  );

INSERT INTO recruitment.work_experience (
	candidate_id,
	company_name,
	position,
	start_date, 
	end_date, 
	description,
	is_current
)
SELECT
	candidate_id,
	'Analytics Inc.',
	'Senior Analyst',
	DATE '2023-01-15',
	NULL,
	'Leading analytics team and stakeholder reporting',
	TRUE
FROM recruitment.candidates 
WHERE UPPER(email) = UPPER('maria.kowalska@email.com') AND
	  NOT EXISTS (
		  SELECT 1 
		  FROM recruitment.work_experience we
		  JOIN recruitment.candidates ca ON ca.candidate_id = we.candidate_id
		  WHERE UPPER(ca.email) = UPPER('maria.kowalska@email.com') AND
			    UPPER(we.company_name) = UPPER('Analytics Inc.')
	  );

INSERT INTO recruitment.work_experience (
	candidate_id,
	company_name,
	position,
	start_date,
	end_date,
	description,
	is_current
)
SELECT
	candidate_id,
	'WebStart Ltd',
	'Junior Developer',
	DATE '2021-06-01',
	NULL,
	'Full stack development with React and Node.js',
	TRUE
FROM recruitment.candidates 
WHERE UPPER(email) = UPPER('james.obi@email.com') AND
	  NOT EXISTS (
		  SELECT 1 
		  FROM recruitment.work_experience we
		  JOIN recruitment.candidates ca ON ca.candidate_id = we.candidate_id
		  WHERE UPPER(ca.email) = UPPER('james.obi@email.com') AND
			    UPPER(we.company_name) = UPPER('WebStart Ltd')
	  );


-- candidate_services
INSERT INTO recruitment.candidate_services (
	candidate_id,
	service_id,
	status,
	active_from,
	active_to,
	is_active,
	notes
)
SELECT
	ca.candidate_id, 
	sv.service_id,
	'completed',
	TIMESTAMPTZ '2024-04-01 10:00:00+00',
	TIMESTAMPTZ '2024-04-03 09:00:00+00',
	FALSE,
	'CV restructured, quantified achievements added'
FROM recruitment.candidates ca, 
	 recruitment.services sv
WHERE UPPER(ca.email) = UPPER('maria.kowalska@email.com') AND
	  UPPER(sv.service_name) = UPPER('Resume Review') AND
	  NOT EXISTS (
		  SELECT 1 
		  FROM recruitment.candidate_services cs
		  JOIN recruitment.candidates c ON c.candidate_id = cs.candidate_id
		  JOIN recruitment.services s ON s.service_id = cs.service_id
		  WHERE UPPER(c.email) = UPPER('maria.kowalska@email.com') AND
			    UPPER(s.service_name) = UPPER('Resume Review') AND
			    cs.status = 'completed'
	  );

INSERT INTO recruitment.candidate_services (
	candidate_id,
	service_id,
	status,
	active_from,
	active_to,
	is_active,
	notes
)
SELECT
	ca.candidate_id,
	sv.service_id,
	'in_progress',
	TIMESTAMPTZ '2024-04-05 10:00:00+00',
	NULL,
	TRUE,
	'Two sessions completed, one remaining'
FROM recruitment.candidates ca, 
	 recruitment.services sv
WHERE UPPER(ca.email) = UPPER('james.obi@email.com') AND
	  UPPER(sv.service_name) = UPPER('Interview Coaching') AND
	  NOT EXISTS (
		  SELECT 1 
		  FROM recruitment.candidate_services cs
		  JOIN recruitment.candidates c ON c.candidate_id = cs.candidate_id
		  JOIN recruitment.services s ON s.service_id = cs.service_id
		  WHERE UPPER(c.email) = UPPER('james.obi@email.com') AND
			    UPPER(s.service_name) = UPPER('Interview Coaching') AND
			    cs.is_active = TRUE
	  );


-- applications
INSERT INTO recruitment.applications (
	candidate_id,
	job_id,
	applied_at
)
SELECT ca.candidate_id,
	   j.job_id,
	   TIMESTAMPTZ '2024-05-01 08:00:00+00'
FROM recruitment.candidates ca
JOIN recruitment.jobs j ON UPPER(j.title) = UPPER('Python Backend Developer')
JOIN recruitment.companies co ON co.company_id = j.company_id AND
								 UPPER(co.name) = UPPER('TechCorp Ltd')
WHERE UPPER(ca.email) = UPPER('maria.kowalska@email.com')
ON CONFLICT (candidate_id, job_id) DO NOTHING;

INSERT INTO recruitment.applications (
	candidate_id, 
	job_id, 
	applied_at
)
SELECT ca.candidate_id, 
	   j.job_id,
	   TIMESTAMPTZ '2024-05-10 10:15:00+00'
FROM recruitment.candidates ca
JOIN recruitment.jobs j ON UPPER(j.title) = UPPER('Financial Analyst')
JOIN recruitment.companies co ON co.company_id = j.company_id AND
								 UPPER(co.name) = UPPER('FinGroup AG')
WHERE UPPER(ca.email) = UPPER('james.obi@email.com')
ON CONFLICT (candidate_id, job_id) DO NOTHING;

INSERT INTO recruitment.applications (
	candidate_id, 
	job_id, 
	applied_at
)
SELECT ca.candidate_id,
	   j.job_id,
	   TIMESTAMPTZ '2024-05-12 09:00:00+00'
FROM recruitment.candidates ca
JOIN recruitment.jobs j ON UPPER(j.title) = UPPER('DevOps Engineer')
JOIN recruitment.companies co ON co.company_id = j.company_id AND
								 UPPER(co.name) = UPPER('TechCorp Ltd')
WHERE UPPER(ca.email) = UPPER('anna.schmidt@email.com')
ON CONFLICT (candidate_id, job_id) DO NOTHING;

-- application_status_history
INSERT INTO recruitment.application_status_history (
	application_id,
	status,
	changed_at,
	is_active,
	notes
)
SELECT a.application_id,
	   'submitted',
	   TIMESTAMPTZ '2024-05-01 08:00:00+00',
	   FALSE,
	   NULL
FROM recruitment.applications a
JOIN recruitment.candidates ca ON ca.candidate_id = a.candidate_id
JOIN recruitment.jobs j ON j.job_id = a.job_id
WHERE UPPER(ca.email) = UPPER('maria.kowalska@email.com') AND
	  UPPER(j.title) = UPPER('Python Backend Developer') AND
	  NOT EXISTS (
		  SELECT 1 
		  FROM recruitment.application_status_history h
		  WHERE h.application_id = a.application_id AND
			    h.status = 'submitted'
	  );

INSERT INTO recruitment.application_status_history (
	application_id,
	status,
	changed_at,
	is_active,
	notes
)
SELECT a.application_id,
	   'review',
	   TIMESTAMPTZ '2024-05-03 09:00:00+00',
	   TRUE,
	   'Screened by recruiter'
FROM recruitment.applications a
JOIN recruitment.candidates ca ON ca.candidate_id = a.candidate_id
JOIN recruitment.jobs j ON j.job_id = a.job_id
WHERE UPPER(ca.email) = UPPER('maria.kowalska@email.com') AND
	  UPPER(j.title) = UPPER('Python Backend Developer') AND
	  NOT EXISTS (
		  SELECT 1 
		  FROM recruitment.application_status_history h
		  WHERE h.application_id = a.application_id AND
			    h.status = 'review'
	  );

INSERT INTO recruitment.application_status_history (
	application_id,
	status,
	changed_at,
	is_active,
	notes
)
SELECT a.application_id,
	   'submitted',
	   TIMESTAMPTZ '2024-05-10 10:15:00+00',
	   TRUE, 
	   NULL
FROM recruitment.applications a
JOIN recruitment.candidates ca ON ca.candidate_id = a.candidate_id
JOIN recruitment.jobs j ON j.job_id = a.job_id
WHERE UPPER(ca.email) = UPPER('james.obi@email.com') AND
	  UPPER(j.title) = UPPER('Financial Analyst') AND
	  NOT EXISTS (
		  SELECT 1
		  FROM recruitment.application_status_history h
		  WHERE h.application_id = a.application_id AND
			    h.status = 'submitted'
	  );

INSERT INTO recruitment.application_status_history (
	application_id,
	status,
	changed_at,
	is_active,
	notes
)
SELECT a.application_id,
	  'submitted',
	  TIMESTAMPTZ '2024-05-12 09:00:00+00',
	  TRUE,
	  NULL
FROM recruitment.applications a
JOIN recruitment.candidates ca ON ca.candidate_id = a.candidate_id
JOIN recruitment.jobs j ON j.job_id = a.job_id
WHERE UPPER(ca.email) = UPPER('anna.schmidt@email.com') AND
	  UPPER(j.title) = UPPER('DevOps Engineer') AND
	  NOT EXISTS (
		  SELECT 1 
		  FROM recruitment.application_status_history h
		  WHERE h.application_id = a.application_id AND
			    h.status = 'submitted'
	  );

-- interviews
INSERT INTO recruitment.interviews (
	application_id,
	rep_id,
	scheduled_at,
	interview_type,
	interview_city_id,
	meeting_link,
	outcome,
	notes
)
SELECT
	a.application_id,
	r.rep_id,
	TIMESTAMPTZ '2024-05-05 11:00:00+00',
	'video',
	NULL,
	'https://meet.example.com/abc123',
	'passed',
	'Strong technical answers'
FROM recruitment.applications a
JOIN recruitment.candidates ca ON ca.candidate_id = a.candidate_id
JOIN recruitment.jobs j ON j.job_id = a.job_id
JOIN recruitment.company_representatives r ON UPPER(r.email) = UPPER('alice.walker@techcorp.example')
WHERE UPPER(ca.email) = UPPER('maria.kowalska@email.com') AND
	  UPPER(j.title) = UPPER('Python Backend Developer') AND
	  NOT EXISTS (
		  SELECT 1 
		  FROM recruitment.interviews i
		  WHERE i.application_id = a.application_id AND
			    i.interview_type = 'video' AND
			    i.scheduled_at = TIMESTAMPTZ '2024-05-05 11:00:00+00'
	  );

INSERT INTO recruitment.interviews (
	application_id,
	rep_id,
	scheduled_at,
	interview_type,
	interview_city_id,
	meeting_link,
	outcome,
	notes
)
SELECT
	a.application_id,
	r.rep_id,
	TIMESTAMPTZ '2024-05-07 10:00:00+00',
	'on_site',
	ci.city_id,
	NULL,
	'passed',
	'Cultural fit confirmed'
FROM recruitment.applications a
JOIN recruitment.candidates ca ON ca.candidate_id = a.candidate_id
JOIN recruitment.jobs j ON j.job_id = a.job_id
JOIN recruitment.company_representatives r ON UPPER(r.email) = UPPER('alice.walker@techcorp.example')
JOIN recruitment.cities ci ON UPPER(ci.city_name) = UPPER('London')
JOIN recruitment.countries co ON co.country_id = ci.country_id AND
								 UPPER(co.country_name) = UPPER('United Kingdom')
WHERE UPPER(ca.email) = UPPER('maria.kowalska@email.com') AND
	  UPPER(j.title) = UPPER('Python Backend Developer') AND
	  NOT EXISTS (
		  SELECT 1
		  FROM recruitment.interviews i
		  WHERE i.application_id = a.application_id AND
			    i.interview_type = 'on_site' AND
			    i.scheduled_at = TIMESTAMPTZ '2024-05-07 10:00:00+00'
	  );

-- placements
INSERT INTO recruitment.placements (
	application_id,
	start_date,
	end_date,
	salary_agreed,
	placement_fee
)
SELECT
	a.application_id,
	DATE '2024-06-01',
	NULL,
	95000.00,
	9500.00
FROM recruitment.applications a
JOIN recruitment.candidates ca ON ca.candidate_id = a.candidate_id
JOIN recruitment.jobs j ON j.job_id = a.job_id
WHERE UPPER(ca.email) = UPPER('maria.kowalska@email.com') AND
	  UPPER(j.title) = UPPER('Python Backend Developer')
ON CONFLICT (application_id) DO NOTHING;


----------------------------------------------------------------------------
-- STEP 7: ADD record_ts TO ALL TABLES
/*
record_ts records the calendar date when the row was physically inserted.
DEFAULT current_date ensures every new row is stamped automatically.
NOT NULL ensures the audit trail is never incomplete.
ADD COLUMN IF NOT EXISTS makes this block safely rerunnable.
The verification SELECT after each ALTER confirms no existing rows are NULL.
*/

ALTER TABLE recruitment.countries
	ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT current_date;
SELECT COUNT(*) AS countries_missing_ts
FROM recruitment.countries
WHERE record_ts IS NULL;

ALTER TABLE recruitment.cities
	ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT current_date;
SELECT COUNT(*) AS cities_missing_ts
FROM recruitment.cities
WHERE record_ts IS NULL;

ALTER TABLE recruitment.skills
	ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT current_date;
SELECT COUNT(*) AS skills_missing_ts
FROM recruitment.skills
WHERE record_ts IS NULL;

ALTER TABLE recruitment.services
	ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT current_date;
SELECT COUNT(*) AS services_missing_ts
FROM recruitment.services
WHERE record_ts IS NULL;

ALTER TABLE recruitment.companies
	ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT current_date;
SELECT COUNT(*) AS companies_missing_ts
FROM recruitment.companies
WHERE record_ts IS NULL;

ALTER TABLE recruitment.candidates
	ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT current_date;
SELECT COUNT(*) AS candidates_missing_ts
FROM recruitment.candidates
WHERE record_ts IS NULL;

ALTER TABLE recruitment.company_representatives
	ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT current_date;
SELECT COUNT(*) AS reps_missing_ts
FROM recruitment.company_representatives
WHERE record_ts IS NULL;

ALTER TABLE recruitment.jobs
	ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT current_date;
SELECT COUNT(*) AS jobs_missing_ts
FROM recruitment.jobs
WHERE record_ts IS NULL;

ALTER TABLE recruitment.candidate_skills
	ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT current_date;
SELECT COUNT(*) AS candidate_skills_missing_ts
FROM recruitment.candidate_skills
WHERE record_ts IS NULL;

ALTER TABLE recruitment.work_experience
	ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT current_date;
SELECT COUNT(*) AS work_exp_missing_ts
FROM recruitment.work_experience
WHERE record_ts IS NULL;

ALTER TABLE recruitment.candidate_services
	ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT current_date;
SELECT COUNT(*) AS candidate_services_missing_ts
FROM recruitment.candidate_services
WHERE record_ts IS NULL;

ALTER TABLE recruitment.applications
	ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT current_date;
SELECT COUNT(*) AS applications_missing_ts
FROM recruitment.applications
WHERE record_ts IS NULL;

ALTER TABLE recruitment.application_status_history
	ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT current_date;
SELECT COUNT(*) AS app_history_missing_ts
FROM recruitment.application_status_history
WHERE record_ts IS NULL;

ALTER TABLE recruitment.interviews
	ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT current_date;
SELECT COUNT(*) AS interviews_missing_ts
FROM recruitment.interviews
WHERE record_ts IS NULL;

ALTER TABLE recruitment.placements
	ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT current_date;
SELECT COUNT(*) AS placements_missing_ts
FROM recruitment.placements
WHERE record_ts IS NULL;


----------------------------------------------------------------------------
-- STEP 8: INDEXES ON FOREIGN KEY COLUMNS

-- cities: filtering/joining cities by country is a frequent operation
CREATE INDEX IF NOT EXISTS IDX_cities_country_id
ON recruitment.cities (country_id);

-- candidates: looking up a candidate's preferred location
CREATE INDEX IF NOT EXISTS IDX_candidates_preferred_city_id
ON recruitment.candidates (preferred_city_id);

-- company_representatives: all reps for a given company
CREATE INDEX IF NOT EXISTS IDX_company_representatives_company_id
ON recruitment.company_representatives (company_id);

-- jobs: all vacancies for a given company; all vacancies in a given city
CREATE INDEX IF NOT EXISTS IDX_jobs_company_id
ON recruitment.jobs (company_id);

CREATE INDEX IF NOT EXISTS IDX_jobs_city_id
ON recruitment.jobs (city_id);

-- candidate_skills: the composite PK covers (candidate_id, skill_id) for
-- lookups by candidate. A separate index on skill_id alone speeds up
-- "find all candidates with skill X" queries.
CREATE INDEX IF NOT EXISTS IDX_candidate_skills_skill_id
ON recruitment.candidate_skills (skill_id);

-- work_experience: all jobs in a candidate's history
CREATE INDEX IF NOT EXISTS IDX_work_experience_candidate_id
ON recruitment.work_experience (candidate_id);

-- candidate_services: services delivered to a candidate, candidates using a service
CREATE INDEX IF NOT EXISTS IDX_candidate_services_candidate_id
ON recruitment.candidate_services (candidate_id);

CREATE INDEX IF NOT EXISTS IDX_candidate_services_service_id
ON recruitment.candidate_services (service_id);

-- application_status_history: all status changes for a given application
CREATE INDEX IF NOT EXISTS IDX_application_status_history_application_id
ON recruitment.application_status_history (application_id);

-- interviews: all interviews tied to an application, all interviews conducted
-- by a rep, all on-site interviews in a given city
CREATE INDEX IF NOT EXISTS IDX_interviews_application_id
ON recruitment.interviews (application_id);

CREATE INDEX IF NOT EXISTS IDX_interviews_rep_id
ON recruitment.interviews (rep_id);

CREATE INDEX IF NOT EXISTS IDX_interviews_interview_city_id
ON recruitment.interviews (interview_city_id);