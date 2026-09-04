/*
-- Create a new MySQL user
CREATE USER 'tutisani'@'%' IDENTIFIED BY 'YourStrongPassword123!';

-- Grant privileges on the dilab_dev database
GRANT ALL PRIVILEGES ON dilab_dev.* TO 'tutisani'@'%';

-- Apply changes
FLUSH PRIVILEGES;
*/

-- RDS MySQL initial script
-- Tamar Tutisani
-- ----------------------------------------------------------------------

-- STEP 1: Create own schema
CREATE DATABASE IF NOT EXISTS tutisani_schema;
USE tutisani_schema;

-- STEP 2: Create a table
CREATE TABLE IF NOT EXISTS tutisani_schema.employees (
    employee_id INT AUTO_INCREMENT PRIMARY KEY,
    employee_name VARCHAR(100) NOT NULL,
    department VARCHAR(50),
    hire_date DATE,
    salary DECIMAL(10,2)
);

-- Insert a few sample rows (safe to re-run: check first)
INSERT INTO tutisani_schema.employees (employee_name, department, hire_date, salary)
SELECT * 
FROM (SELECT 
			'Nini Test', 
			'Engineering', 
			'2023-01-15', 
			55000.00
	  ) AS tmp
WHERE NOT EXISTS (
    SELECT 1 
    FROM tutisani_schema.employees 
    WHERE employee_name = 'Nini Test'
);

-- STEP 3: Create a view
CREATE OR REPLACE VIEW tutisani_schema.vw_high_earners AS
SELECT 
	employee_id, 
	employee_name, 
	department, 
	salary
FROM tutisani_schema.employees
WHERE salary > 50000;

-- STEP 4: Create a procedure
DROP PROCEDURE IF EXISTS tutisani_schema.prc_get_employees_by_dept;

DELIMITER $$
CREATE PROCEDURE tutisani_schema.prc_get_employees_by_dept(IN dept_name VARCHAR(50))
BEGIN
    SELECT 
    	employee_id, 
    	employee_name, 
    	department, 
    	salary
    FROM tutisani_schema.employees
    WHERE department = dept_name;
END$$
DELIMITER ;

-- STEP 5: Verification queries
SELECT * FROM tutisani_schema.employees;
SELECT * FROM tutisani_schema.vw_high_earners;
CALL tutisani_schema.prc_get_employees_by_dept('Engineering');