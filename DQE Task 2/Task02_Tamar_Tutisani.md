# Task 2: Test Cases Creation for DWH/BI System

**Student Name:** Tamar Tutisani  
**TestRail Project Link:** https://gr1dq26testcasestask2.testrail.io/index.php?/projects/overview/18

## Summary

For this task, I created a comprehensive test suite in TestRail to verify the data quality of our Data Warehouse and BI system. To keep the testing process structured, I organized the test cases into three distinct paths:

- **Smoke Path (4 tests):** Verified basic core functionalities, including data extraction from source systems, transformation into landing areas, and initial loading into the DWH and dashboard.
- **Critical Path (5 tests):** Focused on daily business operations, checking table completeness, data accuracy, product master data uniqueness, and foreign key referential integrity.
- **Extended Path (5 tests):** Covered complex edge cases such as special character handling, boundary value limits, null/default value rules, dashboard aggregation calculations, and ETL performance under large data volumes (100K+ records).

Each test case was fully detailed with prerequisites, step-by-step instructions, expected results, automation levels, and assigned Data Quality dimensions (such as Accuracy, Completeness, Consistency, and Validity). Finally, I set up a dedicated test run titled "Unit Testing - DWH Data Completeness" containing the relevant unit-level tests and assigned it to myself to track execution progress. 

**Total test cases created:** 14