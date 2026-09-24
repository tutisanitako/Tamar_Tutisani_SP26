"""
Unit-level checks for the DWH landing objects created in the earlier
homework. Each test is a single, isolated, independent check of one table.

sql / expected / case_name come from Tests/unit/conftest.py, which reads
them out of Configs/config_sql_unit.yaml and parametrizes this one function
6 times (3 marked smoke, 3 marked critical) - see checklist requirement
"parametrize used to keep tests DRY".
"""
import allure
import pytest


@allure.step("Run SQL check and return the result")
def _run_check(db_cursor, sql):
    db_cursor.execute(sql)
    return db_cursor.fetchone()[0]


@pytest.mark.unit
def test_sql_check(db_cursor, track_test_time, sql, expected, case_name):
    allure.dynamic.title(case_name)
    with allure.step(f"Executing: {sql}"):
        actual = _run_check(db_cursor, sql)

    assert actual == expected, (
        f"[{case_name}] expected {expected}, got {actual}. "
        f"Query was: {sql}"
    )
