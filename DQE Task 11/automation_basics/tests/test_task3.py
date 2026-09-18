"""
Task 3 - DB testing framework: smoke tests for table presence and critical
tests for data-quality rules, driven by config/sql_config.yaml, using a
DB connection fixture from conftest.py, reported via Allure.
"""
import os
import allure
import pytest
import yaml

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "sql_config.yaml")


def load_sql_cases(section):
    with open(CONFIG_PATH, "r") as stream:
        config = yaml.safe_load(stream)
    return config[section]


smoke_cases = load_sql_cases("smoke_tests")
critical_cases = load_sql_cases("critical_tests")


@allure.suite("DWH Table Testing")
@allure.feature("Smoke Tests")
@pytest.mark.smoke
@pytest.mark.parametrize("case", smoke_cases, ids=[c["name"] for c in smoke_cases])
def test_smoke_table_presence(db_cursor, case):
    with allure.step(f"Execute SQL: {case['sql']}"):
        db_cursor.execute(case["sql"])
        result = db_cursor.fetchone()[0]
    with allure.step(f"Verify result equals expected value {case['expected']}"):
        assert result == case["expected"], (
            f"[{case['name']}] Expected {case['expected']} but got {result}"
        )


@allure.suite("DWH Table Testing")
@allure.feature("Critical Path Tests")
@pytest.mark.critical
@pytest.mark.parametrize("case", critical_cases, ids=[c["name"] for c in critical_cases])
def test_critical_data_quality(db_cursor, case):
    with allure.step(f"Execute SQL: {case['sql']}"):
        db_cursor.execute(case["sql"])
        result = db_cursor.fetchone()[0]
    with allure.step(f"Verify result equals expected value {case['expected']}"):
        assert result == case["expected"], (
            f"[{case['name']}] Expected {case['expected']} but got {result}"
        )
