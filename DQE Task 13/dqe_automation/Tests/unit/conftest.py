import pytest
from Utils.config_reader import load_config


def _build_cases():
    """Read Configs/config_sql_unit.yaml once and turn every entry into a
    pytest.param, tagged with its own custom mark (smoke / critical).
    """
    cases = load_config("config_sql_unit.yaml")["tests"]
    params = []
    ids = []
    for case in cases:
        mark = getattr(pytest.mark, case["mark"])
        params.append(pytest.param(case["sql"], case["expected"], case["name"], marks=mark))
        ids.append(case["name"])
    return params, ids


def pytest_generate_tests(metafunc):
    if {"sql", "expected", "case_name"}.issubset(metafunc.fixturenames):
        params, ids = _build_cases()
        metafunc.parametrize("sql,expected,case_name", params, ids=ids)
