"""
Task 2 - Parametrize: test_add_numbers driven by config/numbers_config.yaml
(marked smoke), and test_add_invalid_types verifying TypeError (marked critical).
"""
import os
import pytest
import yaml

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "numbers_config.yaml")


def get_numbers_data(config_name):
    with open(config_name, "r") as stream:
        config = yaml.safe_load(stream)
    return config["cases"]


def add_numbers(a, b, c):
    try:
        return a + b + c
    except TypeError:
        raise TypeError("Please check the parameters. All of them must be numeric")


test_cases = get_numbers_data(CONFIG_PATH)


@pytest.mark.smoke
@pytest.mark.parametrize(
    "case",
    test_cases,
    ids=[case["case_name"] for case in test_cases],  # case names come from the YAML file
)
def test_add_numbers(case):
    a, b, c = case["input"]
    expected = case["expected"]
    result = add_numbers(a, b, c)
    assert result == expected, (
        f"Case '{case['case_name']}': expected {expected}, got {result}"
    )


@pytest.mark.critical
def test_add_invalid_types():
    a, b, c = "a", 2, 1
    with pytest.raises(TypeError):
        add_numbers(a, b, c)
