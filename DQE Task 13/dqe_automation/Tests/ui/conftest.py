import pytest
import allure
from selenium import webdriver
from Utils.config_reader import load_config


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if report.when == "call" and report.failed:
        driver_fixture = item.funcargs.get("driver")
        if driver_fixture is not None:
            allure.attach(
                driver_fixture.get_screenshot_as_png(),
                name="failure_screenshot",
                attachment_type=allure.attachment_type.PNG,
            )
            allure.attach(
                driver_fixture.page_source,
                name="page_source",
                attachment_type=allure.attachment_type.HTML,
            )


@pytest.fixture(scope="session")
def selenium_config():
    return load_config("config_selenium.yaml")["global"]


@pytest.fixture(scope="function")
def driver():
    """One fresh browser per test (function scope) - keeps UI tests
    independent of each other, at the cost of a slower run.
    """
    chrome_driver = webdriver.Chrome()
    chrome_driver.maximize_window()
    yield chrome_driver
    chrome_driver.quit()
