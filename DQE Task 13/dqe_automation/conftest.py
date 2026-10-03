"""
Global, framework-wide fixtures and helpers.
"""
import os
import time
import psycopg2
import subprocess
import pytest
from Utils.config_reader import load_config


# ---------------------------------------------------------------------------
# DB connection fixture - used by Tests/unit.
# Connection is opened once per test ("function" scope keeps tests
# independent of each other), and always closed via the yield-fixture
# teardown, even if the test itself fails.
# ---------------------------------------------------------------------------
@pytest.fixture(scope="function")
def db_cursor():
    config = load_config("config_sql_unit.yaml")["database"]
    conn = psycopg2.connect(
        host=config["host"],
        port=config["port"],
        dbname=config["dbname"],
        user=config["user"],
        password=config["password"],
    )
    cursor = conn.cursor()
    yield cursor
    cursor.close()
    conn.close()


# ---------------------------------------------------------------------------
# Suite-wide / per-test timing fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session", autouse=True)
def track_suite_time():
    start = time.time()
    yield
    print(f"\n[SUITE] total execution time: {time.time() - start:.2f}s")


@pytest.fixture(scope="function")
def track_test_time(request):
    start = time.time()
    yield
    print(f"[TEST] {request.node.name} took {time.time() - start:.2f}s")


# ---------------------------------------------------------------------------
# Allure environment info - written once per run so every report clearly
# states what was tested (checklist item: reports must be understandable
# without extra context).
# ---------------------------------------------------------------------------
def pytest_sessionstart(session):
    results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "allure-results")
    os.makedirs(results_dir, exist_ok=True)
    env_path = os.path.join(results_dir, "environment.properties")
    with open(env_path, "w") as f:
        f.write("Project=DQE_LAB_2024\n")
        f.write("Framework=PyTest+Selenium+Allure\n")
        f.write("Target=PowerBI Playground / dwh_hw_db\n")

def pytest_sessionfinish(session, exitstatus):
    """Optional/starred requirement: generate the Allure HTML report the
    moment the pytest session finishes, instead of requiring a manual
    `allure generate` call afterward."""
    results_dir = session.config.getoption("--alluredir")
    if not results_dir:
        return  # nothing to generate if allure wasn't enabled for this run

    report_dir = results_dir.replace("allure-results", "allure-report")
    try:
        subprocess.run(
            ["allure", "generate", results_dir, "-o", report_dir, "--single-file", "--clean"],
            check=True,
            shell=True,  # needed on Windows since allure.bat isn't directly executable
        )
        print(f"\n[ALLURE] report generated at {report_dir}\\index.html")
        os.startfile(os.path.join(report_dir, "index.html"))
    except Exception as e:
        print(f"\n[ALLURE] could not auto-generate report: {e}")

