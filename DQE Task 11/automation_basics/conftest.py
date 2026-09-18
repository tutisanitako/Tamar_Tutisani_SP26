import psycopg2
import pytest
import subprocess


@pytest.fixture(scope="session")
def db_connection():
    """Opens one DB connection for the whole test session, closes it at the end."""
    conn = psycopg2.connect(
        database="dwh_hw_db",
        user="postgres",
        password="",
        host="localhost",
        port="5432",
    )
    yield conn
    conn.close()


@pytest.fixture()
def db_cursor(db_connection):
    """Fresh cursor per test - keeps tests independent even though the connection is shared."""
    cursor = db_connection.cursor()
    yield cursor
    cursor.close()


def pytest_sessionfinish(session, exitstatus):
    markexpr = session.config.getoption("markexpr", default="")

    if markexpr == "smoke":
        results_dir, report_dir = "allure-results/smoke", "allure-report/smoke"
    elif markexpr == "critical":
        results_dir, report_dir = "allure-results/critical", "allure-report/critical"
    else:
        results_dir, report_dir = "allure-results/full", "allure-report/full"

    subprocess.run(
        f"allure generate {results_dir} -o {report_dir} --single-file --clean",
        shell=True,
        check=False,
    )