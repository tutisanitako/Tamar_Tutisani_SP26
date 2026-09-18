"""
Task 1 - Fixtures: track_test_time (per-test execution time) and
track_suite_time (whole-suite execution time, session-scoped, autouse).
"""
import time
import pytest


@pytest.fixture(scope="session", autouse=True)
def track_suite_time():
    """Runs once for the entire test session (autouse ensures it always fires)."""
    suite_start = time.time()
    print(f"\n[SUITE] Started at {time.strftime('%H:%M:%S', time.localtime(suite_start))}")
    yield
    suite_end = time.time()
    print(f"\n[SUITE] Finished. Total suite execution time: {suite_end - suite_start:.4f} seconds")


@pytest.fixture()
def track_test_time():
    """Function-scoped: tracks the execution time of a single test."""
    test_start = time.time()
    yield
    test_end = time.time()
    print(f"\n[TEST] Execution time: {test_end - test_start:.4f} seconds")


def add_numbers(a, b):
    return a + b


def test_add_two_positive_numbers(track_test_time):
    a, b = 3, 5
    result = add_numbers(a, b)
    time.sleep(2)
    assert result == 8, f"Expected 8 but got {result}"


def test_add_two_negative_numbers(track_test_time):
    a, b = -3, -5
    result = add_numbers(a, b)
    time.sleep(3)
    assert result == -8, f"Expected -8 but got {result}"


def test_add_negative_and_positive_numbers():
    # Deliberately does NOT use track_test_time — per requirement, all tests
    # use fixture 1 except this last one.
    a, b = -3, 5
    result = add_numbers(a, b)
    time.sleep(10)
    assert result == 2, f"Expected 2 but got {result}"
