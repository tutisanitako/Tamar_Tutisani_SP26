"""
Task 2 deliverable: 2 UI checks against the Power BI playground
"Capture report views" showcase (playground.powerbi.com/en-us/showcases-gallery/capture-report-views).

Check 1 (smoke)    - the "Saved views" button, which the whole showcase is
                     built around, is present as soon as the report loads.
Check 2 (critical) - after the user filters the report down to a single
                     manufacturer ("Abbas"), the "Capture view" button
                     (the actual feature under test) becomes available -
                     i.e. the showcase's core business logic works.
"""
import allure
import pytest
from Pages.captureReportViewsPage import CaptureReportViewsPage


@pytest.fixture(scope="function")
def capture_report_page(driver, selenium_config):
    driver.get(selenium_config["report_uri"])
    page = CaptureReportViewsPage(driver, selenium_config["delay"])
    page.switch_to_report_frame()
    return page


@allure.feature("Capture report views showcase")
@pytest.mark.ui
@pytest.mark.smoke
def test_saved_views_button_is_present(capture_report_page):
    assert capture_report_page.is_saved_views_button_present(), (
        "The 'Saved views' button was not found - "
        "the showcase failed to load its main entry point."
    )


@allure.feature("Capture report views showcase")
@pytest.mark.ui
@pytest.mark.critical
def test_capture_view_button_enabled_after_filtering(capture_report_page):
    capture_report_page.select_manufacturer_abbas()
    assert capture_report_page.is_capture_view_button_enabled(), (
        "The 'Capture view' button is not enabled after filtering by "
        "manufacturer 'Abbas' - the user would be unable to save this view."
    )
