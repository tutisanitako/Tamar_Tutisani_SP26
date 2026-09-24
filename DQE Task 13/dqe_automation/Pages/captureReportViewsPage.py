from selenium.webdriver.support.ui import WebDriverWait as WDW
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

class CaptureReportViewsPage:
    def __init__(self, driver, delay):
        self.driver = driver
        self.delay = delay

        self.outer_iframe = (By.CSS_SELECTOR, "iframe.showcase-frame")
        self.inner_iframe = (By.TAG_NAME, "iframe")

        self.saved_views_button = (By.XPATH, "//*[@aria-label='Saved views' or contains(text(),'Saved views')]")
        self.capture_view_button = (By.XPATH, "//*[@aria-label='Capture view' or contains(text(),'Capture view')]")

        self.manufacturer_dropdown = (By.XPATH, "//div[@role='combobox' and @aria-label='Manufacturer']")
        self.manufacturer_item_abbas = (By.XPATH, "//div[@class='slicerItemContainer' and @title='Abbas']")

    def _switch_to_showcase_frame(self):
        """The showcase toolbar (Saved views / Capture view) lives in the
        outer iframe.showcase-frame, one level up from the report canvas."""
        self.driver.switch_to.default_content()
        outer = WDW(self.driver, self.delay).until(EC.presence_of_element_located(self.outer_iframe))
        self.driver.switch_to.frame(outer)

    def switch_to_report_frame(self):
        """The report canvas (slicers, visuals) lives one level deeper,
        inside the innermost iframe nested within the showcase frame."""
        self._switch_to_showcase_frame()
        inner = WDW(self.driver, self.delay).until(EC.presence_of_element_located(self.inner_iframe))
        self.driver.switch_to.frame(inner)
        # Confirms real report content has rendered (not just the spinner)
        # before returning control to the caller.
        WDW(self.driver, self.delay).until(EC.presence_of_element_located(self.manufacturer_dropdown))

    def select_manufacturer_abbas(self):
        dropdown = WDW(self.driver, self.delay).until(EC.element_to_be_clickable(self.manufacturer_dropdown))
        dropdown.click()
        WDW(self.driver, self.delay).until(lambda d: dropdown.get_attribute("aria-expanded") == "true")
        item = WDW(self.driver, self.delay).until(EC.element_to_be_clickable(self.manufacturer_item_abbas))
        item.click()

    def is_saved_views_button_present(self) -> bool:
        try:
            self._switch_to_showcase_frame()
            WDW(self.driver, self.delay).until(EC.visibility_of_element_located(self.saved_views_button))
            return True
        except Exception:
            return False

    def is_capture_view_button_enabled(self) -> bool:
        self._switch_to_showcase_frame()
        button = WDW(self.driver, self.delay).until(EC.visibility_of_element_located(self.capture_view_button))
        return button.is_enabled()
