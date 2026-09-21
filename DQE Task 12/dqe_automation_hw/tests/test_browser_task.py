from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager


def test_open_google_chrome_auto():
    """Automatic approach: Selenium Manager / webdriver-manager downloads
    and manages the correct ChromeDriver version automatically."""
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()))
    driver.get("https://www.google.com")
    print("Chrome (auto) page title:", driver.title)
    assert "Google" in driver.title
    driver.quit()


def test_open_google_chrome_manual():
    """Manual approach: a specific chromedriver.exe path is hardcoded,
    instead of letting a manager resolve/download it automatically."""
    # Replace this path with wherever you place your manually downloaded chromedriver.exe
    manual_driver_path = r"C:\drivers\chromedriver.exe"
    driver = webdriver.Chrome(service=ChromeService(executable_path=manual_driver_path))
    driver.get("https://www.google.com")
    print("Chrome (manual) page title:", driver.title)
    assert "Google" in driver.title
    driver.quit()