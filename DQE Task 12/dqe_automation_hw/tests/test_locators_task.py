from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.relative_locator import locate_with


def test_locators():
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()))

    # ============================================================
    # SITE 1: https://phptravels.com/demo/  (red box = whole form)
    # ============================================================
    driver.get("https://phptravels.com/demo/")

    # 1. CLASS NAME (2) - distinct elements, each used once
    demo_first_name_by_class = driver.find_element(By.CLASS_NAME, "first_name")
    demo_last_name_by_class = driver.find_element(By.CLASS_NAME, "last_name")

    # 2. ID (2)
    demo_submit_by_id = driver.find_element(By.ID, "demo")
    demo_captcha_by_id = driver.find_element(By.ID, "number")

    print("Demo page locators found successfully.")

    # ============================================================
    # SITE 2: https://phptravels.org/register.php
    # (red boxes = Personal Information + Billing Address)
    # ============================================================
    driver.get("https://phptravels.org/register.php")

    # 3. NAME (2)
    reg_firstname_by_name = driver.find_element(By.NAME, "firstname")
    reg_city_by_name = driver.find_element(By.NAME, "city")

    # 4. CSS SELECTOR (2)
    reg_lastname_by_css = driver.find_element(By.CSS_SELECTOR, "#inputLastName")
    reg_email_by_css = driver.find_element(By.CSS_SELECTOR, "input[name='email']")

    # 5. XPATH (2)
    reg_firstname_by_xpath = driver.find_element(By.XPATH, "//input[@id='inputFirstName']")
    reg_postcode_by_xpath = driver.find_element(By.XPATH, "//input[@name='postcode']")

    # 6*. Relative locator (bonus) - Last Name is to the right of First Name
    reg_lastname_relative = locate_with(By.TAG_NAME, "input").to_right_of({By.ID: "inputFirstName"})

    print("Register page locators found successfully.")

    # ============================================================
    # SITE 3: https://phptravels.com/blog/
    # NOTE: task's screenshot shows an older blog layout (nav:
    # Travel/Tech/Business/Versions/Events).
    # Using the current live equivalent of the boxed search/nav area.
    # ============================================================
    driver.get("https://phptravels.com/blog/")

    blog_search_by_id = driver.find_element(By.ID, "searchBtn")
    blog_search_by_css = driver.find_element(By.CSS_SELECTOR, "#searchBtn")
    blog_blog_link_by_xpath = driver.find_element(By.XPATH, "//a[text()='Blog']")

    print("Blog page locators found successfully (using current live layout - see note).")

    driver.quit()