import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time

def setup_driver_with_cookies():
    options = webdriver.ChromeOptions()
    options.add_argument("user-data-dir=/tmp/chrome_profile")  # Use a temporary directory
    driver = webdriver.Chrome(options=options)
    
    # Load LinkedIn and add your cookies
    driver.get("https://www.linkedin.com")
    cookie_string = os.environ.get('LINKEDIN_COOKIE', '')
    if not cookie_string:
        raise ValueError("LINKEDIN_COOKIE environment variable is not set")
    
    cookies = cookie_string.split('; ')
    for cookie in cookies:
        name, value = cookie.split('=', 1)
        driver.add_cookie({'name': name, 'value': value})
    
    return driver

def get_company_linkedin_name(company_name):
    # This is a simple implementation and may need to be more sophisticated
    return company_name.lower().replace(' ', '-')

def scrape_engineering_headcount(driver, company_name):
    company_url_name = get_company_linkedin_name(company_name)
    url = f"https://www.linkedin.com/company/{company_url_name}/people/?facetCurrentFunction=8&facetGeoRegion=103644278"
    
    driver.get(url)
    
    try:
        # Wait for the element containing the headcount to load
        headcount_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.org-people__header-spacing-carousel h2"))
        )
        headcount_text = headcount_element.text
        # Extract the number from the text (e.g., "1,234 employees")
        headcount = int(''.join(filter(str.isdigit, headcount_text)))
        return headcount
    except (TimeoutException, NoSuchElementException) as e:
        print(f"Could not find headcount element for {company_name}: {e}")
        return None

def scrape_headcounts(companies):
    driver = setup_driver_with_cookies()
    results = {}
    
    try:
        for company in companies:
            print(f"Scraping headcount for {company}")
            headcount = scrape_engineering_headcount(driver, company)
            if headcount:
                results[company] = headcount
                print(f"Headcount for {company}: {headcount}")
            else:
                print(f"Failed to get headcount for {company}")
            time.sleep(1)  # Add a delay to avoid rate limiting
    
    finally:
        driver.quit()
    
    return results

def test_scraper():
    print("Starting test for LinkedIn headcount scraper...")
    
    test_companies = ['Microsoft', 'Google', 'Amazon', 'Apple']
    results = scrape_headcounts(test_companies)
    
    print("\nTest Results:")
    for company, headcount in results.items():
        print(f"{company}: {headcount}")

if __name__ == "__main__":
    test_scraper()