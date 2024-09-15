import os
import duckdb
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import time
import urllib.parse
import argparse

def setup_driver_with_cookies():
    try:
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
    except WebDriverException as e:
        print(f"ChromeDriver error: {e.msg}")
        return None

def create_engineering_headcounts_table(conn):
    create_sequence_query = """
    CREATE SEQUENCE IF NOT EXISTS engineering_headcounts_id_seq;
    """
    conn.execute(create_sequence_query)

    create_table_query = """
    CREATE TABLE IF NOT EXISTS engineering_headcounts (
        id INTEGER PRIMARY KEY DEFAULT nextval('engineering_headcounts_id_seq'),
        company VARCHAR,
        headcount INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(company, headcount)
    );
    """
    conn.execute(create_table_query)
    print("Engineering headcounts table created or already exists.")

def find_company_linkedin_url(driver, company_name):
    search_url = f"https://www.linkedin.com/search/results/all/?keywords={urllib.parse.quote(company_name)}"
    driver.get(search_url)
    
    try:
        # Wait for the search results to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "span.entity-result__title-line.entity-result__title-line--2-lines.pt1"))
        )
        
        # Find all title lines
        title_lines = driver.find_elements(By.CSS_SELECTOR, "span.entity-result__title-line.entity-result__title-line--2-lines.pt1")
        
        for title_line in title_lines:
            # Find the anchor tag within the title line
            anchor = title_line.find_element(By.TAG_NAME, "a")
            href = anchor.get_attribute('href')
            
            # Check if it's a company URL
            if '/company/' in href:
                company_url_name = href.split('/company/')[1].split('/')[0]
                # Verify if the company name matches (case-insensitive)
                if company_name.lower() in anchor.text.lower():
                    return company_url_name
        
        print(f"Could not find a matching company URL for {company_name}")
        return None
    except (TimeoutException, NoSuchElementException, IndexError) as e:
        print(f"Error finding company URL for {company_name}")
        return None

def scrape_engineering_headcount(driver, company_name):
    company_url_name = find_company_linkedin_url(driver, company_name)
    if not company_url_name:
        return None
    
    url = f"https://www.linkedin.com/company/{company_url_name}/people/?facetCurrentFunction=8&facetGeoRegion=103644278"
    driver.get(url)
    
    try:
        headcount_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.org-people__header-spacing-carousel h2"))
        )
        headcount_text = headcount_element.text
        headcount = int(''.join(filter(str.isdigit, headcount_text)))
        return headcount
    except (TimeoutException, NoSuchElementException) as e:
        print(f"Could not find headcount element for {company_name}: {e}")
        return None

def get_unique_companies(conn):
    query = """
    SELECT company
    FROM jobs
    WHERE NOT EXISTS (
        SELECT 1
        FROM engineering_headcounts
        WHERE jobs.company = engineering_headcounts.company
    )
    GROUP BY company
    HAVING COUNT(*) > 5
    ORDER BY COUNT(*) DESC
    """
    result = conn.execute(query).fetchall()
    return [row[0] for row in result]

def scrape_headcounts(companies):
    driver = setup_driver_with_cookies()
    if driver is None:
        print("Failed to set up ChromeDriver. Exiting.")
        return {}

    results = {}
    
    try:
        for index, company in enumerate(companies, start=1):
            print(f"[{index}/{len(companies)}] Scraping headcount for {company}")
            headcount = scrape_engineering_headcount(driver, company)
            if headcount:
                results[company] = headcount
                print(f"[{index}/{len(companies)}] Headcount for {company}: {headcount}")
            else:
                print(f"[{index}/{len(companies)}] Failed to get headcount for {company}")
            time.sleep(0.5)  # Reduced delay to 0.5 seconds
    
    except WebDriverException as e:
        print(f"ChromeDriver error during scraping: {e.msg}")
    finally:
        if driver:
            driver.quit()
    
    return results

def insert_headcount(conn, company, headcount):
    insert_query = """
    INSERT OR IGNORE INTO engineering_headcounts (company, headcount)
    VALUES (?, ?)
    """
    conn.execute(insert_query, (company, headcount))

def test_scraper():
    print("Starting test for LinkedIn headcount scraper...")
    
    test_companies = ['Uber', 'Tinder']
    results = scrape_headcounts(test_companies)
    
    print("\nTest Results:")
    for company, headcount in results.items():
        print(f"{company}: {headcount}")

def main(test_mode=False):
    if test_mode:
        test_scraper()
        return

    print("Starting LinkedIn headcount scraper...")
    
    # Connect to the database
    conn = duckdb.connect('linkedin_jobs.db')
    
    try:
        # Create the table if it doesn't exist
        create_engineering_headcounts_table(conn)
        
        # Get unique companies from JOBS table
        companies = get_unique_companies(conn)
        print(f"Found {len(companies)} unique companies to scrape")
        
        # Scrape headcounts
        results = scrape_headcounts(companies)
        
        # Persist the data
        for company, headcount in results.items():
            if headcount:
                insert_headcount(conn, company, headcount)
        
        conn.commit()
        print(f"Inserted headcounts for {len(results)} companies")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        conn.rollback()
    finally:
        conn.close()
    
    print("Script execution completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LinkedIn Headcount Scraper")
    parser.add_argument("--test", action="store_true", help="Run in test mode")
    args = parser.parse_args()

    main(test_mode=args.test)