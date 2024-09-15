import requests
import re
from bs4 import BeautifulSoup
import os
from enum import Enum
import time
from sympy import fibonacci
import pandas as pd
import duckdb
from tabulate import tabulate

class DebugLevel(Enum):
    WARN = 0
    GENERAL = 1
    GRANULAR = 2

class Constants:
    # Configurations
    db_name = 'linkedin_jobs.db'
    table_name = 'jobs'
    base_url_prefix = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=software%20engineer%20OR%20engineering%20manager&location=United%2BStates&geoId=103644278&trk=public_jobs_jobs-search-bar_search-submit&start="
    staff_url_postfix = "/people/?facetCurrentFunction=8&facetGeoRegion=103644278"
    suffix_to_remove = "?trk=public_jobs_jserp-result_job-search-card-subtitle"
    row_increment_default = 10
    max_row_default = 0
    EM_AS_ENG_MULTIPLIER = 3
    csv_name = "linkedin-job-scraper-database.csv"
    csv_columns = ['job_id', 'company', 'job_type', 'title', 'location', 'link', 'date']
    debug_level = None
    debug_company = ""
    ignore_list = ["SynergisticIT", "Kforce Inc", "ICONMA", "Team Remotely Inc", "Ampcus Inc", "Genesis10", "Intellectt Inc", "Stealth", "LanceSoft, Inc.", "Insight Global", "Griffin Global Systems, Inc.", "EVONA", "Steneral Consulting", "TALENT Software Services", "Avid Technology Professionals"]
    ignore_words = ["Job", "Recruit", "Career", "Hire", "Staffing", "Work"]
    cookie_value = 'lang=v=2&lang=en-us; AMCVS_14215E3D5995C57C0A495C55%40AdobeOrg=1; spectroscopyId=596c9ff3-60d0-41e3-85d0-35f6a301a11d; lil-lang=en_US; s_cc=true; li_gp=MTsxNjk1ODI4MzEwOzA=; JSESSIONID="ajax:8314525928829728200"; li_sugr=25841127-ed35-4ab9-8c85-570471870609; at_check=true; dfpfpt=7c7fe9cf02884edfa5a39e7017cfb32b; li_theme=dark; li_theme_set=user; bcookie="v=2&cb2fb2a1-4d80-45fc-8516-207f51a51f3e"; bscookie="v=1&202403021342299a907719-a943-42ef-8402-5792074858caAQE5A2dVIE09Qv30nLvy78tZY6gf5eEG"; liap=true; PLAY_SESSION=eyJhbGciOiJIUzI1NiJ9.eyJkYXRhIjp7ImZsb3dUcmFja2luZ0lkIjoieThwNEdpbUdUMFNVZUxHOXB6a2JmUT09In0sIm5iZiI6MTcxMDU5NDUxOSwiaWF0IjoxNzEwNTk0NTE5fQ.zR1TJu4vkt13WOLRPTwV50YI6uKXf5-UWWpz1X8QgMg; li_ep_auth_context=AFlhcHA9YWNjb3VudENlbnRlckh1YixhaWQ9MjAzNDA1ODI2LGlpZD0yMDMyNzg1MDAscGlkPTE5MjE3ODY2MCxleHA9MTcxMjg3NzAwNTE0OSxjdXI9dHJ1ZQAxwAx3K9P0J545UbnU6UvphQmb5A; _guid=420eeedd-0945-4be8-bb3d-7560213e03fb; timezone=America/Vancouver; s_sq=%5B%5BB%5D%5D; s_plt=1.46; s_pltp=developer.linkedin.com%2Fproduct-catalog%2Ftalent; s_ppv=developer.linkedin.com%2Fproduct-catalog%2Ftalent%2C100%2C55%2C1423.5%2C1%2C1; sdsc=22%3A1%2C1716311324586%7EJAPP%2C0dedPjDVh%2BnAliGcpJp0roYoGNuw%3D; li_at=AQEFAHIBAAAAAA8jfCgAAAGOR10hfQAAAY_uskrwVgAAFXVybjpsaTptZW1iZXI6MzM4NDU2NU6brTbPy_8D6pC-LEWNMQWWggzdn90tgsgOGINZ1JiCUwWg0jjZ4fwOCHI8ibezBk-sq2rG0gWHW-ulrvjBzzMhMlq9Vc9YUoPNP2Oe7L6KZhStednWSbMHFDxEMKi0ozXeE3NSqQ_1zMcVxS1Sy8Psa_ecRtrlglvRK8p-WSlgOGV6kfw-eN32XyXFuFvKwHgyBRg; AnalyticsSyncHistory=AQIZphPFkK3ZKQAAAY_Mzq7abzj-6wvwEgRLs0GJCSzxG08LOA_E2744k-Zp5x5TZGw_AnE9lEIEseNrdpFCKw; lms_ads=AQFvxAKBoK1iygAAAY_Mzq9v3DOdlaVZl4qxnR_BD7cXTWpIKFhF2v_tJ4KGNQOfpXbWWzd-IfUly3XlZV7sYqipcUhV80yD; lms_analytics=AQFvxAKBoK1iygAAAY_Mzq9v3DOdlaVZl4qxnR_BD7cXTWpIKFhF2v_tJ4KGNQOfpXbWWzd-IfUly3XlZV7sYqipcUhV80yD; fptctx2=taBcrIH61PuCVH7eNCyH0APzNoEiOrOqF4FbdtfiWWLEKzm%252bX525gSVvOxw6HIhb348dZ6J1N8EuDRt0U2jbG82ZGirLI%252bhHcWa0hlUWdLUPXJFCfJ5w7PthZ7NuHITSyG8i6hWyXkR2x66zZbxKzFN11rsxN3eYrgNtIUhlMWVeBF1S5OSejF9QWpkgnrgpntUiBR7KmqIrTSIIa9KjKeE4dyVvuj3O2%252fT3wUUcVS%252bFKOUFa7ZD6BkrXrm%252fGEDCkuD8oHpH6sA83a8WZOsUC60sKxVfykYg%252bmYz8%252f8%252fSzfEHTEP1VvVAovOpVsO%252fLK8pyIW80Dtsb%252fcMu4eBEwfWMbkWmjlwuuYKmJZdJbC%252bDA%253d; UserMatchHistory=AQJLBw2SJxerMwAAAY_QtvsWhUBlJ5CfWM3192ID3TVM6nwb-UgUr00WbhMhwjEJKuPgcKeeck_ZJYmdqJ1v6B90twjdmoh5hrRQDFjTAG9KjDjBkYVMq68jUxHwblY2z0YSaSe68KjSA8HNO--3yyuFSGWNmTmODCEs6kUyswfs_Uk0PsQQgJyy9MkZIDhSk1n_4oYpVUkcrjlTvAphEwTl2y6VnRNN_laDwx_YefOrXG4b3Os4UbYIDS-ZHGp1MytvWuSLl5Cihf-TE2i5zYlsiS3F1F8R5eOAK-d5tqHzVLmBw3t3Q1HaacbSoGQqstL5ihsDnXVPrJNqCleLRBzV3Inu7p3LJgkZ7xKVNQW0ekvIQw; lidc="b=OB65:s=O:r=O:a=O:p=O:g=4802:u=470:x=1:i=1717193606:t=1717266555:v=2:sig=AQEuCN_ZyU-YIDV_x-0QRtE5Gy9QBpeQ"'
    headers = {
        "Cookie": cookie_value
    }

# Global mutable variables
jobs = {}  # updated cache, won't be written back to csv
jobs_to_add = []  # write-back as additions, strictly as a write buffer

def create_table_if_not_exists(conn, table_name):
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        job_id VARCHAR PRIMARY KEY,
        company VARCHAR,
        job_type VARCHAR,
        title VARCHAR,
        location VARCHAR,
        link VARCHAR,
        date VARCHAR
    )
    """
    conn.execute(create_table_query)

def is_ignored(company):
    c = Constants()
    if company in c.ignore_list or any(word in company for word in c.ignore_words):
        print(f"Ignoring company from computation: {company}")
        return True
    return False

def parse_job(job):
    try:
        date = job.find('time', {'class':'job-search-card__listdate'}).attrs['datetime']
    except AttributeError:
        date = job.find('time', {'class':'job-search-card__listdate--new'}).attrs['datetime']
    
    title = job.find('h3', {'class': 'base-search-card__title'}).text.strip()
    job_type = 'em' if 'manager' in title.lower() else 'eng'
    company = job.find('a', {'class': 'hidden-nested-link'}).text.strip()
    location = job.find('span', {'class': 'job-search-card__location'}).text.strip()
    link = job.find('a', {'class': 'base-card__full-link'}).attrs['href']
    pattern = r"/view/.*?(\d+)"
    try:
        job_id = re.search(pattern, link).group(1)
    except Exception as e:
        print(link)
        print(e)
    return {
        'date': date,
        'title': title,
        'job_type': job_type,
        'company': company,
        'location': location,
        'link': link,
        'job_id': job_id,
    }

def debug_data(job_id, company, job_type, title, location, link, date):
    c = Constants()
    if c.debug_level == DebugLevel.GRANULAR and c.debug_company != "" and company == c.debug_company:
        print(f"id:{job_id},title:{title},job_type:{job_type},link:{link}")
    elif c.debug_level == DebugLevel.GENERAL:
        print(f"id:{job_id},title:{title},job_type:{job_type},company:{company}")

def upsert_jobs_to_add(job_id, company, job_type, title, location, link, date, conn):
    c = Constants()
    debug_data(job_id, company, job_type, title, location, link, date)
    
    upsert_query = f"""
    INSERT INTO {c.table_name} (job_id, company, job_type, title, location, link, date)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (job_id) DO UPDATE SET
        company = excluded.company,
        job_type = excluded.job_type,
        title = excluded.title,
        location = excluded.location,
        link = excluded.link,
        date = excluded.date
    """
    conn.execute(upsert_query, (job_id, company, job_type, title, location, link, date))

def setup_database():
    c = Constants()
    conn = duckdb.connect(c.db_name)
    create_table_if_not_exists(conn, c.table_name)
    return conn

def load_existing_jobs(conn):
    c = Constants()
    # Only fetch the job IDs to check for duplicates
    existing_job_ids = set(row[0] for row in conn.execute(f"SELECT job_id FROM {c.table_name}").fetchall())
    print(f"Number of existing jobs in DuckDB: {len(existing_job_ids)}")
    return existing_job_ids

def scrape_linkedin_jobs(conn, existing_job_ids):
    c = Constants()
    n = 0  # Start with no sleep
    i = 0
    jobs_to_add = []
    while i <= c.max_row_default:
        base_url = f"{c.base_url_prefix}{i}"
        print(f"Fetching URL: {base_url}")
        
        if n > 0:
            sleep_time = fibonacci(n)
            print(f"Sleeping for {sleep_time} seconds")
            time.sleep(sleep_time)
        
        try:
            response = requests.get(base_url, headers=c.headers)
            print(f"Response status code: {response.status_code}")

            if response.status_code == 200:
                i += c.row_increment_default
                job_listings = process_response(response)
                process_job_listings(conn, existing_job_ids, job_listings, jobs_to_add)
                n = max(0, n - 1)  # Decrease sleep time if successful, but not below 0
            elif response.status_code == 429:
                n = handle_429_response(n)
            elif response.status_code == 400:
                print("Received 400 status code. Ending scraping and proceeding to data persistence and analysis.")
                break  # Exit the loop to proceed with data persistence and analysis
            else:
                print(f"Received unexpected status code: {response.status_code}. Proceeding to next iteration.")
                i += c.row_increment_default  # Move to next page despite the error
        except Exception as e:
            print(f"Error during request or parsing: {e}")
            i += c.row_increment_default  # Move to next page despite the error

    print(f"Finished scraping. Total new jobs found: {len(jobs_to_add)}")
    return jobs_to_add

def process_response(response):
    soup = BeautifulSoup(response.text, 'html.parser')
    job_listings = soup.find_all('div', {'class':'job-search-card'})
    print(f"Found {len(job_listings)} job listings")
    return job_listings

def process_job_listings(conn, existing_job_ids, job_listings, jobs_to_add):
    c = Constants()
    for job in job_listings:
        try:
            job_fields = parse_job(job)
            if is_ignored(job_fields['company']):
                continue
            
            if job_fields['job_id'] not in existing_job_ids:
                upsert_jobs_to_add(job_fields['job_id'], job_fields['company'], job_fields['job_type'], 
                                   job_fields['title'], job_fields['location'], job_fields['link'], 
                                   job_fields['date'], conn)
                jobs_to_add.append(job_fields['job_id'])
        except Exception as e:
            print(f"Error parsing job: {e}")
    
    print(f"Total jobs to add after this page: {len(jobs_to_add)}")

def handle_429_response(n):
    n += 1
    sleep_time = fibonacci(n)
    print(f"Received 429 (Too Many Requests). Retrying after {sleep_time} seconds")
    time.sleep(sleep_time)
    return n

def analyze_data(conn):
    c = Constants()
    try:
        # Perform analysis directly in SQL
        analysis_query = f"""
        WITH job_counts AS (
            SELECT 
                company,
                SUM(CASE WHEN job_type = 'eng' THEN 1 ELSE 0 END) as eng_count,
                SUM(CASE WHEN job_type = 'em' THEN {c.EM_AS_ENG_MULTIPLIER} ELSE 0 END) as em_count,
                MAX(date) as max_date,
                MIN(date) as min_date
            FROM {c.table_name}
            GROUP BY company
        ),
        total_counts AS (
            SELECT 
                company,
                eng_count + em_count as total_count,
                max_date,
                min_date
            FROM job_counts
        ),
        percentile AS (
            SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_count) as p95
            FROM total_counts
        )
        SELECT company, total_count, max_date, min_date
        FROM total_counts, percentile
        WHERE total_count > p95
        ORDER BY total_count DESC
        """
        result = conn.execute(analysis_query).fetchall()
        print("Top companies by job count:")
        
        # Prepare data for tabulate
        table_data = [[row[0], row[1], row[2], row[3]] for row in result]
        headers = ["Company", "Total Count", "Latest Date", "Earliest Date"]
        
        # Print the table
        print(tabulate(table_data, headers=headers, tablefmt="grid"))
    except Exception as e:
        print(f"Error during SQL analysis: {e}")

def main():
    print("Starting the LinkedIn job scraper...")
    
    conn = None
    try:
        conn = setup_database()
        existing_job_ids = load_existing_jobs(conn)
        
        jobs_to_add = scrape_linkedin_jobs(conn, existing_job_ids)
        
        conn.commit()
        print(f"{len(jobs_to_add)} jobs added to DuckDB")
        
        analyze_data(conn)
        
    except Exception as e:
        print(f"An error occurred: {e}")
        if conn:
            conn.rollback()
            print("Transaction rolled back due to error.")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")
    
    print("Script execution completed.")

if __name__ == "__main__":
    main()
