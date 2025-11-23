import os
import re
import time
import logging
from urllib.parse import quote

import psycopg2
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager

# Database configuration
DB_HOST = os.environ.get("DB_HOST", "ep-empty-scene-a4hzohhu.us-east-1.pg.koyeb.app")
DB_PORT = os.environ.get("DB_PORT", 5432)
DB_NAME = os.environ.get("DB_NAME", "koyebdb")
DB_USER = os.environ.get("DB_USER", "koyeb-adm")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "npg_3Ec9qVGNgAvP")

# Locations & Settings
locations = {
    "Gauteng": {"city": "City of Johannesburg", "geoId": "101069296"},
    "Sandton": {"city": "Sandton", "geoId": "101069300"},
    "Midrand": {"city": "Midrand", "geoId": "101069305"},
    "Randburg": {"city": "Randburg", "geoId": "101069310"}
}

allowed_locations = ["Johannesburg", "Gauteng", "Sandton", "Midrand", "Randburg", "Remote"]
MAX_JOBS_PER_LOCATION = 20

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()]
)

def get_db_connection():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    return conn

def init_database():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id SERIAL PRIMARY KEY,
                title TEXT,
                company TEXT,
                location TEXT,
                apply_link TEXT UNIQUE,
                easy_apply TEXT,
                description TEXT,
                emails TEXT[],
                phones TEXT[],
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Database initialized successfully")
    except Exception as e:
        logging.error(f"Database initialization failed: {e}")

def save_job_to_db(job_data):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO jobs (title, company, location, apply_link, easy_apply, description, emails, phones)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (apply_link) DO NOTHING
        """, (
            job_data['title'],
            job_data['company'],
            job_data['location'],
            job_data['apply_link'],
            job_data['easy_apply'],
            job_data['description'],
            job_data['contacts']['emails'],
            job_data['contacts']['phones']
        ))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logging.error(f"Failed to save job to database: {e}")
        return False

def setup_driver():
    options = Options()
    options.add_argument(f"user-agent={UserAgent().random}")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--headless=new")
    
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), 
        options=options
    )
    return driver

def close_signin_popup(driver):
    try:
        body = driver.find_element(By.TAG_NAME, "body")
        body.send_keys(Keys.ESCAPE)
        time.sleep(1)
    except Exception:
        pass

def extract_contact_details(text):
    emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
    phones = re.findall(r"\b(?:\+?\d{1,3})?[\s-]?(?:\d{3}[\s-]?){2,4}\d{3,4}\b", text)
    return list(set(emails)), list(set(phones))

def scrape_location_jobs(driver, city, geoId):
    base_url = "https://www.linkedin.com/jobs/search?"
    url = f"{base_url}keywords=&location={quote(city)}&geoId={geoId}&f_TPR=r86400&position=1&pageNum=0"

    logging.info(f"Scraping jobs for {city}")
    driver.get(url)
    time.sleep(3)
    close_signin_popup(driver)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    jobs = soup.select("div.base-card")
    logging.info(f"Found {len(jobs)} job cards for {city}")

    extracted = []

    for index, job in enumerate(jobs[:MAX_JOBS_PER_LOCATION], 1):
        try:
            title_tag = job.select_one("h3")
            title = title_tag.get_text(strip=True) if title_tag else "N/A"

            company_tag = job.select_one("h4")
            company = company_tag.get_text(strip=True) if company_tag else "N/A"

            location_tag = job.select_one(".base-search-card__metadata span")
            location_text = location_tag.get_text(strip=True) if location_tag else city

            if not any(loc.lower() in location_text.lower() for loc in allowed_locations):
                continue

            job_link_tag = job.select_one("a.base-card__full-link")
            job_link = job_link_tag["href"] if job_link_tag else None
            if not job_link:
                continue

            driver.get(job_link)
            time.sleep(2)
            close_signin_popup(driver)

            jd_soup = BeautifulSoup(driver.page_source, "html.parser")
            description_tag = jd_soup.select_one(".jobs-description__content, .show-more-less-html__markup")
            description = description_tag.get_text(" ", strip=True) if description_tag else "N/A"

            emails, phones = extract_contact_details(description)

            easy_apply = None
            if jd_soup.find("button", string=lambda t: t and "Easy Apply" in t):
                easy_apply = job_link

            job_data = {
                "title": title,
                "company": company,
                "location": location_text,
                "apply_link": job_link,
                "easy_apply": easy_apply,
                "description": description,
                "contacts": {"emails": emails, "phones": phones}
            }
            
            if save_job_to_db(job_data):
                extracted.append(job_data)
                logging.info(f"Job #{index} saved: {title}")

        except Exception as e:
            logging.error(f"Error scraping job #{index}: {e}")
            continue

    return extracted

def main():
    logging.info("=== LinkedIn Scraper Started ===")
    init_database()
    
    driver = None
    try:
        driver = setup_driver()
        all_results = []
        
        for loc, info in locations.items():
            jobs = scrape_location_jobs(driver, info["city"], info["geoId"])
            all_results.extend(jobs)
            logging.info(f"Completed {loc}: {len(jobs)} jobs")
        
        logging.info(f"=== FINISHED: {len(all_results)} total jobs saved ===")
        
    except Exception as e:
        logging.error(f"Scraper failed: {e}")
    finally:
        if driver:
            driver.quit()
        logging.info("=== Scraper Finished ===")

if __name__ == "__main__":
    main()
