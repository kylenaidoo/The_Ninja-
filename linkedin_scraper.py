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

# Database configuration
DB_HOST = os.environ.get("DB_HOST", "ep-empty-scene-a4hzohhu.us-east-1.pg.koyeb.app")
DB_PORT = os.environ.get("DB_PORT", 5432)
DB_NAME = os.environ.get("DB_NAME", "koyebdb")
DB_USER = os.environ.get("DB_USER", "koyeb-adm")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "npg_3Ec9qVGNgAvP")

# Locations & Settings
locations = {
    "Gauteng": {"city": "City of Johannesburg", "geoId": "101069296"}
}

allowed_locations = ["Johannesburg", "Gauteng", "Sandton", "Midrand", "Randburg", "Remote"]
MAX_JOBS_PER_LOCATION = 5  # Reduced for testing

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
    # REMOVED headless to match your working local script
    # options.add_argument("--headless=new")

    # Use manually installed ChromeDriver
    service = Service("/usr/local/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def close_signin_popup(driver):
    try:
        body = driver.find_element(By.TAG_NAME, "body")
        body.send_keys(Keys.ESCAPE)
        time.sleep(1)
        logging.info("Sent ESC to close popup")
    except Exception:
        pass

    try:
        close_buttons = driver.find_elements(By.XPATH, "//button[contains(@class,'artdeco-modal__dismiss')]")
        for btn in close_buttons:
            try:
                btn.click()
                logging.info("Clicked popup X button")
                time.sleep(1)
                break
            except:
                continue
    except Exception:
        pass

def extract_contact_details(text):
    emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
    phones = re.findall(r"\b(?:\+?\d{1,3})?[\s-]?(?:\d{3}[\s-]?){2,4}\d{3,4}\b", text)
    return list(set(emails)), list(set(phones))

def scrape_location_jobs(driver, city, geoId):
    base_url = "https://www.linkedin.com/jobs/search?"
    url = f"{base_url}keywords=&location={quote(city)}&geoId={geoId}&f_TPR=r86400&position=1&pageNum=0"

    logging.info(f"Opening LinkedIn jobs URL for {city}: {url}")
    driver.get(url)
    time.sleep(5)
    close_signin_popup(driver)

    # DEBUG: Save the actual page content to see what we're getting
    page_source = driver.page_source
    
    # Check if we're getting a sign-in page or blocked
    if "sign in" in page_source.lower():
        logging.error("🚨 DETECTED SIGN-IN PAGE - LinkedIn is blocking us!")
    if "robot" in page_source.lower() or "captcha" in page_source.lower():
        logging.error("🚨 DETECTED BOT/BLOCK PAGE - LinkedIn is blocking automation!")
    
    # Save page title and first 1000 chars for debugging
    soup = BeautifulSoup(page_source, "html.parser")
    page_title = soup.find("title")
    logging.info(f"📄 PAGE TITLE: {page_title.get_text() if page_title else 'NO TITLE FOUND'}")
    
    # Log the actual page content for debugging
    page_preview = page_source[:1000] if len(page_source) > 1000 else page_source
    logging.info(f"🔍 PAGE CONTENT PREVIEW: {page_preview}")
    
    # Try multiple selectors for job cards
    selectors = [
        "div.base-card",
        "li.jobs-search-results__list-item", 
        "div.job-search-card",
        "div.occludable-update",
        "[data-entity-urn*='jobPosting']",
        ".job-card-container",
        ".jobs-search-results__list-item"
    ]
    
    jobs = []
    for selector in selectors:
        found_jobs = soup.select(selector)
        if found_jobs:
            logging.info(f"✅ Found {len(found_jobs)} jobs with selector: {selector}")
            jobs = found_jobs
            break
        else:
            logging.info(f"❌ No jobs with selector: {selector}")
    
    if not jobs:
        logging.error("❌ NO JOB CARDS FOUND WITH ANY SELECTOR!")
        # Try to find ANY divs with job-related classes
        all_divs = soup.find_all('div', class_=True)
        job_related_divs = [div for div in all_divs if any(word in div.get('class', []) for word in ['job', 'card', 'list', 'result'])]
        logging.info(f"🔍 Found {len(job_related_divs)} potentially job-related divs")
        
    logging.info(f"📊 Total job cards found for {city}: {len(jobs)}")

    extracted = []

    for index, job in enumerate(jobs[:MAX_JOBS_PER_LOCATION], 1):
        try:
            # Try multiple selectors for job title
            title = "N/A"
            for selector in ["h3", "h3.base-search-card__title", ".job-card-list__title", ".job-card-search__title"]:
                title_tag = job.select_one(selector)
                if title_tag and title_tag.get_text(strip=True):
                    title = title_tag.get_text(strip=True)
                    break

            if title == "N/A":
                logging.warning(f"Job #{index}: No title found")
                continue

            logging.info(f"🎯 Processing job #{index}: {title}")
            extracted.append({"title": title, "company": "TEST", "location": city, "apply_link": "TEST"})
            
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
        
        # Test with just ONE location for debugging
        for loc, info in list(locations.items())[:1]:
            jobs = scrape_location_jobs(driver, info["city"], info["geoId"])
            all_results.extend(jobs)
            logging.info(f"Completed {loc}: {len(jobs)} jobs")
        
        logging.info(f"=== FINISHED: {len(all_results)} total jobs found ===")
        
    except Exception as e:
        logging.error(f"Scraper failed: {e}")
    finally:
        if driver:
            driver.quit()
        logging.info("=== Scraper Finished ===")

if __name__ == "__main__":
    main()
