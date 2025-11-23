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

# -------------------------------
# DATABASE CONFIG (Environment Variables)
# -------------------------------
# These will be set in Koyeb dashboard
DB_CONNECTION_STRING = os.environ.get("DB_CONNECTION_STRING", "")
DB_PARAMS = {}

if DB_CONNECTION_STRING:
    # Parse the connection string if provided
    import urllib.parse
    result = urllib.parse.urlparse(DB_CONNECTION_STRING)
    DB_PARAMS = {
        'host': result.hostname,
        'port': result.port or 5432,
        'database': result.path[1:],  # remove leading slash
        'user': result.username,
        'password': result.password
    }
else:
    # Fallback to individual environment variables
    DB_PARAMS = {
        'host': os.environ.get("DB_HOST", "ep-empty-scene-a4hzohhu.us-east-1.pg.koyeb.app"),
        'port': os.environ.get("DB_PORT", 5432),
        'database': os.environ.get("DB_NAME", "koyebdb"),
        'user': os.environ.get("DB_USER", "koyeb-adm"),
        'password': os.environ.get("DB_PASSWORD", "npg_3Ec9qVGNgAvP")
    }

# -------------------------------
# LOCATIONS & SETTINGS
# -------------------------------
locations = {
    "Gauteng": {"city": "City of Johannesburg", "geoId": "101069296"},
    "Sandton": {"city": "Sandton", "geoId": "101069300"},
    "Midrand": {"city": "Midrand", "geoId": "101069305"},
    "Randburg": {"city": "Randburg", "geoId": "101069310"}
}

allowed_locations = ["Johannesburg", "Gauteng", "Sandton", "Midrand", "Randburg", "Remote"]
MAX_JOBS_PER_LOCATION = 20

# -------------------------------
# LOGGING SETUP
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),  # Console output
        logging.FileHandler("linkedin_scraper.log")  # File output
    ]
)

# -------------------------------
# DATABASE FUNCTIONS
# -------------------------------
def get_db_connection():
    """Create and return database connection"""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        raise

def init_database():
    """Initialize database table"""
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
        
        # Create index for better performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_apply_link ON jobs(apply_link)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_scraped_at ON jobs(scraped_at)")
        
        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Database initialized successfully")
    except Exception as e:
        logging.error(f"Database initialization failed: {e}")

def save_job_to_db(job_data):
    """Save individual job to database"""
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
        logging.error(f"Failed to save job '{job_data['title']}' to database: {e}")
        return False

# -------------------------------
# SELENIUM SETUP
# -------------------------------
def setup_driver():
    """Setup and return Chrome driver"""
    options = Options()
    options.add_argument(f"user-agent={UserAgent().random}")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    # For Koyeb deployment, we might need headless
    if os.environ.get('KOYEB_DEPLOYMENT'):
        options.add_argument("--headless=new")
    
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), 
        options=options
    )
    return driver

# -------------------------------
# POPUP HANDLING
# -------------------------------
def close_signin_popup(driver):
    """Close LinkedIn sign-in popups"""
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

# -------------------------------
# CONTACT INFO EXTRACTION
# -------------------------------
def extract_contact_details(text):
    emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
    phones = re.findall(r"\b(?:\+?\d{1,3})?[\s-]?(?:\d{3}[\s-]?){2,4}\d{3,4}\b", text)
    return list(set(emails)), list(set(phones))

# -------------------------------
# JOB SCRAPING
# -------------------------------
def scrape_location_jobs(driver, city, geoId):
    """Scrape jobs for a specific location"""
    base_url = "https://www.linkedin.com/jobs/search?"
    url = f"{base_url}keywords=&location={quote(city)}&geoId={geoId}&f_TPR=r86400&position=1&pageNum=0"

    logging.info(f"Opening LinkedIn jobs URL for {city}: {url}")
    driver.get(url)
    time.sleep(3)
    close_signin_popup(driver)

    soup = BeautifulSoup(driver.page_source, "lxml")
    jobs = soup.select("div.base-card")
    logging.info(f"Found {len(jobs)} job cards on the page for {city}.")

    extracted = []

    for index, job in enumerate(jobs[:MAX_JOBS_PER_LOCATION], 1):
        try:
            title_tag = job.select_one("h3")
            title = title_tag.get_text(strip=True) if title_tag else "N/A"

            company_tag = job.select_one("h4")
            company = company_tag.get_text(strip=True) if company_tag else "N/A"

            location_tag = job.select_one(".base-search-card__metadata span")
            location_text = location_tag.get_text(strip=True) if location_tag else city

            # FILTER BY ALLOWED LOCATIONS
            if not any(loc.lower() in location_text.lower() for loc in allowed_locations):
                logging.info(f"Skipping job '{title}' in '{location_text}' (outside target locations)")
                continue

            job_link_tag = job.select_one("a.base-card__full-link")
            job_link = job_link_tag["href"] if job_link_tag else None
            if not job_link:
                logging.warning(f"Job #{index} skipped: no job link found")
                continue

            logging.info(f"Opening job #{index} for {city}: {title} | {company}")
            driver.get(job_link)
            time.sleep(2)
            close_signin_popup(driver)

            jd_soup = BeautifulSoup(driver.page_source, "lxml")
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
                "contacts": {
                    "emails": emails,
                    "phones": phones
                }
            }
            
            # Save to database immediately
            if save_job_to_db(job_data):
                extracted.append(job_data)
                logging.info(f"Job #{index} saved to database successfully for {city}.")
            else:
                logging.warning(f"Job #{index} failed to save to database for {city}.")

        except Exception as e:
            logging.error(f"Error scraping job #{index} for {city}: {e}")
            continue

    return extracted

# -------------------------------
# MAIN EXECUTION
# -------------------------------
def main():
    logging.info("=== LinkedIn Scraper Started ===")
    
    # Initialize database
    init_database()
    
    driver = None
    try:
        driver = setup_driver()
        all_results = []
        
        for loc, info in locations.items():
            jobs = scrape_location_jobs(driver, info["city"], info["geoId"])
            all_results.extend(jobs)
            logging.info(f"Completed scraping for {loc}. Found {len(jobs)} jobs.")
        
        logging.info(f"=== Scraping Complete. Total jobs saved: {len(all_results)} ===")
        
    except Exception as e:
        logging.error(f"Scraper failed: {e}")
        
    finally:
        if driver:
            driver.quit()
            logging.info("Browser closed.")
        
        logging.info("=== Scraper Finished ===")

if __name__ == "__main__":
    main()