import time
import random
import logging
import chromedriver_autoinstaller
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, validator
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from langchain.document_loaders import SeleniumURLLoader
import requests
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import os
import base64
import tempfile
import shutil
from urllib.parse import quote_plus
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed  # For parallelization
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')


# Constants for rate limiting
MAX_REQUESTS_PER_MINUTE = 7
REQUEST_WINDOW_SECONDS = 60  # seconds for rate limiter window


class RateLimiter:
    def __init__(self, max_requests, window_seconds):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.request_times = []

    def wait_if_needed(self):
        now = time.monotonic()
        self.request_times = [t for t in self.request_times if now - t < self.window_seconds]
        if len(self.request_times) >= self.max_requests:
            sleep_time = self.window_seconds - (now - self.request_times[0])
            logging.info(f"Rate limit reached, sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        self.request_times.append(time.monotonic())


rate_limiter = RateLimiter(MAX_REQUESTS_PER_MINUTE, REQUEST_WINDOW_SECONDS)


def random_mouse_movements(driver):
    """Simulate human-like random mouse movements."""
    try:
        action = ActionChains(driver)
        window_size = driver.get_window_size()
        for _ in range(random.randint(3, 7)):
            x = random.randint(0, window_size['width'])
            y = random.randint(0, window_size['height'])
            action.move_by_offset(x, y).perform()
            time.sleep(random.uniform(0.2, 0.7))
        action.reset_actions()
    except Exception:
        pass


def random_scroll(driver):
    """Simulate human-like random scrolling behavior."""
    try:
        scroll_height = driver.execute_script("return document.body.scrollHeight")
        for _ in range(random.randint(2, 5)):
            scroll_pos = random.randint(0, scroll_height)
            driver.execute_script(f"window.scrollTo(0, {scroll_pos});")
            time.sleep(random.uniform(0.5, 1.5))
    except Exception:
        pass


def maybe_simulate_user_interaction(driver, probability=0.5):
    """
    Randomly simulate user interactions (mouse move + scroll) on ~50% of page loads.
    """
    if random.random() < probability:
        random_mouse_movements(driver)
        random_scroll(driver)


def random_delay(min_seconds=4, max_seconds=10):
    """Random delay to avoid detection; adjustable."""
    delay = random.uniform(min_seconds, max_seconds)
    logging.info(f"Sleeping for {delay:.2f} seconds to avoid detection")
    time.sleep(delay)


def retry_with_backoff(func, max_retries=3):
    """Retries a function with exponential backoff on failure."""
    delay = 5
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            logging.warning(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                logging.info(f"Backing off for {delay} seconds before retry")
                time.sleep(delay)
                delay *= 2
            else:
                raise


class AdaptiveController:
    """Adaptive delay controller: decreases delay after success, increases after failure."""
    def __init__(self, base_delay=10, max_delay=30):
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.current_delay = base_delay
        self.fail_count = 0

    def record_success(self):
        self.fail_count = 0
        self.current_delay = max(self.base_delay, self.current_delay - 1)

    def record_failure(self):
        self.fail_count += 1
        self.current_delay = min(self.max_delay, self.current_delay * 1.5)

    def wait(self):
        logging.info(f"Adaptive delay: sleeping for {self.current_delay:.2f} seconds")
        time.sleep(self.current_delay)


app = FastAPI(title="NCLT Court Cases Scraper 7")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:4200",    # Angular dev server
        "http://192.168.1.77:4444"  # Production server
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

base_search_url = "https://nclt.gov.in/party-name-wise-search"

benches = {
    "Mumbai": "bXVtYmFp",
    "Chennai": "Y2hlbm5haQ%3D%3D",
    "Delhi": "ZGVsaGk",
    "Bangalore": "YmVuZ2FsdXJ1",
    "Ahmedabad": "a29sa2F0YQ%3D%3D",
    "Kolkata": "a29sa2F0YQ",
    "Kochi": "a29jaGk%3D",
    "Jaipur": "amFpcHVy",
    "Indore": "aW5kb3Jl",
    "Hyderabad": "aHlkZXJhYmFk",
    "Guwahati": "Z3V3YWhhdGk%3D",
    "cuttak": "Y3V0dGFjaw",
    "Chandigarh": "Y2hhbmRpZ2FyaA",
    "Amaravati": "YW1yYXZhdGk",
    "Allahabad": "YWxsYWhhYmFk"
}

party_type_encoded = "Mw=="

case_status_map = {
    "Pending": "UA==",
    "Disposed": "RA=="
}

output_root = "output"

class ScrapeRequest(BaseModel):
    company_name: str
    benches: List[str]
    years: List[int] = [2025]
    statuses: List[str] = ["Pending", "Disposed"]

    @validator('company_name')
    def validate_company_name(cls, v):
        if not v.strip():
            raise ValueError("Company name must not be empty")
        return v.strip()

    @validator('benches')
    def validate_benches(cls, v):
        if not v:
            raise ValueError("At least one bench must be selected")
        invalid = set(v).difference(set(benches.keys()))
        if invalid:
            raise ValueError(f"Invalid benches specified: {', '.join(invalid)}")
        return v

    @validator('statuses', each_item=True)
    def validate_statuses(cls, v):
        if v not in case_status_map:
            raise ValueError(f"Invalid case status: {v}")
        return v

    @validator('years', each_item=True)
    def validate_years(cls, v):
        if v < 2005 or v > 2100:
            raise ValueError(f"Year {v} out of valid range (2005-2100)")
        return v


def create_driver():
    chromedriver_autoinstaller.install() 
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")  # Disable GPU to avoid errors
    options.add_argument("--disable-blink-features=AutomationControlled")  # Hide automation flag
    # Set a common realistic user agent string
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    )

    temp_dir = tempfile.mkdtemp()
    options.add_argument(f"--user-data-dir={temp_dir}")

    driver = webdriver.Chrome(options=options)

    # Further evade navigator.webdriver property
    driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
        'source': '''
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        '''
    })

    return driver, temp_dir


def encode_party_name(company_name: str) -> str:
    encoded_bytes = base64.b64encode(company_name.encode())
    encoded_str = encoded_bytes.decode()
    return quote_plus(encoded_str)


def download_pdfs_directly(pdf_links, folder_path):
    downloaded_pdfs = []
    for pdf_url in pdf_links:
        pdf_filename = os.path.join(folder_path, pdf_url.split('/')[-1].split('?')[0])
        try:
            r = requests.get(pdf_url, timeout=15)
            r.raise_for_status()
            with open(pdf_filename, 'wb') as f:
                f.write(r.content)
            downloaded_pdfs.append(pdf_filename)
            time.sleep(random.uniform(2, 5))  # shorter delay for faster execution
        except Exception:
            logging.warning(f"Failed to download PDF {pdf_url}")
    return downloaded_pdfs


# Helper function to scrape a single case andd used for parallel execution
def scrape_single_case(case_link, folder_path, bench_name, year, status_name):
    driver, temp_dir = create_driver()
    try:
        rate_limiter.wait_if_needed()
        maybe_simulate_user_interaction(driver, probability=0.5)
        retry_with_backoff(lambda: driver.get(case_link))
        random_delay()

        loader = SeleniumURLLoader(urls=[case_link])
        documents = loader.load()
        page_text_path = os.path.join(folder_path, f"{case_link.split('=')[-1]}.txt")
        with open(page_text_path, "w", encoding="utf-8") as f:
            for doc in documents:
                f.write(doc.page_content)

        links = [elem.get_attribute('href') for elem in driver.find_elements(By.TAG_NAME, 'a')]
        pdf_links = [link for link in links if link and link.lower().endswith('.pdf')]
        downloaded_pdfs = download_pdfs_directly(pdf_links, folder_path)

        return {
            "bench": bench_name,
            "year": year,
            "status": status_name,
            "case_link": case_link,
            "text_file": page_text_path,
            "pdfs": downloaded_pdfs,
        }
    finally:
        driver.quit()
        shutil.rmtree(temp_dir)


@app.post("/scrape")
async def scrape_cases(req: ScrapeRequest):
    os.makedirs(output_root, exist_ok=True)
    encoded_party_name = encode_party_name(req.company_name)

    scraped_data = []
    adaptive_ctrl = AdaptiveController()

    try:
        for bench_name in req.benches:
            bench_encoded = benches[bench_name]
            for year in req.years:
                year_encoded = base64.b64encode(str(year).encode()).decode()
                for status_name in req.statuses:
                    if status_name not in case_status_map:
                        continue
                    status_encoded = case_status_map[status_name]

                    url = (
                        f"{base_search_url}?bench={bench_encoded}&party_type={party_type_encoded}"
                        f"&party_name={encoded_party_name}&case_year={year_encoded}&case_status={status_encoded}"
                    )

                    logging.info(f"Navigating to search URL: {url}")

                    rate_limiter.wait_if_needed()

                    driver, temp_dir = create_driver()
                    try:

                        maybe_simulate_user_interaction(driver, probability=0.5)
                        retry_with_backoff(lambda: driver.get(url))
                        adaptive_ctrl.record_success()
                    except Exception as e:
                        adaptive_ctrl.record_failure()
                        logging.error(f"Failed to load URL {url}: {e}")
                        driver.quit()
                        shutil.rmtree(temp_dir)
                        continue

                    adaptive_ctrl.wait()

                    try:
                        table_rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
                        if not table_rows or "please click here" in table_rows[0].text.lower():
                            logging.info(f"No case data found for bench {bench_name}, year {year}, status {status_name}")
                            driver.quit()
                            shutil.rmtree(temp_dir)
                            continue
                    except Exception as e:
                        logging.error(f"Error accessing table rows: {e}")
                        driver.quit()
                        shutil.rmtree(temp_dir)
                        continue

                    case_links = []
                    for row in table_rows:
                        try:
                            link_elem = row.find_element(By.TAG_NAME, "a")
                            case_url = link_elem.get_attribute('href')
                            if case_url:
                                case_links.append(case_url)
                        except Exception:
                            continue

                    folder_path = os.path.join(output_root, req.company_name, bench_name, str(year), status_name)
                    os.makedirs(folder_path, exist_ok=True)

                    driver.quit()
                    shutil.rmtree(temp_dir)

                    # Parallelize case fetching with 3 threads
                    with ThreadPoolExecutor(max_workers=3) as executor:
                        futures = {
                            executor.submit(
                                scrape_single_case,
                                case_link,
                                folder_path,
                                bench_name,
                                year,
                                status_name,
                            ): case_link
                            for case_link in case_links
                        }

                        for future in as_completed(futures):
                            try:
                                case_data = future.result()
                                scraped_data.append(case_data)
                                adaptive_ctrl.record_success()
                            except Exception as e:
                                adaptive_ctrl.record_failure()
                                logging.warning(f"Failed to scrape case {futures[future]}: {e}")

        return {"message": "Scraping completed successfully", "results": scraped_data}

    except Exception as e:
        logging.error(f"Unexpected error during scraping: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    
app.mount("/files", StaticFiles(directory="output"), name="files")

@app.get("/download")
async def download_file(file_path: str):
    """ Simple file download endpoint """
    try:
        from urllib.parse import unquote
        
        # Decode the URL-encoded path
        decoded_path = unquote(file_path)
        print(f"Requested file: {decoded_path}")
        
        # Convert to absolute path
        absolute_path = Path(decoded_path).absolute()
        print(f"Absolute path: {absolute_path}")
        
        # Check if file exists
        if not absolute_path.exists():
            print(f"File not found: {absolute_path}")
            raise HTTPException(status_code=404, detail="File not found")
        
        # Get filename for download
        filename = absolute_path.name
        
        # Determine content type based on file extension
        if filename.lower().endswith('.pdf'):
            media_type = "application/pdf"
        elif filename.lower().endswith(('.txt', '.text')):
            media_type = "text/plain"
        else:
            media_type = "application/octet-stream"
        
        print(f"Serving file: {filename} with type: {media_type}")
        return FileResponse(
            str(absolute_path),
            media_type=media_type,
            filename=filename
        )
        
    except Exception as e:
        print(f"Error downloading file: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error downloading file: {str(e)}")