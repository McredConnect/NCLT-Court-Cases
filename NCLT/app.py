import os
import time
import random
import logging
import base64
import shutil
from urllib.parse import quote_plus
from typing import List
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, validator
import asyncio
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import json
import aiohttp
import aiofiles
import os
from urllib.parse import unquote, urlparse


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')


MAX_REQUESTS_PER_MINUTE = int(os.getenv("MAX_REQUESTS_PER_MINUTE", "180"))
REQUEST_WINDOW_SECONDS = int(os.getenv("REQUEST_WINDOW_SECONDS", "60"))
MAX_CASE_WORKERS = int(os.getenv("MAX_CASE_WORKERS", "9"))


class RateLimiter:
    def __init__(self, max_requests, window_seconds):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.request_times = []
        self.lock = asyncio.Lock()


    async def wait_if_needed(self):
        async with self.lock:
            now = time.monotonic()
            self.request_times = [t for t in self.request_times if now - t < self.window_seconds]
            if len(self.request_times) >= self.max_requests:
                sleep_time = self.window_seconds - (now - self.request_times[0])
                logging.info(f"Rate limit reached, sleeping for {sleep_time:.2f} seconds")
                await asyncio.sleep(sleep_time)
            self.request_times.append(time.monotonic())


rate_limiter = RateLimiter(MAX_REQUESTS_PER_MINUTE, REQUEST_WINDOW_SECONDS)


async def random_mouse_movements(page):
    try:
        for _ in range(random.randint(3, 5)):
            viewport = page.viewport_size or {"width": 1280, "height": 720}
            x = random.randint(0, viewport['width'])
            y = random.randint(0, viewport['height'])
            await page.mouse.move(x, y)
            await asyncio.sleep(random.uniform(0.3, 0.6))
    except Exception:
        pass


async def random_scroll(page):
    try:
        scroll_height = await page.evaluate("() => document.body.scrollHeight")
        for _ in range(random.randint(2, 5)):
            scroll_pos = random.randint(0, scroll_height)
            await page.evaluate(f"window.scrollTo(0, {scroll_pos});")
            await asyncio.sleep(random.uniform(0.3, 0.5))
    except Exception:
        pass


async def maybe_simulate_user_interaction(page, probability=0.3):
    if random.random() < probability:
        await random_mouse_movements(page)
        await random_scroll(page)


async def retry_with_backoff(func, max_retries=2):
    delay = 5
    for attempt in range(max_retries):
        try:
            return await func()
        except Exception as e:
            logging.warning(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                logging.info(f"Backing off for {delay} seconds before retry")
                await asyncio.sleep(delay)
                delay *= 2
            else:
                raise


class AdaptiveController:
    def __init__(self, base_delay=3, max_delay=7):
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.current_delay = base_delay
        self.fail_count = 0


    async def record_success(self):
        self.fail_count = 0
        self.current_delay = max(self.base_delay, self.current_delay - 1)


    async def record_failure(self):
        self.fail_count += 1
        self.current_delay = min(self.max_delay, self.current_delay * 1.5)


    async def wait(self):
        logging.info(f"Adaptive delay: sleeping for {self.current_delay:.2f} seconds")
        await asyncio.sleep(self.current_delay)


app = FastAPI(title="NCLT Court Cases Scraper 7 with Playwright Async")


# Base search URL and parameter maps
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


def encode_party_name(company_name: str) -> str:
    encoded_bytes = base64.b64encode(company_name.encode())
    encoded_str = encoded_bytes.decode()
    return quote_plus(encoded_str)


BASE_URL = "https://nclt.gov.in/"


async def download_pdf(session, url, folder_path):
    filename = os.path.join(folder_path, url.split('/')[-1].split('?')[0])
    try:
        async with session.get(url, timeout=15) as resp:
            resp.raise_for_status()
            f = await aiofiles.open(filename, mode='wb')
            await f.write(await resp.read())
            await f.close()
            logging.info(f"Downloaded PDF {filename}")
            return filename
    except Exception as e:
        logging.warning(f"Failed to download PDF {url}: {e}")
        return None


async def download_pdfs_async(pdf_links, folder_path, max_concurrent=7):
    os.makedirs(folder_path, exist_ok=True)
    semaphore = asyncio.Semaphore(max_concurrent)
    async with aiohttp.ClientSession() as session:

        async def sem_download(url):
            async with semaphore:
                return await download_pdf(session, url, folder_path)

        tasks = [sem_download(url) for url in pdf_links]
        downloaded_files = await asyncio.gather(*tasks)
    return [f for f in downloaded_files if f is not None]




ignored_pdf_names_set = {
    "CSR Report March, 2025a",
    "Notice dated 28.08.2025 All over NCLT Scrutiny Pendency Report"}

async def scrape_single_case(case_link, company_name, bench_name, year, status_name):
    full_link = BASE_URL + case_link
    output_folder = os.path.join(output_root, company_name, bench_name, str(year), status_name)

    excluded_pdf_urls = {
        "https://nclt.gov.in/sites/default/files/tender/circulars/publicnotices/Notice%20dated%2028.08.2025%20All%20over%20NCLT%20Scrutiny%20Pendency%20Report.pdf",
        "https://nclt.gov.in/sites/default/files/2025-05/CSR%20Report%20March%2C%202025a.pdf"
    }

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            await page.goto(full_link)

            # Extract all PDF links ending with .pdf (case-insensitive)
            pdf_links_all = await page.eval_on_selector_all(
                'a',
                """elements => elements
                    .map(a => a.href)
                    .filter(href => href && href.toLowerCase().endsWith('.pdf'))"""
            )

            # Filter out the specific excluded URLs, download all others
            pdf_links = [
                url for url in pdf_links_all
                if url not in excluded_pdf_urls
            ]

            # Download PDFs concurrently with limit of 5 at a time
            downloaded_pdfs = await download_pdfs_async(pdf_links, output_folder, max_concurrent=5)

            await browser.close()

            return {
                "bench": bench_name,
                "year": year,
                "status": status_name,
                "case_link": full_link,
                "pdfs": downloaded_pdfs,
            }
    except Exception as e:
        logging.error(f"Error in scrape_single_case {full_link}: {e}")
        return {
            "bench": bench_name,
            "year": year,
            "status": status_name,
            "case_link": full_link,
            "pdfs": [],
            "error": str(e),
        }




# ... all your imports and existing code remain unchanged ...

async def scrape_one_search(url: str, company_name: str, bench_name: str, year: int, status_name: str, adaptive_ctrl: AdaptiveController):
    try:
        await rate_limiter.wait_if_needed()
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            context = await browser.new_context(
                user_agent=("Mozilla/5.0 (Windows NT 10.0; x64) AppleWebKit/537.36 "
                            "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"),
                viewport={"width": 1280, "height": 720},
                bypass_csp=True,
                java_script_enabled=True
            )
            page = await context.new_page()
            await page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")

            await maybe_simulate_user_interaction(page, probability=0.5)
            await retry_with_backoff(lambda: page.goto(url, timeout=70000))
            await adaptive_ctrl.record_success()
            await adaptive_ctrl.wait()

            try:
                await page.wait_for_selector("table tbody tr", timeout=55000)
                rows = await page.query_selector_all("table tbody tr")
                table_text = (await page.locator("table").inner_text()).lower()
                if not rows or "please click here" in table_text:
                    logging.info(f"No case data found for bench {bench_name}, year {year}, status {status_name}")
                    await browser.close()
                    return []

                case_links = []
                for row in rows:
                    try:
                        link_elem = await row.query_selector('a')
                        if link_elem:
                            href = await link_elem.get_attribute('href')
                            if href:
                                case_links.append(href)
                    except Exception:
                        continue
                await browser.close()

                semaphore = asyncio.Semaphore(MAX_CASE_WORKERS)

                async def sem_scrape(link):
                    async with semaphore:
                        return await scrape_single_case(link, company_name, bench_name, year, status_name)

                case_scrape_tasks = [asyncio.create_task(sem_scrape(link)) for link in case_links]
                results = await asyncio.gather(*case_scrape_tasks)

                return results

            except PlaywrightTimeoutError:
                logging.error(f"Timeout error waiting for table rows at {url}")
                await browser.close()
                return []
    except Exception as e:
        await adaptive_ctrl.record_failure()
        logging.error(f"Failed to load URL {url}: {e}")
        return []



# Fixed concurrent streaming scrape implementation
async def scrape_cases_stream_concurrent(req: ScrapeRequest):
    encoded_party_name = encode_party_name(req.company_name)
    adaptive_ctrl = AdaptiveController()
    semaphore = asyncio.Semaphore(MAX_CASE_WORKERS)

    result_queue = asyncio.Queue()

    async def scrape_and_queue(bench_name, year, status_name):
        bench_encoded = benches[bench_name]
        year_encoded = base64.b64encode(str(year).encode()).decode()
        status_encoded = case_status_map[status_name]
        url = (
            f"{base_search_url}?bench={bench_encoded}&party_type={party_type_encoded}"
            f"&party_name={encoded_party_name}&case_year={year_encoded}&case_status={status_encoded}"
        )
        async with semaphore:
            results = await scrape_one_search(url, req.company_name, bench_name, year, status_name, adaptive_ctrl)
        await result_queue.put({
            "bench": bench_name,
            "year": year,
            "status": status_name,
            "cases": results,
            "message": f"Data found: {len(results)} cases" if results else "No data found"
        })

    tasks = []
    for bench_name in req.benches:
        for year in req.years:
            for status_name in req.statuses:
                if status_name in case_status_map:
                    tasks.append(scrape_and_queue(bench_name, year, status_name))

    scraping_tasks = asyncio.gather(*tasks)

    finished_tasks = 0
    total_tasks = len(tasks)
    while finished_tasks < total_tasks:
        result = await result_queue.get()
        finished_tasks += 1
        yield f"data: {json.dumps(result)}\n\n"

    await scraping_tasks


@app.post("/stream-scrape")
async def stream_scrape(req: ScrapeRequest):
    return StreamingResponse(scrape_cases_stream_concurrent(req), media_type="text/event-stream")



# Original scrape implementation
async def scrape_cases_impl(req: ScrapeRequest):
    os.makedirs(output_root, exist_ok=True)
    encoded_party_name = encode_party_name(req.company_name)
    scraped_data = []
    adaptive_ctrl = AdaptiveController()
    semaphore = asyncio.Semaphore(MAX_CASE_WORKERS)

    async def scrape_task(bench_name, year, status_name):
        bench_encoded = benches[bench_name]
        year_encoded = base64.b64encode(str(year).encode()).decode()
        status_encoded = case_status_map[status_name]
        url = (
            f"{base_search_url}?bench={bench_encoded}&party_type={party_type_encoded}"
            f"&party_name={encoded_party_name}&case_year={year_encoded}&case_status={status_encoded}"
        )
        async with semaphore:
            results = await scrape_one_search(url, req.company_name, bench_name, year, status_name, adaptive_ctrl)
        return results

    tasks = []
    for bench_name in req.benches:
        for year in req.years:
            for status_name in req.statuses:
                if status_name in case_status_map:
                    tasks.append(scrape_task(bench_name, year, status_name))

    results = await asyncio.gather(*tasks)
    for sublist in results:
        scraped_data.extend(sublist)

    logging.info("Scraping completed successfully")
    return {"message": "Scraping completed successfully", "results": scraped_data}


@app.post("/scrape")
async def scrape_cases(req: ScrapeRequest):
    return await scrape_cases_impl(req)
