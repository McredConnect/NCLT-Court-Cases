import os
import time
import random
import logging
import base64
import json
from urllib.parse import quote_plus
from typing import List, Optional
from fastapi import FastAPI, BackgroundTasks, Query, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, validator
import asyncio
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import aiohttp
import aiofiles
import aiofiles.os
from urllib.parse import unquote, urlparse
from fastapi.middleware.cors import CORSMiddleware 

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# Optimized Configuration
MAX_REQUESTS_PER_MINUTE = int(os.getenv("MAX_REQUESTS_PER_MINUTE", "800"))  
REQUEST_WINDOW_SECONDS = int(os.getenv("REQUEST_WINDOW_SECONDS", "60"))
MAX_CASE_WORKERS = int(os.getenv("MAX_CASE_WORKERS", "50")) 
PDF_DOWNLOAD_WORKERS = int(os.getenv("PDF_DOWNLOAD_WORKERS", "30"))
MAX_BROWSERS = int(os.getenv("MAX_BROWSERS", "5"))
MAX_CONTEXTS_PER_BROWSER = int(os.getenv("MAX_CONTEXTS_PER_BROWSER", "60"))

# Performance monitoring decorator
def monitor_performance(func_name):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                duration = time.time() - start_time
                logging.info(f"{func_name} completed in {duration:.2f}s")
                return result
            except Exception as e:
                duration = time.time() - start_time
                logging.error(f"{func_name} failed after {duration:.2f}s: {e}")
                raise
        return wrapper
    return decorator

# Optimized Rate Limiter with burst capability
class OptimizedRateLimiter:
    def __init__(self, max_requests=800, window_seconds=60, burst_limit=20):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.burst_limit = burst_limit
        self.request_times = []
        self.lock = asyncio.Lock()

    async def wait_if_needed(self):
        async with self.lock:
            now = time.monotonic()
            # Clean old requests
            self.request_times = [t for t in self.request_times if now - t < self.window_seconds]

            # Allow burst processing
            if len(self.request_times) < self.burst_limit:
                self.request_times.append(now)
                return

            # Only wait if we exceed the limit
            if len(self.request_times) >= self.max_requests:
                sleep_time = self.window_seconds - (now - self.request_times[0])
                if sleep_time > 0:
                    await asyncio.sleep(min(sleep_time, 1.0))  

            self.request_times.append(now)

# Browser Pool for context reuse
class BrowserPool:
    def __init__(self, max_browsers=5, max_contexts_per_browser=7):
        self.max_browsers = max_browsers
        self.max_contexts_per_browser = max_contexts_per_browser
        self.browsers = []
        self.available_contexts = asyncio.Queue()
        self.context_usage = {}
        self._initialized = False
        self._playwright = None

    async def initialize(self):
        if self._initialized:
            return

        self._playwright = await async_playwright().start()

        for _ in range(self.max_browsers):
            browser = await self._playwright.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox", 
                    "--disable-dev-shm-usage", 
                    "--disable-images",
                    "--disable-javascript",  # Disable JS for faster loading
                    "--disable-plugins",
                    "--disable-extensions"
                ]
            )
            self.browsers.append(browser)

            for _ in range(self.max_contexts_per_browser):
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
                    viewport={"width": 1280, "height": 720}
                )
                await self._setup_context(context)
                await self.available_contexts.put(context)
                self.context_usage[context] = 0

        self._initialized = True
        logging.info(f"Browser pool initialized with {self.max_browsers} browsers")

    async def _setup_context(self, context):
        # Block unnecessary resources 
        await context.route("**/*.{png,jpg,jpeg,gif,svg,css,woff,woff2,ico}", lambda route: route.abort())
        await context.route("**/ads/**", lambda route: route.abort())
        await context.route("**/analytics/**", lambda route: route.abort())
        await context.route("**/tracking/**", lambda route: route.abort())

    async def get_context(self):
        if not self._initialized:
            await self.initialize()
        return await self.available_contexts.get()

    async def return_context(self, context):
        self.context_usage[context] += 1

        if self.context_usage[context] > 20:
            try:
                await context.close()
                browser = self.browsers[0]  # Use first available browser
                new_context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
                    viewport={"width": 1280, "height": 720}
                )
                await self._setup_context(new_context)
                context = new_context
                self.context_usage[context] = 0
                logging.info("Context refreshed")
            except Exception as e:
                logging.warning(f"Error refreshing context: {e}")

        await self.available_contexts.put(context)

    async def close(self):
        for browser in self.browsers:
            await browser.close()
        if self._playwright:
            await self._playwright.stop()

# Global instances
rate_limiter = OptimizedRateLimiter(MAX_REQUESTS_PER_MINUTE, REQUEST_WINDOW_SECONDS)
browser_pool = BrowserPool(MAX_BROWSERS, MAX_CONTEXTS_PER_BROWSER)

# Configuration maps
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

# Optimized PDF download with async file operations
async def download_pdf_optimized(session, url, folder_path):
    filename = os.path.join(folder_path, url.split('/')[-1].split('?')[0])
    try:
        async with session.get(url, timeout=20) as resp:  # Shorter timeout
            resp.raise_for_status()

            # Use aiofiles for non-blocking file operations
            async with aiofiles.open(filename, mode='wb') as f:
                async for chunk in resp.content.iter_chunked(8192):  # Stream in chunks
                    await f.write(chunk)

            logging.info(f"Downloaded PDF {filename}")
            return filename
    except Exception as e:
        logging.warning(f"Failed to download PDF {url}: {e}")
        return None

async def ensure_directory_exists(path):
    try:
        await aiofiles.os.makedirs(path, exist_ok=True)
    except Exception as e:
        logging.warning(f"Failed to create directory {path}: {e}")

async def download_pdfs_async_optimized(pdf_links, folder_path, max_concurrent=PDF_DOWNLOAD_WORKERS):
    await ensure_directory_exists(folder_path)
    semaphore = asyncio.Semaphore(max_concurrent)

    connector = aiohttp.TCPConnector(limit=50, limit_per_host=10)
    timeout = aiohttp.ClientTimeout(total=30, connect=10)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        async def sem_download(url):
            async with semaphore:
                return await download_pdf_optimized(session, url, folder_path)

        tasks = [sem_download(url) for url in pdf_links]
        downloaded_files = await asyncio.gather(*tasks, return_exceptions=True)

    return [f for f in downloaded_files if isinstance(f, str) and f is not None]

# Excluded PDFs - same as original
excluded_pdf_urls = {
    "https://nclt.gov.in/sites/default/files/tender/circulars/publicnotices/Notice%20dated%2028.08.2025%20All%20over%20NCLT%20Scrutiny%20Pendency%20Report.pdf",
    "https://nclt.gov.in/sites/default/files/2025-05/CSR%20Report%20March%2C%202025a.pdf"
}

@monitor_performance("scrape_single_case")
async def scrape_single_case_optimized(case_link, company_name, bench_name, year, status_name):
    full_link = BASE_URL + case_link
    output_folder = os.path.join(output_root, company_name, bench_name, str(year), status_name)

    context = await browser_pool.get_context()
    try:
        page = await context.new_page()
        await page.goto(full_link, timeout=50000, wait_until="domcontentloaded")

        # Extract PDF links faster
        pdf_links_all = await page.eval_on_selector_all(
            'a',
            """elements => elements
                .map(a => a.href)
                .filter(href => href && href.toLowerCase().endsWith('.pdf'))"""
        )

        # Filter out excluded URLs
        pdf_links = [url for url in pdf_links_all if url not in excluded_pdf_urls]

        # Download PDFs with optimized async operations
        downloaded_pdfs = await download_pdfs_async_optimized(pdf_links, output_folder, max_concurrent=5)

        await page.close()

        return {
            "company_name": company_name,
            "bench": bench_name,
            "year": year,
            "status": status_name,
            "case_link": full_link,
            "pdfs": downloaded_pdfs,
        }
    except Exception as e:
        logging.error(f"Error in scrape_single_case_optimized {full_link}: {e}")
        return {
            "company_name": company_name,
            "bench": bench_name,
            "year": year,
            "status": status_name,
            "case_link": full_link,
            "pdfs": [],
            "error": str(e),
        }
    finally:
        await browser_pool.return_context(context)

@monitor_performance("scrape_one_search")
async def scrape_one_search_optimized(url: str, company_name: str, bench_name: str, year: int, status_name: str):
    try:
        await rate_limiter.wait_if_needed()

        context = await browser_pool.get_context()
        try:
            page = await context.new_page()
            await page.goto(url, timeout=50000, wait_until="domcontentloaded")

            # Faster table detection
            try:
                await page.wait_for_selector("table tbody tr", timeout=50000)
                rows = await page.query_selector_all("table tbody tr")
                table_text = (await page.locator("table").inner_text()).lower()

                if not rows or "please click here" in table_text:
                    logging.info(f"No case data found for bench {bench_name}, year {year}, status {status_name}")
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

                await page.close()

                # Process cases with controlled concurrency
                semaphore = asyncio.Semaphore(MAX_CASE_WORKERS)

                async def sem_scrape(link):
                    async with semaphore:
                        return await scrape_single_case_optimized(link, company_name, bench_name, year, status_name)

                results = await asyncio.gather(*[sem_scrape(link) for link in case_links])
                return results

            except PlaywrightTimeoutError:
                logging.error(f"Timeout error waiting for table rows at {url}")
                return []
        finally:
            await browser_pool.return_context(context)

    except Exception as e:
        logging.error(f"Failed to load URL {url}: {e}")
        return []



async def scrape_cases_stream_optimized(req: ScrapeRequest):
    encoded_party_name = encode_party_name(req.company_name)
    semaphore = asyncio.Semaphore(MAX_CASE_WORKERS)
    result_queue = asyncio.Queue()
    all_results = []

    async def scrape_and_queue(bench_name, year, status_name):
        bench_encoded = benches[bench_name]
        year_encoded = base64.b64encode(str(year).encode()).decode()
        status_encoded = case_status_map[status_name]
        url = (
            f"{base_search_url}?bench={bench_encoded}&party_type={party_type_encoded}"
            f"&party_name={encoded_party_name}&case_year={year_encoded}&case_status={status_encoded}"
        )
        async with semaphore:
            results = await scrape_one_search_optimized(url, req.company_name, bench_name, year, status_name)
        await result_queue.put({
            "company_name": req.company_name,
            "bench": bench_name,
            "year": year,
            "status": status_name,
            "cases": results,
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
        all_results.append(result)
        yield f"data: {json.dumps(result)}\n\n"

    await scraping_tasks

    #save i json format
    output_folder = os.path.join(output_root, req.company_name)
    os.makedirs(output_folder, exist_ok=True)
    async with aiofiles.open(os.path.join(output_folder, "result.json"), "w") as f:
        await f.write(json.dumps({
            "message": "Scraping completed successfully",
            "results": all_results
        }, indent=2))

@monitor_performance("scrape_cases_impl")
async def scrape_cases_impl_optimized(req: ScrapeRequest):
    await ensure_directory_exists(output_root)
    encoded_party_name = encode_party_name(req.company_name)
    scraped_data = []
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
            results = await scrape_one_search_optimized(url, req.company_name, bench_name, year, status_name)
        return results

    tasks = []
    for bench_name in req.benches:
        for year in req.years:
            for status_name in req.statuses:
                if status_name in case_status_map:
                    tasks.append(scrape_task(bench_name, year, status_name))

    results = await asyncio.gather(*tasks)
    for sublist in results:
        # Add company_name into each result to include it in output
        scraped_data.extend([
            {
                "company_name": req.company_name,
                **item
            }
            for item in sublist
        ])

    logging.info("Optimized scraping completed successfully")
    return {"message": "scraping completed successfully", "results": scraped_data}

app = FastAPI()

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

@app.post("/stream-scrape")
async def stream_scrape_optimized_endpoint(req: ScrapeRequest):
    return StreamingResponse(scrape_cases_stream_optimized(req), media_type="text/event-stream")

@app.post("/scrape")
async def scrape_cases_optimized_endpoint(req: ScrapeRequest):
    return await scrape_cases_impl_optimized(req)

@app.get("/result")
async def get_scrape_result(
    company_name: str = Query(...)
):
    json_path = os.path.join(output_root, company_name, "result.json")
    if not os.path.exists(json_path):
        return {
            "message": "No results found for this company",
            "results": []
        }
    
    try:
        async with aiofiles.open(json_path, "r") as f:
            content = await f.read()
            result_data = json.loads(content)
            return result_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read cached results: {e}")

@app.get("/health")
async def health_check():
    return {"status": "healthy", "browser_pool_initialized": browser_pool._initialized}

@app.on_event("startup")
async def startup_event():
    """Initialize browser pool on startup"""
    await browser_pool.initialize()

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up browser pool on shutdown"""
    await browser_pool.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8888)
