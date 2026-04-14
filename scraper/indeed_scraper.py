"""
Indeed Job Market Intelligence Scraper
---------------------------------------
Scrapes job listings from Indeed for B2B competitive intelligence.
Handles pagination, missing fields, and failures gracefully.
Exports raw data as structured JSON/CSV.
"""

import os
import re
import json
import csv
import time
import random
import hashlib
import logging
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL = "https://in.indeed.com/jobs"
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "data/raw")
MAX_PAGES = int(os.getenv("MAX_PAGES", "5"))        # 10 results/page on Indeed India
DELAY_MIN = float(os.getenv("DELAY_MIN", "2.0"))    # seconds between requests
DELAY_MAX = float(os.getenv("DELAY_MAX", "5.0"))

DEFAULT_SEARCHES = [
    {"what": "data engineer",        "where": "Bangalore"},
    {"what": "product manager",      "where": "Hyderabad"},
    {"what": "software engineer",    "where": "Mumbai"},
    {"what": "devops engineer",      "where": "Pune"},
    {"what": "data scientist",       "where": "Delhi"},
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-IN,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

def fetch_page(url: str, retries: int = 3) -> Optional[str]:
    """Fetch URL with retry logic and exponential backoff."""
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=15) as resp:
                if resp.status == 200:
                    return resp.read().decode("utf-8", errors="replace")
                log.warning("HTTP %s for %s", resp.status, url)
        except urllib.error.HTTPError as e:
            log.warning("HTTPError %s on attempt %d for %s", e.code, attempt, url)
            if e.code == 429:
                time.sleep(30)   # rate-limited — back off hard
        except urllib.error.URLError as e:
            log.warning("URLError on attempt %d: %s", attempt, e.reason)
        except Exception as e:
            log.warning("Unexpected error on attempt %d: %s", attempt, e)

        backoff = (2 ** attempt) + random.uniform(1, 3)
        log.info("Retrying in %.1fs…", backoff)
        time.sleep(backoff)

    log.error("All retries exhausted for %s", url)
    return None


# ---------------------------------------------------------------------------
# Parsing helpers  (regex-based since we can't install bs4 in sandbox)
# ---------------------------------------------------------------------------

def _extract(pattern: str, text: str, group: int = 1, default: str = "") -> str:
    m = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
    return m.group(group).strip() if m else default


def _clean_html(text: str) -> str:
    """Strip HTML tags and normalise whitespace."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"&nbsp;|&#160;", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def parse_job_card(card_html: str, search_query: str, search_location: str) -> dict:
    """
    Extract fields from a single job card HTML block.
    Returns a dict; missing fields default to None / empty string.
    """

    # Job ID — Indeed embeds it as data-jk="..."
    job_id = _extract(r'data-jk="([^"]+)"', card_html) or _extract(r'id="job_([^"]+)"', card_html)
    if not job_id:
        job_id = hashlib.md5(card_html[:200].encode()).hexdigest()[:12]

    title        = _clean_html(_extract(r'<span[^>]*jobTitle[^>]*>(.*?)</span>', card_html))
    company      = _clean_html(_extract(r'<span[^>]*companyName[^>]*>(.*?)</span>', card_html))
    location     = _clean_html(_extract(r'<div[^>]*companyLocation[^>]*>(.*?)</div>', card_html))
    salary       = _clean_html(_extract(r'<div[^>]*salary-snippet[^>]*>(.*?)</div>', card_html))
    job_type     = _clean_html(_extract(r'<span[^>]*metadata[^>]*>(.*?)</span>', card_html))
    posted       = _clean_html(_extract(r'<span[^>]*date[^>]*>(.*?)</span>', card_html))
    summary      = _clean_html(_extract(r'<div[^>]*job-snippet[^>]*>(.*?)</ul>', card_html))
    job_url_path = _extract(r'href="(/rc/clk[^"]+)"', card_html)
    job_url      = f"https://in.indeed.com{job_url_path}" if job_url_path else ""

    return {
        "job_id":          job_id,
        "title":           title or None,
        "company":         company or None,
        "location":        location or None,
        "salary_raw":      salary or None,
        "job_type":        job_type or None,
        "posted_raw":      posted or None,
        "summary":         summary[:500] if summary else None,
        "job_url":         job_url or None,
        "search_query":    search_query,
        "search_location": search_location,
        "scraped_at":      datetime.now(timezone.utc).isoformat(),
    }


def parse_results_page(html: str, search_query: str, search_location: str) -> list[dict]:
    """Extract all job cards from a search results page."""
    # Indeed wraps each job in a <div class="job_seen_beacon"> or similar
    card_pattern = r'<div[^>]+class="[^"]*job_seen_beacon[^"]*"[^>]*>(.*?)(?=<div[^>]+class="[^"]*job_seen_beacon|</ul>)'
    cards = re.findall(card_pattern, html, re.DOTALL)

    if not cards:
        # Fallback: try mosaic-provider-jobcards
        card_pattern = r'<li[^>]+class="[^"]*css-[^"]*[^>]*>(.*?)</li>'
        cards = re.findall(card_pattern, html, re.DOTALL)

    jobs = []
    for card in cards:
        if 'data-jk' in card or 'jobTitle' in card:
            job = parse_job_card(card, search_query, search_location)
            if job["title"]:   # skip ghost cards
                jobs.append(job)

    log.info("  Parsed %d job cards", len(jobs))
    return jobs


def get_next_page_start(html: str, current_start: int) -> Optional[int]:
    """Find the next pagination offset. Returns None if last page."""
    # Indeed uses ?start=10, ?start=20 …
    next_pattern = rf'href="[^"]*start={current_start + 10}[^"]*"'
    if re.search(next_pattern, html):
        return current_start + 10
    return None


# ---------------------------------------------------------------------------
# Main scrape loop
# ---------------------------------------------------------------------------

def scrape_search(what: str, where: str) -> list[dict]:
    """Scrape multiple pages for one search term + location."""
    all_jobs = []
    start = 0

    for page in range(MAX_PAGES):
        params = urllib.parse.urlencode({"q": what, "l": where, "start": start})
        url = f"{BASE_URL}?{params}"
        log.info("Fetching page %d — %s", page + 1, url)

        html = fetch_page(url)
        if not html:
            log.warning("Skipping page %d — no content returned", page + 1)
            break

        jobs = parse_results_page(html, what, where)
        if not jobs:
            log.info("No jobs found on page %d — stopping pagination", page + 1)
            break

        all_jobs.extend(jobs)

        next_start = get_next_page_start(html, start)
        if next_start is None:
            log.info("Reached last page for '%s' in '%s'", what, where)
            break

        start = next_start
        delay = random.uniform(DELAY_MIN, DELAY_MAX)
        log.info("Waiting %.1fs before next page…", delay)
        time.sleep(delay)

    return all_jobs


def run_scraper(searches: list[dict] = None) -> str:
    """
    Run all searches, deduplicate, and export to JSON + CSV.
    Returns path to the JSON output file.
    """
    if searches is None:
        searches = DEFAULT_SEARCHES

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = os.path.join(OUTPUT_DIR, f"jobs_{timestamp}.json")
    csv_path  = os.path.join(OUTPUT_DIR, f"jobs_{timestamp}.csv")

    all_jobs: list[dict] = []
    seen_ids: set[str] = set()

    for search in searches:
        log.info("=== Scraping: %s in %s ===", search["what"], search["where"])
        try:
            jobs = scrape_search(search["what"], search["where"])
            for job in jobs:
                if job["job_id"] not in seen_ids:
                    seen_ids.add(job["job_id"])
                    all_jobs.append(job)
            log.info("Total unique so far: %d", len(all_jobs))
        except Exception as e:
            log.error("Failed search '%s': %s", search["what"], e)

        time.sleep(random.uniform(DELAY_MIN * 2, DELAY_MAX * 2))

    # Export JSON
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(all_jobs, f, ensure_ascii=False, indent=2)
    log.info("Saved %d jobs → %s", len(all_jobs), json_path)

    # Export CSV
    if all_jobs:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_jobs[0].keys())
            writer.writeheader()
            writer.writerows(all_jobs)
        log.info("Saved CSV → %s", csv_path)

    return json_path


if __name__ == "__main__":
    run_scraper()
