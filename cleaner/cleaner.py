"""
Data Cleaning & Normalisation Pipeline
----------------------------------------
Decisions documented inline. Reads raw JSON, cleans, and stores in SQLite.
"""

import os
import re
import json
import sqlite3
import logging
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)

DB_PATH  = os.getenv("DB_PATH",  "data/jobs.db")
RAW_DIR  = os.getenv("OUTPUT_DIR", "data/raw")

# ---------------------------------------------------------------------------
# Salary parsing
# ---------------------------------------------------------------------------
# Decision: salary strings on Indeed India are wildly inconsistent —
# "₹4,00,000 - ₹6,00,000 a year", "₹25,000 a month", "Not disclosed".
# We normalise everything to annual INR integers with a low/high range.

SALARY_MULTIPLIERS = {
    "hour": 2080 * 83,    # ~2080 work-hours/year; illustrative USD→INR fallback
    "day":  250 * 83,
    "week": 52,
    "month": 12,
    "year":  1,
    "annum": 1,
}

def _parse_number(s: str) -> Optional[float]:
    s = s.replace(",", "").replace("₹", "").replace("$", "").strip()
    m = re.search(r"[\d]+(?:\.\d+)?", s)
    return float(m.group()) if m else None


def parse_salary(raw: Optional[str]) -> dict:
    """
    Returns {"salary_min": int|None, "salary_max": int|None, "salary_currency": str}.
    """
    result = {"salary_min": None, "salary_max": None, "salary_currency": "INR"}
    if not raw:
        return result

    raw_lower = raw.lower()

    # Detect currency
    if "₹" in raw or "inr" in raw_lower:
        result["salary_currency"] = "INR"
    elif "$" in raw or "usd" in raw_lower:
        result["salary_currency"] = "USD"

    # Detect period multiplier
    multiplier = 1
    for key, mult in SALARY_MULTIPLIERS.items():
        if key in raw_lower:
            multiplier = mult
            break

    # Extract numeric range
    numbers = re.findall(r"[\d,]+(?:\.\d+)?", raw.replace("₹","").replace("$",""))
    nums = [_parse_number(n) for n in numbers if _parse_number(n)]

    if len(nums) >= 2:
        result["salary_min"] = int(nums[0] * multiplier)
        result["salary_max"] = int(nums[1] * multiplier)
    elif len(nums) == 1:
        result["salary_min"] = result["salary_max"] = int(nums[0] * multiplier)

    return result


# ---------------------------------------------------------------------------
# Date normalisation
# ---------------------------------------------------------------------------
# Decision: Indeed uses relative dates ("3 days ago", "Just posted", "30+ days ago").
# We convert to an approximate ISO date.

def parse_posted_date(raw: Optional[str], scraped_at: str) -> Optional[str]:
    if not raw:
        return None
    raw = raw.lower().strip()
    try:
        base = datetime.fromisoformat(scraped_at.replace("Z", "+00:00"))
    except Exception:
        base = datetime.now(timezone.utc)

    if "just posted" in raw or "today" in raw:
        days_ago = 0
    elif "hour" in raw:
        m = re.search(r"(\d+)", raw)
        days_ago = 0 if m and int(m.group(1)) < 24 else 1
    elif "day" in raw:
        m = re.search(r"(\d+)", raw)
        days_ago = int(m.group(1)) if m else 1
        if "30+" in raw:
            days_ago = 30
    elif "week" in raw:
        m = re.search(r"(\d+)", raw)
        days_ago = (int(m.group(1)) if m else 1) * 7
    elif "month" in raw:
        m = re.search(r"(\d+)", raw)
        days_ago = (int(m.group(1)) if m else 1) * 30
    else:
        return None

    from datetime import timedelta
    return (base - timedelta(days=days_ago)).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Location normalisation
# ---------------------------------------------------------------------------
# Decision: locations arrive as "Bangalore, Karnataka", "Bengaluru",
# "Remote in Hyderabad", etc. We extract city and remote flag.

CITY_ALIASES = {
    "bengaluru": "Bangalore",
    "bangalore": "Bangalore",
    "bombay":    "Mumbai",
    "delhi":     "Delhi",
    "ncr":       "Delhi",
    "new delhi": "Delhi",
    "gurugram":  "Gurgaon",
    "gurgaon":   "Gurgaon",
}

def parse_location(raw: Optional[str]) -> dict:
    result = {"city": None, "state": None, "is_remote": False}
    if not raw:
        return result

    raw_lower = raw.lower()
    result["is_remote"] = "remote" in raw_lower or "work from home" in raw_lower or "wfh" in raw_lower

    # Strip "Remote in", "Hybrid in", etc.
    clean = re.sub(r"(remote|hybrid|work from home|wfh)\s*(in)?\s*", "", raw, flags=re.IGNORECASE)
    parts = [p.strip() for p in clean.split(",") if p.strip()]

    if parts:
        city = parts[0].strip()
        city_key = city.lower()
        result["city"] = CITY_ALIASES.get(city_key, city.title())
    if len(parts) >= 2:
        result["state"] = parts[1].strip().title()

    return result


# ---------------------------------------------------------------------------
# Title normalisation
# ---------------------------------------------------------------------------
# Decision: job titles are free-text and messy. We extract a normalised
# seniority level and a clean role bucket for grouping.

SENIORITY_PATTERNS = [
    (r"\b(senior|sr\.?|lead|staff|principal|l[4-7])\b", "Senior"),
    (r"\b(junior|jr\.?|associate|entry[- ]?level|fresher|l[12])\b", "Junior"),
    (r"\b(manager|mgr|head|director|vp|vice president)\b", "Manager"),
    (r"\b(intern|internship|trainee)\b", "Intern"),
]

ROLE_BUCKETS = {
    "data engineer":   r"\bdata\s+engineer",
    "data scientist":  r"\bdata\s+scien",
    "ml engineer":     r"\b(ml|machine\s+learning)\s+engineer",
    "software engineer": r"\b(software|sde|swe)\s*(engineer|developer|dev)\b",
    "devops":          r"\b(devops|sre|platform\s+engineer|cloud\s+engineer)\b",
    "product manager": r"\b(product\s+manager|pm\b|product\s+owner)\b",
    "data analyst":    r"\bdata\s+anal",
    "backend":         r"\bback[\s-]?end",
    "frontend":        r"\bfront[\s-]?end",
    "fullstack":       r"\bfull[\s-]?stack",
    "android":         r"\bandroid\b",
    "ios":             r"\b(ios|swift)\b",
    "qa":              r"\b(qa|quality\s+assur|test\s+engineer)\b",
}

def parse_title(raw: Optional[str]) -> dict:
    result = {"title_clean": raw, "seniority": "Mid", "role_bucket": "Other"}
    if not raw:
        return result

    lower = raw.lower()

    for pattern, level in SENIORITY_PATTERNS:
        if re.search(pattern, lower, re.IGNORECASE):
            result["seniority"] = level
            break

    for bucket, pattern in ROLE_BUCKETS.items():
        if re.search(pattern, lower, re.IGNORECASE):
            result["role_bucket"] = bucket
            break

    # Clean title: remove excessive punctuation / parentheses
    clean = re.sub(r"\s*[\(\[].*?[\)\]]\s*", " ", raw)
    clean = re.sub(r"\s+", " ", clean).strip()
    result["title_clean"] = clean

    return result


# ---------------------------------------------------------------------------
# Main cleaning function
# ---------------------------------------------------------------------------

def clean_job(raw: dict) -> dict:
    salary   = parse_salary(raw.get("salary_raw"))
    location = parse_location(raw.get("location"))
    title    = parse_title(raw.get("title"))
    posted   = parse_posted_date(raw.get("posted_raw"), raw.get("scraped_at", ""))

    return {
        "job_id":          raw.get("job_id"),
        "title_raw":       raw.get("title"),
        "title_clean":     title["title_clean"],
        "seniority":       title["seniority"],
        "role_bucket":     title["role_bucket"],
        "company":         (raw.get("company") or "").strip().title() or None,
        "city":            location["city"],
        "state":           location["state"],
        "is_remote":       1 if location["is_remote"] else 0,
        "location_raw":    raw.get("location"),
        "salary_raw":      raw.get("salary_raw"),
        "salary_min":      salary["salary_min"],
        "salary_max":      salary["salary_max"],
        "salary_currency": salary["salary_currency"],
        "salary_midpoint": (
            (salary["salary_min"] + salary["salary_max"]) // 2
            if salary["salary_min"] and salary["salary_max"] else None
        ),
        "job_type":        raw.get("job_type"),
        "posted_date":     posted,
        "summary":         raw.get("summary"),
        "job_url":         raw.get("job_url"),
        "search_query":    raw.get("search_query"),
        "search_location": raw.get("search_location"),
        "scraped_at":      raw.get("scraped_at"),
        "cleaned_at":      datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id          TEXT PRIMARY KEY,
    title_raw       TEXT,
    title_clean     TEXT,
    seniority       TEXT,
    role_bucket     TEXT,
    company         TEXT,
    city            TEXT,
    state           TEXT,
    is_remote       INTEGER DEFAULT 0,
    location_raw    TEXT,
    salary_raw      TEXT,
    salary_min      INTEGER,
    salary_max      INTEGER,
    salary_currency TEXT DEFAULT 'INR',
    salary_midpoint INTEGER,
    job_type        TEXT,
    posted_date     TEXT,
    summary         TEXT,
    job_url         TEXT,
    search_query    TEXT,
    search_location TEXT,
    scraped_at      TEXT,
    cleaned_at      TEXT
);

CREATE INDEX IF NOT EXISTS idx_role    ON jobs(role_bucket);
CREATE INDEX IF NOT EXISTS idx_company ON jobs(company);
CREATE INDEX IF NOT EXISTS idx_city    ON jobs(city);
CREATE INDEX IF NOT EXISTS idx_posted  ON jobs(posted_date);
CREATE INDEX IF NOT EXISTS idx_scraped ON jobs(scraped_at);
"""


def get_db(path: str = DB_PATH) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


def upsert_jobs(conn: sqlite3.Connection, jobs: list[dict]) -> int:
    """Insert or replace jobs. Returns count of new/updated rows."""
    sql = """
    INSERT OR REPLACE INTO jobs
        (job_id, title_raw, title_clean, seniority, role_bucket,
         company, city, state, is_remote, location_raw,
         salary_raw, salary_min, salary_max, salary_currency, salary_midpoint,
         job_type, posted_date, summary, job_url,
         search_query, search_location, scraped_at, cleaned_at)
    VALUES
        (:job_id, :title_raw, :title_clean, :seniority, :role_bucket,
         :company, :city, :state, :is_remote, :location_raw,
         :salary_raw, :salary_min, :salary_max, :salary_currency, :salary_midpoint,
         :job_type, :posted_date, :summary, :job_url,
         :search_query, :search_location, :scraped_at, :cleaned_at)
    """
    conn.executemany(sql, jobs)
    conn.commit()
    return len(jobs)


# ---------------------------------------------------------------------------
# Pipeline entry point
# ---------------------------------------------------------------------------

def run_cleaner(raw_json_path: str, db_path: str = DB_PATH) -> int:
    log.info("Loading raw data from %s", raw_json_path)
    with open(raw_json_path, encoding="utf-8") as f:
        raw_jobs = json.load(f)

    log.info("Cleaning %d raw records…", len(raw_jobs))
    cleaned = []
    for raw in raw_jobs:
        try:
            cleaned.append(clean_job(raw))
        except Exception as e:
            log.warning("Skipping job %s: %s", raw.get("job_id"), e)

    conn = get_db(db_path)
    count = upsert_jobs(conn, cleaned)
    conn.close()
    log.info("Stored %d jobs in %s", count, db_path)
    return count


if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else sorted(
        [f for f in os.listdir(RAW_DIR) if f.endswith(".json")]
    )[-1]
    run_cleaner(os.path.join(RAW_DIR, path))
