# JobIntel — B2B Hiring Intelligence Pipeline

> Automatically scrape, clean, store, and visualise job market data from Indeed India.  
> Built for talent acquisition teams, HR consultancies, and competitive intelligence.

---

## Live Demo

🚀 **Frontend Dashboard**: [https://agent-69dde8ccdfa3f645da--frabjous-sprite-5fa389.netlify.app/](https://agent-69dde8ccdfa3f645da--frabjous-sprite-5fa389.netlify.app/)

🔧 **Backend API**: [https://indeedscrapweb.onrender.com/](https://indeedscrapweb.onrender.com/)

---

## The Problem

Talent acquisition teams and B2B SaaS companies need to know:

- **Which companies are aggressively hiring** (and therefore growing, and therefore good sales targets)?
- **What skills are actually in demand** for a given role — not last year's survey, but right now?
- **What do competitors pay?** Salary benchmarks from the real market, not self-reported surveys.
- **Which roles are trending up or down** in a specific city?

This data exists publicly on job boards, but scraping it manually, cleaning it, and making it usable is a multi-hour task done repeatedly. JobIntel automates the full pipeline end-to-end, refreshes every 6 hours, and serves a live dashboard.

---

## Architecture

```
Indeed India (in.indeed.com)
        │
        ▼
┌───────────────┐     raw JSON/CSV
│  Scraper      │ ─────────────────▶  data/raw/jobs_YYYYMMDD_HHMMSS.json
│  (Phase 1)    │
└───────────────┘
        │
        ▼
┌───────────────┐     cleaned rows
│  Cleaner      │ ─────────────────▶  data/jobs.db  (SQLite)
│  (Phase 2)    │
└───────────────┘
        │
        ├──▶ FastAPI REST API  (port 8000)
        │         │
        │         └──▶ /jobs, /analytics/*, /ai/*
        │
        └──▶ Frontend Dashboard (port 3000)
                  └──▶ Charts, filters, salary benchmarks, skills
```

The **scheduler** runs both scraper and cleaner automatically every 6 hours (configurable).

---

## Quick Start

### Option A — Docker (recommended, one command)

```bash
git clone https://github.com/your-username/job-intel
cd job-intel
cp .env.example .env
docker-compose up --build
```

| Service  | URL                    |
|----------|------------------------|
| Dashboard| http://localhost:3000  |
| API docs | http://localhost:8000/docs |
| API      | http://localhost:8000  |

### Option B — Local Python

```bash
git clone https://github.com/your-username/job-intel
cd job-intel
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env

# Run the pipeline once (scrape + clean)
python scheduler/pipeline.py --once

# Start the API server
uvicorn api.main:app --reload --port 8000

# Open frontend/index.html in your browser
# (or: python -m http.server 3000 --directory frontend)
```

---

## Environment Variables

Copy `.env.example` → `.env` and adjust:

| Variable             | Default        | Description                              |
|----------------------|----------------|------------------------------------------|
| `DB_PATH`            | `data/jobs.db` | SQLite database location                 |
| `OUTPUT_DIR`         | `data/raw`     | Directory for raw JSON/CSV exports       |
| `MAX_PAGES`          | `5`            | Pagination depth per search (10 jobs/page)|
| `DELAY_MIN`          | `2.0`          | Min seconds between requests             |
| `DELAY_MAX`          | `5.0`          | Max seconds between requests             |
| `RUN_INTERVAL_HOURS` | `6`            | How often the scheduler re-scrapes       |
| `API_PORT`           | `8000`         | FastAPI server port                      |

---

## API Endpoints

All endpoints return JSON. Full interactive docs at `/docs`.

| Method | Endpoint                        | Description                              |
|--------|---------------------------------|------------------------------------------|
| GET    | `/health`                       | System status + record count             |
| GET    | `/jobs`                         | Paginated job listings with filters      |
| GET    | `/analytics/summary`            | Executive KPI overview                   |
| GET    | `/analytics/role-trends`        | Demand by role bucket                    |
| GET    | `/analytics/top-companies`      | Companies ranked by open roles           |
| GET    | `/analytics/salary-benchmarks`  | Min/median/max salary by role + seniority|
| GET    | `/analytics/hiring-activity`    | Posting volume over time                 |
| GET    | `/analytics/skills`             | Technology skill frequency               |
| GET    | `/ai/skill-demand`              | TF-IDF ranked skills for a target role   |
| GET    | `/ai/hiring-velocity`           | Company hiring velocity scoring          |

### Example queries

```bash
# Top 10 companies hiring data engineers in Bangalore
curl "localhost:8000/analytics/top-companies?role=data+engineer&city=Bangalore&limit=10"

# Salary benchmarks for senior engineers
curl "localhost:8000/analytics/salary-benchmarks?seniority=Senior"

# Companies on an accelerating hiring curve (sales intelligence)
curl "localhost:8000/ai/hiring-velocity?top_n=15"

# Skills distinctively associated with MLOps roles
curl "localhost:8000/ai/skill-demand?role=ml+engineer"
```

---

## Phase 1 — Scraper

**File:** `scraper/indeed_scraper.py`

- Searches Indeed India across 5 configurable role+location pairs
- Handles pagination up to `MAX_PAGES` deep
- Retries with exponential backoff on failures (3 attempts)
- Rate-limited with randomised delays to respect the server
- Deduplicates by `job_id` (Indeed's `data-jk` attribute or MD5 fallback)
- Exports timestamped JSON + CSV to `data/raw/`

**Missing field handling:**  
Every field defaults gracefully. If a field regex fails, the field is `None` — the record is still stored. Only records with a missing `title` are dropped entirely (ghost cards).

---

## Phase 2 — Cleaning

**File:** `cleaner/cleaner.py`

### Decisions documented:

| Field         | Raw form                               | Cleaned form                          | Decision |
|---------------|----------------------------------------|---------------------------------------|----------|
| `salary`      | "₹4,00,000 - ₹6,00,000 a year"        | `{min: 400000, max: 600000, mid: 500000}` | Regex extracts numbers + multiplier |
| `salary`      | "₹25,000 a month"                     | `{min: 300000, max: 300000}`          | Multiply × 12 for annual |
| `location`    | "Remote in Bengaluru, Karnataka"       | `{city: "Bangalore", is_remote: true}` | Alias map + remote flag |
| `posted_date` | "3 days ago"                           | `2025-04-11`                          | Relative → absolute ISO date |
| `title`       | "Sr. Data Engineer (Contract)"         | `{seniority: "Senior", role: "data engineer"}` | Regex seniority + role bucketing |
| `company`     | "infosys limited"                      | `"Infosys Limited"`                   | `.title()` normalisation |

Upsert on `job_id` — re-running the pipeline updates existing records.

---

## Phase 3 — Deployment

```
frontend/index.html   ← Single-file dashboard (Chart.js)
api/main.py           ← FastAPI, 9 endpoints
docker-compose.yml    ← One-command full-stack deploy
```

The dashboard runs entirely client-side (no build step). The API is stateless — the SQLite file is the only state.

**Production:** Replace SQLite with PostgreSQL via `DB_URL` env var (schema is compatible).

---

## Phase 4 — AI/ML Bonus

**File:** `api/ai_layer.py`

### 1. TF-IDF Skill Demand Scoring (`/ai/skill-demand`)

Identifies skills that are **distinctively** associated with a target role rather than just common across all jobs. Uses term-frequency × inverse-document-frequency on job summaries.

- **Why:** Simple bag-of-words is dominated by ubiquitous skills (Python, SQL). TF-IDF surfaces the skills that separate, say, an MLOps role from a generic backend role.
- **Trade-off:** Ignores word order (no contextual understanding). A sentence encoder would be more accurate but 400MB heavier.

### 2. Company Hiring Velocity Score (`/ai/hiring-velocity`)

Scores companies by `(7-day postings) / (30-day baseline per week)`.

- Score > 1.5 → accelerating hire (high-growth signal → warm sales lead)  
- Score < 0.5 → hiring freeze (skip for now)

- **Why:** Raw posting count favours large incumbents. Velocity reveals momentum — a 20-person startup posting 8 roles in 7 days is a stronger signal than Infosys posting 40 in a month.
- **Trade-off:** Sensitive to weekends / holidays with small datasets. A smoothed 7-day rolling average would be more robust.

---

## Project Structure

```
job-intel/
├── scraper/
│   └── indeed_scraper.py    # Phase 1: HTTP + parsing + export
├── cleaner/
│   └── cleaner.py           # Phase 2: normalisation + SQLite upsert
├── scheduler/
│   └── pipeline.py          # Orchestration + auto-scheduling
├── api/
│   ├── main.py              # FastAPI REST endpoints
│   └── ai_layer.py          # TF-IDF + velocity scoring (bonus)
├── frontend/
│   └── index.html           # Single-file dashboard
├── data/
│   ├── raw/                 # Timestamped raw JSON + CSV dumps
│   └── jobs.db              # Cleaned SQLite database
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
└── README.md
```

---

## Ethical & Legal Notes

- All data scraped is publicly accessible without authentication.
- The scraper uses conservative delays (2–5s randomised) to avoid overloading servers.
- No personal/PII data is collected — only job titles, companies, and locations.

---

## Author

**Kodurupaka Saisadhan**  
Email: [kodurupakasaisadhan@gmail.com](mailto:kodurupakasaisadhan@gmail.com)

---
- Intended for market research, not mass application automation.

---

## Time breakdown (~10h)

| Phase        | Time  |
|--------------|-------|
| Problem research + design | 1h |
| Scraper (Phase 1) | 2h |
| Cleaner (Phase 2) | 2h |
| API (Phase 3) | 1.5h |
| Frontend dashboard | 2h |
| AI/ML layer | 1h |
| README + cleanup | 0.5h |
