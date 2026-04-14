"""
Job Market Intelligence API
-----------------------------
FastAPI app exposing the cleaned job database via REST endpoints.
Designed for business users and frontend consumption.
"""

import os
import sys
import sqlite3
import json
from datetime import datetime, date
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cleaner.cleaner import get_db

try:
    from fastapi import FastAPI, Query, HTTPException
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse
except ImportError:
    raise SystemExit("FastAPI not installed. Run: pip install fastapi uvicorn")

DB_PATH = os.getenv("DB_PATH", "data/jobs.db")

app = FastAPI(
    title="Job Market Intelligence API",
    description="B2B competitive hiring intelligence from Indeed India",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def query_db(sql: str, params: tuple = ()) -> list[dict]:
    conn = get_db(DB_PATH)
    try:
        cur = conn.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def scalar_db(sql: str, params: tuple = ()) -> any:
    conn = get_db(DB_PATH)
    try:
        cur = conn.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/")
def root():
    return {
        "app": "JobIntel - B2B Hiring Intelligence API",
        "status": "online",
        "version": "1.0.0",
        "endpoints": {
            "/health": "System health check",
            "/jobs": "List all jobs with filters",
            "/analytics/summary": "Overall hiring summary",
            "/analytics/top-companies": "Top hiring companies",
            "/analytics/role-trends": "Job roles in demand",
            "/analytics/salary-benchmarks": "Salary data by role & level",
            "/analytics/hiring-activity": "Hiring trends (last 30 days)",
            "/analytics/skills": "Top technology skills demanded"
        }
    }


@app.get("/health")
def health():
    total = scalar_db("SELECT COUNT(*) FROM jobs")
    last  = scalar_db("SELECT MAX(scraped_at) FROM jobs")
    return {"status": "ok", "total_jobs": total, "last_scraped": last}


# ---------------------------------------------------------------------------
# /jobs — paginated job listing with filters
# ---------------------------------------------------------------------------

@app.get("/jobs")
def list_jobs(
    role:     Optional[str] = Query(None, description="Role bucket filter"),
    company:  Optional[str] = Query(None, description="Company name filter (partial)"),
    city:     Optional[str] = Query(None, description="City filter"),
    seniority:Optional[str] = Query(None, description="Junior | Mid | Senior | Manager | Intern"),
    is_remote:Optional[bool]= Query(None),
    limit:    int = Query(50, ge=1, le=200),
    offset:   int = Query(0, ge=0),
):
    conditions = ["1=1"]
    params: list = []

    if role:
        conditions.append("role_bucket = ?")
        params.append(role)
    if company:
        conditions.append("company LIKE ?")
        params.append(f"%{company}%")
    if city:
        conditions.append("city LIKE ?")
        params.append(f"%{city}%")
    if seniority:
        conditions.append("seniority = ?")
        params.append(seniority)
    if is_remote is not None:
        conditions.append("is_remote = ?")
        params.append(1 if is_remote else 0)

    where = " AND ".join(conditions)
    sql = f"""
        SELECT job_id, title_clean, seniority, role_bucket,
               company, city, is_remote, salary_min, salary_max,
               salary_midpoint, job_type, posted_date, job_url, scraped_at
        FROM jobs
        WHERE {where}
        ORDER BY scraped_at DESC
        LIMIT ? OFFSET ?
    """
    total_sql = f"SELECT COUNT(*) FROM jobs WHERE {where}"

    rows  = query_db(sql, tuple(params) + (limit, offset))
    total = scalar_db(total_sql, tuple(params))

    return {"total": total, "limit": limit, "offset": offset, "jobs": rows}


# ---------------------------------------------------------------------------
# /analytics/top-companies — hiring velocity
# ---------------------------------------------------------------------------

@app.get("/analytics/top-companies")
def top_companies(
    role: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100),
):
    conditions = ["company IS NOT NULL"]
    params: list = []
    if role:
        conditions.append("role_bucket = ?")
        params.append(role)
    if city:
        conditions.append("city LIKE ?")
        params.append(f"%{city}%")

    where = " AND ".join(conditions)
    sql = f"""
        SELECT company, COUNT(*) as open_roles,
               GROUP_CONCAT(DISTINCT role_bucket) as roles,
               GROUP_CONCAT(DISTINCT city) as cities
        FROM jobs
        WHERE {where}
        GROUP BY company
        ORDER BY open_roles DESC
        LIMIT ?
    """
    return query_db(sql, tuple(params) + (limit,))


# ---------------------------------------------------------------------------
# /analytics/role-trends — role demand breakdown
# ---------------------------------------------------------------------------

@app.get("/analytics/role-trends")
def role_trends(city: Optional[str] = Query(None)):
    conditions = ["role_bucket IS NOT NULL"]
    params: list = []
    if city:
        conditions.append("city LIKE ?")
        params.append(f"%{city}%")

    where = " AND ".join(conditions)
    sql = f"""
        SELECT role_bucket,
               COUNT(*) as total,
               SUM(CASE WHEN is_remote=1 THEN 1 ELSE 0 END) as remote_count,
               AVG(salary_midpoint) as avg_salary,
               COUNT(DISTINCT company) as hiring_companies
        FROM jobs
        WHERE {where}
        GROUP BY role_bucket
        ORDER BY total DESC
    """
    return query_db(sql, tuple(params))


# ---------------------------------------------------------------------------
# /analytics/salary-benchmarks
# ---------------------------------------------------------------------------

@app.get("/analytics/salary-benchmarks")
def salary_benchmarks(
    role:      Optional[str] = Query(None),
    city:      Optional[str] = Query(None),
    seniority: Optional[str] = Query(None),
):
    conditions = ["salary_midpoint IS NOT NULL"]
    params: list = []
    if role:
        conditions.append("role_bucket = ?")
        params.append(role)
    if city:
        conditions.append("city LIKE ?")
        params.append(f"%{city}%")
    if seniority:
        conditions.append("seniority = ?")
        params.append(seniority)

    where = " AND ".join(conditions)
    sql = f"""
        SELECT role_bucket, seniority,
               COUNT(*) as sample_size,
               MIN(salary_min)  as p10_salary,
               AVG(salary_midpoint) as median_salary,
               MAX(salary_max)  as p90_salary
        FROM jobs
        WHERE {where}
        GROUP BY role_bucket, seniority
        ORDER BY median_salary DESC
    """
    return query_db(sql, tuple(params))


# ---------------------------------------------------------------------------
# /analytics/hiring-activity — posting volume over time
# ---------------------------------------------------------------------------

@app.get("/analytics/hiring-activity")
def hiring_activity(
    role:  Optional[str] = Query(None),
    days:  int = Query(30, ge=1, le=90),
):
    conditions = ["posted_date IS NOT NULL",
                  "posted_date >= date('now', ? || ' days')"]
    params: list = [f"-{days}"]

    if role:
        conditions.append("role_bucket = ?")
        params.append(role)

    where = " AND ".join(conditions)
    sql = f"""
        SELECT posted_date, COUNT(*) as postings,
               COUNT(DISTINCT company) as unique_companies
        FROM jobs
        WHERE {where}
        GROUP BY posted_date
        ORDER BY posted_date DESC
    """
    return query_db(sql, tuple(params))


# ---------------------------------------------------------------------------
# /analytics/skills — keyword frequency in summaries (bonus AI-lite)
# ---------------------------------------------------------------------------

TECH_SKILLS = [
    "python", "java", "javascript", "typescript", "sql", "spark",
    "kafka", "kubernetes", "docker", "aws", "gcp", "azure",
    "react", "node", "go", "rust", "scala", "airflow",
    "tensorflow", "pytorch", "llm", "mlops", "dbt",
]

@app.get("/analytics/skills")
def skill_frequency(role: Optional[str] = Query(None)):
    conditions = ["summary IS NOT NULL"]
    params: list = []
    if role:
        conditions.append("role_bucket = ?")
        params.append(role)

    rows = query_db(
        f"SELECT summary FROM jobs WHERE {' AND '.join(conditions)}",
        tuple(params)
    )
    text = " ".join((r["summary"] or "") for r in rows).lower()

    counts = {}
    for skill in TECH_SKILLS:
        cnt = len([m for m in __import__("re").finditer(r"\b" + skill + r"\b", text)])
        if cnt:
            counts[skill] = cnt

    total = len(rows)
    return [
        {"skill": k, "mentions": v, "pct_jobs": round(v / total * 100, 1) if total else 0}
        for k, v in sorted(counts.items(), key=lambda x: -x[1])
    ]


# ---------------------------------------------------------------------------
# /analytics/summary — executive overview
# ---------------------------------------------------------------------------

@app.get("/analytics/summary")
def summary():
    return {
        "total_jobs":       scalar_db("SELECT COUNT(*) FROM jobs"),
        "unique_companies": scalar_db("SELECT COUNT(DISTINCT company) FROM jobs"),
        "unique_cities":    scalar_db("SELECT COUNT(DISTINCT city) FROM jobs"),
        "remote_jobs":      scalar_db("SELECT COUNT(*) FROM jobs WHERE is_remote=1"),
        "with_salary":      scalar_db("SELECT COUNT(*) FROM jobs WHERE salary_midpoint IS NOT NULL"),
        "last_scraped":     scalar_db("SELECT MAX(scraped_at) FROM jobs"),
        "top_role":         scalar_db(
            "SELECT role_bucket FROM jobs GROUP BY role_bucket ORDER BY COUNT(*) DESC LIMIT 1"
        ),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)
