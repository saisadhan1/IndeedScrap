"""
AI/ML Bonus Layer — Hiring Signal Scoring & Skill Gap Analysis
---------------------------------------------------------------

WHY THIS APPROACH:
  Two lightweight ML techniques that add genuine B2B value
  without requiring GPUs or expensive models:

  1. TF-IDF Skill Demand Scoring
     Uses term-frequency/inverse-document-frequency to surface
     skills that are disproportionately mentioned in a target role
     vs. the corpus as a whole — revealing "signal" skills vs noise.

  2. Company Hiring Velocity Score
     Computes a simple recency-weighted count to identify companies
     that are on an accelerating hiring curve — useful for sales teams
     targeting companies in growth mode.

TRADE-OFFS:
  - TF-IDF is a bag-of-words model; it ignores word order and context.
    A proper sentence encoder (e.g. sentence-transformers) would give
    richer results, but adds ~400MB of model weight — overkill here.
  - Velocity scoring uses a 7-day/14-day/30-day window ratio.
    This is sensitive to weekends and holidays; a smoothed rolling
    average would be more robust with a larger dataset.
  - Everything runs on-device in pure Python — no external API calls
    or model downloads required.
"""

import os
import re
import math
import sqlite3
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional

log = logging.getLogger(__name__)


DB_PATH = os.getenv("DB_PATH", "data/jobs.db")

TECH_SKILLS = [
    "python","java","javascript","typescript","go","rust","scala","kotlin","swift",
    "sql","nosql","postgresql","mongodb","redis","elasticsearch",
    "spark","kafka","airflow","dbt","flink",
    "aws","gcp","azure","terraform","pulumi",
    "docker","kubernetes","helm","ci/cd","github actions","jenkins",
    "react","nextjs","vue","angular","node",
    "tensorflow","pytorch","scikit-learn","xgboost","mlflow","kubeflow",
    "llm","rag","langchain","openai","huggingface",
    "pandas","numpy","matplotlib","pyspark",
    "rest api","graphql","grpc","microservices","event-driven",
    "agile","scrum","jira","product roadmap","okr",
]


# ---------------------------------------------------------------------------
# 1. TF-IDF Skill Demand Scorer
# ---------------------------------------------------------------------------

def build_tfidf_scores(conn: sqlite3.Connection, target_role: Optional[str] = None) -> list[dict]:
    """
    Returns skills ranked by their TF-IDF score for `target_role`
    vs. the whole corpus. High score = strongly associated with that role.
    """
    cur = conn.execute("SELECT role_bucket, summary FROM jobs WHERE summary IS NOT NULL")
    rows = cur.fetchall()

    # Build per-doc (per-job) term sets for IDF
    N = len(rows)
    if N == 0:
        return []

    doc_skill_sets: list[set] = []
    target_skill_counts: dict[str, int] = defaultdict(int)
    target_doc_count = 0

    for role_bucket, summary in rows:
        text = (summary or "").lower()
        present = set()
        for skill in TECH_SKILLS:
            pattern = r"\b" + re.escape(skill) + r"\b"
            if re.search(pattern, text):
                present.add(skill)
                if target_role is None or role_bucket == target_role:
                    target_skill_counts[skill] += 1
        doc_skill_sets.append(present)
        if target_role is None or role_bucket == target_role:
            target_doc_count += 1

    if target_doc_count == 0:
        return []

    # IDF: log(N / df) — penalises skills mentioned everywhere
    df: dict[str, int] = defaultdict(int)
    for skill_set in doc_skill_sets:
        for skill in skill_set:
            df[skill] += 1

    results = []
    for skill in TECH_SKILLS:
        tf = target_skill_counts.get(skill, 0) / target_doc_count
        idf = math.log((N + 1) / (df.get(skill, 0) + 1))
        score = tf * idf
        if score > 0:
            results.append({
                "skill": skill,
                "tf": round(tf, 4),
                "idf": round(idf, 4),
                "tfidf_score": round(score, 4),
                "mention_rate_pct": round(tf * 100, 1),
                "target_role": target_role or "all",
            })

    return sorted(results, key=lambda x: -x["tfidf_score"])


# ---------------------------------------------------------------------------
# 2. Company Hiring Velocity Score
# ---------------------------------------------------------------------------

def hiring_velocity(conn: sqlite3.Connection, top_n: int = 20) -> list[dict]:
    """
    Score companies by their hiring velocity (recent postings vs baseline).
    Score = (7-day postings) / max(1, (30-day postings / 4))
    Score > 1 → accelerating; Score < 1 → decelerating.
    """
    now = datetime.now(timezone.utc)
    d7  = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    d30 = (now - timedelta(days=30)).strftime("%Y-%m-%d")

    sql = """
        SELECT
            company,
            COUNT(*) FILTER (WHERE posted_date >= ?) as postings_7d,
            COUNT(*) FILTER (WHERE posted_date >= ?) as postings_30d,
            COUNT(*) as total_postings,
            GROUP_CONCAT(DISTINCT role_bucket) as roles,
            GROUP_CONCAT(DISTINCT city) as cities
        FROM jobs
        WHERE company IS NOT NULL AND posted_date IS NOT NULL
        GROUP BY company
        HAVING total_postings >= 2
        ORDER BY postings_7d DESC
        LIMIT ?
    """
    # SQLite < 3.25 may not support FILTER; use CASE WHEN fallback
    sql_fallback = """
        SELECT
            company,
            SUM(CASE WHEN posted_date >= ? THEN 1 ELSE 0 END) as postings_7d,
            SUM(CASE WHEN posted_date >= ? THEN 1 ELSE 0 END) as postings_30d,
            COUNT(*) as total_postings,
            GROUP_CONCAT(DISTINCT role_bucket) as roles,
            GROUP_CONCAT(DISTINCT city) as cities
        FROM jobs
        WHERE company IS NOT NULL AND posted_date IS NOT NULL
        GROUP BY company
        HAVING total_postings >= 2
        ORDER BY postings_7d DESC
        LIMIT ?
    """
    try:
        cur = conn.execute(sql, (d7, d30, top_n))
    except sqlite3.OperationalError:
        cur = conn.execute(sql_fallback, (d7, d30, top_n))

    results = []
    for row in cur.fetchall():
        company, p7, p30, total, roles, cities = row
        baseline = max(1, (p30 or 0) / 4)
        velocity = round((p7 or 0) / baseline, 2)
        signal = "🔥 Accelerating" if velocity > 1.5 else ("📈 Growing" if velocity > 0.8 else "📉 Slowing")
        results.append({
            "company": company,
            "postings_7d": p7 or 0,
            "postings_30d": p30 or 0,
            "total_postings": total,
            "velocity_score": velocity,
            "signal": signal,
            "roles": (roles or "").split(","),
            "cities": list(set((cities or "").split(","))),
        })

    return sorted(results, key=lambda x: -x["velocity_score"])


# ---------------------------------------------------------------------------
# API endpoints (added to FastAPI app in api/main.py)
# ---------------------------------------------------------------------------

def register_ai_routes(app, get_db_fn):
    """Call this from api/main.py to mount the AI endpoints."""
    try:
        from fastapi import Query as FQuery
    except ImportError:
        return

    @app.get("/ai/skill-demand", tags=["AI/ML"])
    def skill_demand(role: Optional[str] = FQuery(None)):
        """
        TF-IDF ranked skill demand for a target role.
        High tfidf_score = this skill is distinctively important for this role.
        """
        conn = get_db_fn(DB_PATH)
        try:
            return build_tfidf_scores(conn, role)[:20]
        finally:
            conn.close()

    @app.get("/ai/hiring-velocity", tags=["AI/ML"])
    def hiring_velocity_endpoint(top_n: int = FQuery(20, ge=1, le=100)):
        """
        Company hiring velocity score.
        Score > 1 → posting rate accelerating vs 30-day baseline.
        Useful for identifying companies in growth mode.
        """
        conn = get_db_fn(DB_PATH)
        try:
            return hiring_velocity(conn, top_n)
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# CLI demo
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    conn = sqlite3.connect(DB_PATH)

    role = sys.argv[1] if len(sys.argv) > 1 else "data engineer"
    print(f"\n=== TF-IDF Skill Demand for '{role}' ===")
    for r in build_tfidf_scores(conn, role)[:10]:
        print(f"  {r['skill']:25s}  score={r['tfidf_score']:.4f}  mention_rate={r['mention_rate_pct']}%")

    print("\n=== Company Hiring Velocity ===")
    for c in hiring_velocity(conn, 10):
        print(f"  {c['company']:30s}  velocity={c['velocity_score']:.2f}  {c['signal']}")

    conn.close()
