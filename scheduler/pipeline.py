"""
Pipeline Scheduler
------------------
Runs the full scrape → clean → store pipeline on a schedule.
Can be triggered by cron or run as a daemon process.

Usage:
  python scheduler/pipeline.py                # runs once immediately + every 6h
  python scheduler/pipeline.py --once         # single run only
  RUN_INTERVAL_HOURS=12 python scheduler/pipeline.py
"""

import os
import sys
import time
import logging
import argparse
import threading
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scraper.indeed_scraper import run_scraper
from cleaner.cleaner import run_cleaner

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("pipeline")

RUN_INTERVAL_HOURS = float(os.getenv("RUN_INTERVAL_HOURS", "6"))
DB_PATH = os.getenv("DB_PATH", "data/jobs.db")


def run_pipeline(searches=None):
    """Full pipeline: scrape → clean → store."""
    log.info("=" * 60)
    log.info("Pipeline run started at %s", datetime.now().isoformat())

    try:
        # Phase 1: Scrape
        log.info("[1/2] Starting scraper…")
        raw_path = run_scraper(searches)
        log.info("[1/2] Scraper complete → %s", raw_path)

        # Phase 2: Clean & store
        log.info("[2/2] Starting cleaner…")
        count = run_cleaner(raw_path, DB_PATH)
        log.info("[2/2] Cleaner complete → %d records stored", count)

        log.info("Pipeline run SUCCEEDED")
        return True

    except Exception as e:
        log.error("Pipeline run FAILED: %s", e, exc_info=True)
        return False


def run_loop():
    """Run pipeline, then sleep, then repeat."""
    interval_secs = RUN_INTERVAL_HOURS * 3600
    while True:
        run_pipeline()
        log.info("Next run in %.0f hours", RUN_INTERVAL_HOURS)
        time.sleep(interval_secs)


def main():
    parser = argparse.ArgumentParser(description="Job Intel Pipeline")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    args = parser.parse_args()

    if args.once:
        success = run_pipeline()
        sys.exit(0 if success else 1)
    else:
        run_loop()


if __name__ == "__main__":
    main()
