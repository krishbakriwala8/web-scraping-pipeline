"""
scheduler/batch_scheduler.py
=============================
APScheduler-based batch ingestion scheduler.

Each data source defined in config/sources.yaml is registered as an
independent job with its own `interval_hours` schedule. Jobs run in
a thread pool so slow scrapers do not block each other.

Entrypoints:
  build_scheduler()  — create and configure the scheduler (does not start it)
  run_pipeline_job() — the actual per-source job callable
  start()            — start the scheduler and block until KeyboardInterrupt
"""
import logging
import signal
import sys
from datetime import datetime

import yaml
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.blocking import BlockingScheduler

from config.settings import PipelineSettings
from db.connection import DatabasePool, close_pool
from db.loader import load_records, log_ingestion
from db.models import init_db
from etl.cleaner import clean_batch
from etl.transformer import run_transformations
from etl.validator import validate_batch
from scrapers.source_registry import get_scraper

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Core pipeline job — one per data source
# ---------------------------------------------------------------------------

def run_pipeline_job(source_name: str, source_config: dict) -> None:
    """
    End-to-end pipeline run for a single data source.

    Steps:
      1. Scrape  — fetch raw records via BS4 / API scraper
      2. Clean   — normalise, strip HTML, deduplicate
      3. Validate — enforce Pydantic schema
      4. Load    — bulk-upsert into PostgreSQL
      5. Transform — run SQL enrichment passes
      6. Log     — write ingestion audit row
    """
    logger.info("▶ Job started: %s", source_name)
    pool = DatabasePool()

    scraper = get_scraper(source_name)
    if scraper is None:
        logger.warning("No scraper found for source '%s' — skipping.", source_name)
        return

    scrape_result = scraper.run()

    cleaned = clean_batch(scrape_result.records)
    validation = validate_batch(cleaned)

    inserted = 0
    error_msg = scrape_result.error_message

    try:
        inserted = load_records(pool, validation.valid)
        run_transformations(pool, batch_id=scrape_result.batch_id)
    except Exception as exc:
        error_msg = str(exc)
        logger.error("Load/transform failed for %s: %s", source_name, exc)

    status = "success" if scrape_result.success and not error_msg else "partial"

    log_ingestion(
        pool=pool,
        batch_id=scrape_result.batch_id,
        source_name=source_name,
        records_raw=scrape_result.record_count,
        records_clean=validation.valid_count,
        records_failed=validation.invalid_count,
        started_at=scrape_result.started_at,
        finished_at=datetime.utcnow(),
        status=status,
        error_message=error_msg,
    )

    logger.info(
        "✔ Job complete: %s — raw=%d clean=%d inserted=%d failed=%d [%s]",
        source_name,
        scrape_result.record_count,
        validation.valid_count,
        inserted,
        validation.invalid_count,
        status,
    )


# ---------------------------------------------------------------------------
# Scheduler setup
# ---------------------------------------------------------------------------

def build_scheduler() -> BlockingScheduler:
    """
    Create a BlockingScheduler and register one interval job per source.
    Scrapy sources are excluded here (run separately to avoid Twisted conflicts).
    """
    scheduler = BlockingScheduler(
        jobstores={"default": MemoryJobStore()},
        executors={"default": ThreadPoolExecutor(max_workers=4)},
        job_defaults={
            "coalesce": True,       # merge missed runs into one
            "max_instances": 1,     # prevent overlapping runs per source
            "misfire_grace_time": 300,
        },
    )

    with open(PipelineSettings.SOURCES_CONFIG) as f:
        sources = yaml.safe_load(f).get("sources", {})

    registered = 0
    for source_name, config in sources.items():
        if config.get("scraper_type") == "scrapy":
            logger.info("Skipping Scrapy source from scheduler: %s", source_name)
            continue

        interval_hours = config.get("interval_hours", 6)
        scheduler.add_job(
            func=run_pipeline_job,
            trigger="interval",
            hours=interval_hours,
            id=source_name,
            name=config.get("name", source_name),
            kwargs={"source_name": source_name, "source_config": config},
            next_run_time=datetime.now(),   # run immediately on start
        )
        logger.info(
            "  Registered: %-30s every %dh", source_name, interval_hours
        )
        registered += 1

    logger.info("Scheduler ready: %d sources registered.", registered)
    return scheduler


def start() -> None:
    """
    Initialise the DB, build the scheduler, and run until interrupted.
    Handles SIGTERM for clean shutdown in containerised environments.
    """
    logging.basicConfig(
        level=PipelineSettings.LOG_LEVEL,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger.info("Initialising database…")
    pool = DatabasePool()
    if not pool.test_connection():
        logger.critical("Cannot connect to PostgreSQL. Check your .env settings.")
        sys.exit(1)
    init_db(pool)

    scheduler = build_scheduler()

    def _shutdown(signum, frame):
        logger.info("Shutdown signal received — stopping scheduler…")
        scheduler.shutdown(wait=False)
        close_pool()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)

    logger.info("Starting scheduler. Press Ctrl+C to stop.")
    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt — shutting down.")
        scheduler.shutdown()
        close_pool()
