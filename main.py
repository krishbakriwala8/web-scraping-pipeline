"""
main.py
=======
Pipeline entrypoint — three execution modes via --mode flag.

  batch     Run every configured source once and exit.
  schedule  Start the APScheduler and run indefinitely on cron intervals.
  single    Run a single named source once (--source <name>).

Usage examples:
  python main.py --mode batch
  python main.py --mode schedule
  python main.py --mode single --source hacker_news
  python main.py --mode single --source books_to_scrape
"""
import argparse
import logging
import sys
from datetime import datetime

from config.settings import PipelineSettings
from db.connection import DatabasePool, close_pool
from db.loader import load_records, log_ingestion
from db.models import init_db
from etl.cleaner import clean_batch
from etl.transformer import run_transformations
from etl.validator import validate_batch
from scrapers.source_registry import get_scraper, run_all_scrapers


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def setup_logging() -> None:
    logging.basicConfig(
        level=PipelineSettings.LOG_LEVEL,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("logs/pipeline.log", mode="a", encoding="utf-8"),
        ],
    )


# ---------------------------------------------------------------------------
# Mode: batch
# ---------------------------------------------------------------------------

def run_batch(pool: DatabasePool) -> None:
    """Run all configured (non-Scrapy) sources once, in sequence."""
    logger = logging.getLogger("main.batch")
    logger.info("=" * 60)
    logger.info("BATCH RUN started at %s", datetime.utcnow().isoformat())
    logger.info("=" * 60)

    results = run_all_scrapers()

    total_raw = total_clean = total_inserted = 0

    for result in results:
        cleaned = clean_batch(result.records)
        validation = validate_batch(cleaned)

        try:
            inserted = load_records(pool, validation.valid)
            run_transformations(pool, batch_id=result.batch_id)
        except Exception as exc:
            logger.error("Load failed for %s: %s", result.source_name, exc)
            inserted = 0

        log_ingestion(
            pool=pool,
            batch_id=result.batch_id,
            source_name=result.source_name,
            records_raw=result.record_count,
            records_clean=validation.valid_count,
            records_failed=validation.invalid_count,
            started_at=result.started_at,
            finished_at=datetime.utcnow(),
            status="success" if result.success else "failed",
            error_message=result.error_message,
        )

        total_raw += result.record_count
        total_clean += validation.valid_count
        total_inserted += inserted

    logger.info("=" * 60)
    logger.info(
        "BATCH COMPLETE — raw=%d  clean=%d  inserted=%d",
        total_raw, total_clean, total_inserted,
    )
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# Mode: single
# ---------------------------------------------------------------------------

def run_single(pool: DatabasePool, source_name: str) -> None:
    """Run a single named source and exit."""
    logger = logging.getLogger("main.single")
    scraper = get_scraper(source_name)
    if scraper is None:
        logger.error("Source '%s' not found or is a Scrapy source.", source_name)
        sys.exit(1)

    result = scraper.run()
    cleaned = clean_batch(result.records)
    validation = validate_batch(cleaned)

    try:
        inserted = load_records(pool, validation.valid)
        run_transformations(pool, batch_id=result.batch_id)
    except Exception as exc:
        logger.error("Load failed: %s", exc)
        inserted = 0

    log_ingestion(
        pool=pool,
        batch_id=result.batch_id,
        source_name=result.source_name,
        records_raw=result.record_count,
        records_clean=validation.valid_count,
        records_failed=validation.invalid_count,
        started_at=result.started_at,
        finished_at=datetime.utcnow(),
        status="success" if result.success else "failed",
        error_message=result.error_message,
    )

    logger.info(
        "Single run complete: %s — raw=%d clean=%d inserted=%d",
        source_name, result.record_count, validation.valid_count, inserted,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Web Scraping Data Pipeline — Batch Ingestion System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--mode",
        choices=["batch", "schedule", "single"],
        default="batch",
        help="Execution mode (default: batch)",
    )
    parser.add_argument(
        "--source",
        default=None,
        help="Source name for --mode single (e.g. hacker_news)",
    )
    parser.add_argument(
        "--init-db",
        action="store_true",
        help="Initialise database schema and exit",
    )
    return parser.parse_args()


def main() -> None:
    setup_logging()
    args = parse_args()
    logger = logging.getLogger("main")

    pool = DatabasePool()

    if not pool.test_connection():
        logger.critical(
            "Cannot connect to PostgreSQL.\n"
            "  • Check your .env file (DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD)\n"
            "  • Ensure PostgreSQL is running"
        )
        sys.exit(1)

    if args.init_db:
        init_db(pool)
        logger.info("Database initialised. Exiting.")
        return

    init_db(pool)

    try:
        if args.mode == "batch":
            run_batch(pool)

        elif args.mode == "single":
            if not args.source:
                logger.error("--source is required for --mode single")
                sys.exit(1)
            run_single(pool, args.source)

        elif args.mode == "schedule":
            from scheduler.batch_scheduler import start as start_scheduler
            close_pool()   # scheduler creates its own pool per job thread
            start_scheduler()

    finally:
        close_pool()


if __name__ == "__main__":
    main()
