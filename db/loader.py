"""
db/loader.py
============
Bulk insert and upsert layer for the scraping pipeline.

Uses psycopg2's `execute_values` for efficient batch inserts
(10–100× faster than individual INSERT calls).

Responsibilities:
  - Upsert validated CleanRecord objects into clean_records
    (ON CONFLICT DO NOTHING on the fingerprint unique constraint)
  - Insert ingestion audit rows into ingestion_log
  - Return per-batch insert counts for reporting
"""
import logging
from datetime import datetime

from psycopg2.extras import execute_values

from db.connection import DatabasePool
from etl.validator import CleanRecord

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# INSERT statements
# ---------------------------------------------------------------------------

SQL_UPSERT_CLEAN = """
INSERT INTO clean_records (
    source_name, title, url, description, author,
    category, tags, score, price,
    published_at, ingested_at, batch_id, fingerprint
)
VALUES %s
ON CONFLICT (fingerprint) DO NOTHING;
"""

SQL_UPSERT_LOG = """
INSERT INTO ingestion_log (
    batch_id, source_name,
    records_raw, records_clean, records_failed,
    started_at, finished_at, status, error_message
)
VALUES (%(batch_id)s, %(source_name)s,
        %(records_raw)s, %(records_clean)s, %(records_failed)s,
        %(started_at)s, %(finished_at)s, %(status)s, %(error_message)s)
ON CONFLICT (batch_id, source_name) DO UPDATE
    SET records_raw    = EXCLUDED.records_raw,
        records_clean  = EXCLUDED.records_clean,
        records_failed = EXCLUDED.records_failed,
        finished_at    = EXCLUDED.finished_at,
        status         = EXCLUDED.status,
        error_message  = EXCLUDED.error_message;
"""


def _record_to_tuple(r: CleanRecord) -> tuple:
    """Convert a CleanRecord to the positional tuple expected by execute_values."""
    return (
        r.source_name,
        r.title,
        r.url,
        r.description,
        r.author,
        r.category,
        r.tags,            # psycopg2 auto-converts list → TEXT[]
        r.score,
        r.price,
        r.published_at,
        r.ingested_at,
        r.batch_id,
        r.fingerprint,
    )


def load_records(
    pool: DatabasePool,
    records: list[CleanRecord],
    batch_size: int = 100,
) -> int:
    """
    Bulk-upsert a list of validated CleanRecord objects into clean_records.

    Args:
        pool:       Active DatabasePool instance.
        records:    Validated records from etl.validator.validate_batch().
        batch_size: Number of rows per execute_values call.

    Returns:
        Total number of rows inserted (excluding ignored duplicates).
    """
    if not records:
        logger.debug("load_records called with empty list — nothing to do.")
        return 0

    total_inserted = 0
    batches = [records[i:i + batch_size] for i in range(0, len(records), batch_size)]

    with pool.get_connection() as conn:
        with conn.cursor() as cur:
            for batch in batches:
                rows = [_record_to_tuple(r) for r in batch]
                execute_values(cur, SQL_UPSERT_CLEAN, rows, page_size=batch_size)
                total_inserted += cur.rowcount if cur.rowcount > 0 else 0
        conn.commit()

    logger.info(
        "load_records: %d records upserted (%d batches of ≤%d)",
        total_inserted, len(batches), batch_size,
    )
    return total_inserted


def log_ingestion(
    pool: DatabasePool,
    batch_id: str,
    source_name: str,
    records_raw: int,
    records_clean: int,
    records_failed: int,
    started_at: datetime,
    finished_at: datetime,
    status: str = "success",
    error_message: str = "",
) -> None:
    """
    Write or update an ingestion audit row in ingestion_log.
    Called once per source per batch run.
    """
    with pool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(SQL_UPSERT_LOG, {
                "batch_id": batch_id,
                "source_name": source_name,
                "records_raw": records_raw,
                "records_clean": records_clean,
                "records_failed": records_failed,
                "started_at": started_at,
                "finished_at": finished_at,
                "status": status,
                "error_message": error_message[:1000],
            })
        conn.commit()

    logger.debug(
        "Ingestion log updated: batch=%s source=%s status=%s",
        batch_id, source_name, status,
    )
