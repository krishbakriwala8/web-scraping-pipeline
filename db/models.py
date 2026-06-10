"""
db/models.py
============
PostgreSQL table definitions (DDL) for the scraping pipeline.

Tables:
  raw_scrape_data   — append-only raw HTML/JSON snapshot per batch
  clean_records     — validated, normalised records after ETL
  ingestion_log     — per-batch audit trail with counts and timings

Views (created by transformer.py):
  vw_top_items_per_category
  vw_source_stats

Run `init_db()` once to create all tables and indexes.
"""
import logging

from db.connection import DatabasePool

logger = logging.getLogger(__name__)


DDL_RAW_SCRAPE_DATA = """
CREATE TABLE IF NOT EXISTS raw_scrape_data (
    id          SERIAL PRIMARY KEY,
    source_name VARCHAR(100)  NOT NULL,
    url         TEXT          NOT NULL,
    raw_html    TEXT,
    scraped_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    batch_id    UUID          NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_raw_source_batch
    ON raw_scrape_data (source_name, batch_id);
"""

DDL_CLEAN_RECORDS = """
CREATE TABLE IF NOT EXISTS clean_records (
    id           SERIAL PRIMARY KEY,
    source_name  VARCHAR(100),
    title        TEXT          NOT NULL,
    url          TEXT          DEFAULT '',
    description  TEXT          DEFAULT '',
    author       VARCHAR(255)  DEFAULT '',
    category     VARCHAR(100)  DEFAULT '',
    tags         TEXT[]        DEFAULT '{}',
    score        NUMERIC(12,4),
    price        NUMERIC(12,2),
    published_at TIMESTAMPTZ,
    ingested_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    batch_id     VARCHAR(50)   NOT NULL,
    fingerprint  CHAR(40)      NOT NULL,
    CONSTRAINT uq_clean_fingerprint UNIQUE (fingerprint)
);
CREATE INDEX IF NOT EXISTS idx_clean_source
    ON clean_records (source_name);
CREATE INDEX IF NOT EXISTS idx_clean_category
    ON clean_records (category);
CREATE INDEX IF NOT EXISTS idx_clean_batch
    ON clean_records (batch_id);
CREATE INDEX IF NOT EXISTS idx_clean_ingested
    ON clean_records (ingested_at DESC);
"""

DDL_INGESTION_LOG = """
CREATE TABLE IF NOT EXISTS ingestion_log (
    id             SERIAL PRIMARY KEY,
    batch_id       VARCHAR(50)  NOT NULL,
    source_name    VARCHAR(100) NOT NULL,
    records_raw    INT          DEFAULT 0,
    records_clean  INT          DEFAULT 0,
    records_failed INT          DEFAULT 0,
    started_at     TIMESTAMPTZ,
    finished_at    TIMESTAMPTZ,
    status         VARCHAR(20)  DEFAULT 'pending',
    error_message  TEXT         DEFAULT '',
    CONSTRAINT uq_log_batch_source UNIQUE (batch_id, source_name)
);
CREATE INDEX IF NOT EXISTS idx_log_batch
    ON ingestion_log (batch_id);
CREATE INDEX IF NOT EXISTS idx_log_status
    ON ingestion_log (status);
"""


def init_db(pool: DatabasePool | None = None) -> None:
    """
    Create all pipeline tables and indexes if they do not exist.
    Safe to call multiple times (idempotent via IF NOT EXISTS).

    Args:
        pool: Optional DatabasePool. Creates a new pool if not supplied.
    """
    if pool is None:
        pool = DatabasePool()

    logger.info("Initialising database schema…")
    with pool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(DDL_RAW_SCRAPE_DATA)
            cur.execute(DDL_CLEAN_RECORDS)
            cur.execute(DDL_INGESTION_LOG)
        conn.commit()
    logger.info("Database schema ready.")


def drop_all(pool: DatabasePool | None = None) -> None:
    """
    DROP all pipeline tables — destructive, for dev/test use only.
    """
    if pool is None:
        pool = DatabasePool()

    with pool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DROP TABLE IF EXISTS clean_records, raw_scrape_data, ingestion_log CASCADE;
                DROP VIEW  IF EXISTS vw_top_items_per_category, vw_source_stats CASCADE;
            """)
        conn.commit()
    logger.warning("All pipeline tables dropped.")
