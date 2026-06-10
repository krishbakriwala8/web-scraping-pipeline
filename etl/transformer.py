"""
etl/transformer.py
==================
SQL-based transformation layer.

After records are loaded into the raw clean_records table, this module
runs SQL transformations directly inside PostgreSQL to enrich, aggregate,
and materialise derived datasets. All logic runs as set-based SQL — not
row-by-row Python — to leverage the database engine efficiently.

Transformations:
  1. Category tagging  — populate category from source_name if blank
  2. Score normalisation — normalise scores to [0, 1] per source
  3. Top items view     — materialise a ranked view per category
  4. Source stats       — refresh per-source ingestion summary stats
"""
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from db.connection import DatabasePool

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SQL transformation statements
# ---------------------------------------------------------------------------

SQL_BACKFILL_CATEGORY = """
UPDATE clean_records
SET    category = source_config.category
FROM (
    VALUES
        ('Hacker News',       'tech_news'),
        ('Books to Scrape',   'ecommerce'),
        ('Quotes to Scrape',  'nlp_data'),
        ('ArXiv CS Papers',   'research'),
        ('GitHub Trending',   'developer'),
        ('Python Insider Blog','tech_blog'),
        ('Wikipedia Trending','knowledge'),
        ('Toscrape Jobs (Scrapy)', 'jobs'),
        ('REST Countries',    'geo_data'),
        ('Open Library Subjects', 'books'),
        ('World Bank GDP Data','economics')
) AS source_config(source_name, category)
WHERE  clean_records.source_name = source_config.source_name
  AND  (clean_records.category IS NULL OR clean_records.category = '');
"""

SQL_NORMALISE_SCORES = """
UPDATE clean_records cr
SET    score = ROUND(
           (cr.score - stats.min_score) /
           NULLIF(stats.max_score - stats.min_score, 0),
           4
       )
FROM (
    SELECT
        source_name,
        MIN(score) AS min_score,
        MAX(score) AS max_score
    FROM   clean_records
    WHERE  score IS NOT NULL
    GROUP  BY source_name
) AS stats
WHERE  cr.source_name = stats.source_name
  AND  cr.score IS NOT NULL
  AND  stats.max_score > stats.min_score;
"""

SQL_CREATE_TOP_ITEMS_VIEW = """
CREATE OR REPLACE VIEW vw_top_items_per_category AS
SELECT  *
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY category
            ORDER BY score DESC NULLS LAST, ingested_at DESC
        ) AS rank_in_category
    FROM  clean_records
    WHERE category IS NOT NULL AND category <> ''
) ranked
WHERE rank_in_category <= 10;
"""

SQL_CREATE_SOURCE_STATS_VIEW = """
CREATE OR REPLACE VIEW vw_source_stats AS
SELECT
    source_name,
    COUNT(*)                                         AS total_records,
    COUNT(DISTINCT url)                              AS unique_urls,
    AVG(score)                                       AS avg_score,
    MAX(ingested_at)                                 AS last_ingested_at,
    COUNT(CASE WHEN ingested_at > NOW() - INTERVAL '24 hours'
               THEN 1 END)                           AS records_last_24h
FROM   clean_records
GROUP  BY source_name
ORDER  BY total_records DESC;
"""

SQL_REFRESH_BATCH_SUMMARY = """
INSERT INTO ingestion_log (batch_id, source_name, records_raw, records_clean,
                           records_failed, started_at, finished_at, status)
SELECT
    batch_id,
    source_name,
    COUNT(*) AS records_clean,
    COUNT(*) AS records_clean,
    0        AS records_failed,
    MIN(ingested_at) AS started_at,
    MAX(ingested_at) AS finished_at,
    'success' AS status
FROM   clean_records
WHERE  batch_id = %s
GROUP  BY batch_id, source_name
ON CONFLICT (batch_id, source_name) DO UPDATE
    SET records_clean = EXCLUDED.records_clean,
        finished_at   = EXCLUDED.finished_at,
        status        = EXCLUDED.status;
"""


# ---------------------------------------------------------------------------
# Transformer functions
# ---------------------------------------------------------------------------

def run_transformations(pool: "DatabasePool", batch_id: str | None = None) -> None:
    """
    Execute all SQL transformations against the database.

    Args:
        pool: Active DatabasePool instance
        batch_id: If provided, refresh batch summary for this specific batch
    """
    with pool.get_connection() as conn:
        with conn.cursor() as cur:
            _run(cur, "Backfill category", SQL_BACKFILL_CATEGORY)
            _run(cur, "Normalise scores", SQL_NORMALISE_SCORES)
            _run(cur, "Create top items view", SQL_CREATE_TOP_ITEMS_VIEW)
            _run(cur, "Create source stats view", SQL_CREATE_SOURCE_STATS_VIEW)

            if batch_id:
                _run(cur, "Refresh batch summary",
                     SQL_REFRESH_BATCH_SUMMARY, (batch_id,))

        conn.commit()
    logger.info("All SQL transformations applied successfully.")


def _run(cursor, label: str, sql: str, params=None) -> None:
    """Execute a single SQL statement with error logging."""
    try:
        cursor.execute(sql, params)
        logger.debug("Transformation OK: %s — %d rows affected",
                     label, cursor.rowcount)
    except Exception as exc:
        logger.error("Transformation FAILED [%s]: %s", label, exc)
        raise
