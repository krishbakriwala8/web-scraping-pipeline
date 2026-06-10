# 📚 Web Scraping Pipeline — Team Wiki

> **Confluence-style documentation for onboarding, operations, and maintenance.**

---

## Table of Contents

1. [Overview](#1-overview)
2. [Developer Onboarding](#2-developer-onboarding)
3. [Architecture Deep-Dive](#3-architecture-deep-dive)
4. [Adding a New Scraper Source](#4-adding-a-new-scraper-source)
5. [Database Maintenance Runbook](#5-database-maintenance-runbook)
6. [Scheduler Job Reference](#6-scheduler-job-reference)
7. [ETL Stage Reference](#7-etl-stage-reference)
8. [Troubleshooting & FAQ](#8-troubleshooting--faq)
9. [Contributing Guidelines](#9-contributing-guidelines)

---

## 1. Overview

This pipeline ingests structured data from **11 web sources** into a PostgreSQL database on a configurable schedule. It was designed for reproducibility, low operational overhead, and easy extension by future team members.

**Key capabilities:**

| Capability | Implementation |
|---|---|
| Static HTML scraping | BeautifulSoup 4 + requests |
| Dynamic multi-page crawling | Scrapy spiders |
| REST API ingestion | Requests + JSON parsing |
| Scheduling | APScheduler (interval-based per source) |
| Data validation | Pydantic v2 |
| Database | PostgreSQL 15 via psycopg2 |
| Deduplication | SHA-1 fingerprint (URL/title) |
| SQL enrichment | Set-based UPDATE + VIEW materialisation |

---

## 2. Developer Onboarding

### Prerequisites

- Python 3.10+
- PostgreSQL 15 (local or Docker)
- Git

### First-time setup

```bash
# 1. Clone the repository
git clone https://github.com/krishbakriwala8/web-scraping-pipeline.git
cd web-scraping-pipeline

# 2. Create and activate virtual environment
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Open .env and fill in your PostgreSQL credentials

# 5. Create the database (PostgreSQL must be running)
psql -U postgres -c "CREATE DATABASE scraping_pipeline;"

# 6. Initialise schema
python main.py --init-db

# 7. Run your first batch
python main.py --mode batch
```

### Quick Docker PostgreSQL

If you don't have PostgreSQL installed locally:

```bash
docker run -d \
  --name scraping-pg \
  -e POSTGRES_DB=scraping_pipeline \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgres:15
```

Then set `DB_PASSWORD=postgres` in your `.env`.

---

## 3. Architecture Deep-Dive

### Request / data flow

```
Data Source (HTML / JSON)
        │
        ▼
  [scrapers/]           ← Fetches raw content; returns list[RawRecord]
  BaseScraper.run()
        │
        ▼
  [etl/cleaner.py]      ← Normalises text, URLs, dates; deduplicates
        │
        ▼
  [etl/validator.py]    ← Pydantic schema enforcement; splits valid/invalid
        │
        ▼
  [db/loader.py]        ← execute_values bulk upsert into clean_records
        │
        ▼
  [etl/transformer.py]  ← SQL: backfill category, normalise scores, views
        │
        ▼
  [db/loader.py]        ← log_ingestion() → ingestion_log audit row
```

### Module responsibilities (one-liner)

| Module | Job |
|---|---|
| `scrapers/base_scraper.py` | Abstract interface all scrapers implement |
| `scrapers/bs4_scraper.py` | Requests + BeautifulSoup for static pages |
| `scrapers/scrapy_spider.py` | Scrapy spiders + CrawlerProcess runner |
| `scrapers/source_registry.py` | Factory: map source name → scraper instance |
| `etl/parser.py` | CSS/XPath helpers, URL resolution, score parsing |
| `etl/cleaner.py` | Text normalisation, dedup, fingerprinting |
| `etl/validator.py` | Pydantic CleanRecord schema + batch validation |
| `etl/transformer.py` | SQL-based enrichment executed in PostgreSQL |
| `db/connection.py` | psycopg2 ThreadedConnectionPool + context manager |
| `db/models.py` | DDL for all tables + `init_db()` |
| `db/loader.py` | Bulk insert (`execute_values`) + audit logging |
| `scheduler/batch_scheduler.py` | APScheduler per-source interval jobs |
| `main.py` | CLI: `batch`, `schedule`, `single` modes |

---

## 4. Adding a New Scraper Source

### Step 1 — Add entry to `config/sources.yaml`

```yaml
my_new_source:
  name: "My New Source"
  url: "https://example.com/data"
  scraper_type: "bs4"          # bs4 | api | scrapy
  category: "my_category"
  interval_hours: 6
  selectors:
    items: "div.item"
    title: "h2.title"
    url: "h2.title a"
    author: "span.author"
    description: "p.summary"
```

### Step 2 — Test it

```bash
python main.py --mode single --source my_new_source
```

Check the output in `logs/pipeline.log` and query the database:

```sql
SELECT title, url, category FROM clean_records
WHERE source_name = 'My New Source'
ORDER BY ingested_at DESC LIMIT 10;
```

### Step 3 — For API sources

Set `scraper_type: "api"`. The `APIScraper` in `source_registry.py` will
call the URL and attempt best-effort JSON field mapping. If the API schema
is unusual, subclass `APIScraper` and override `_parse_json()`.

### Step 4 — For Scrapy sources

1. Write a new Spider class in `scrapers/scrapy_spider.py`
2. Register it in the `SPIDER_REGISTRY` dict at the bottom of that file
3. Set `scraper_type: scrapy` and `spider_class: YourSpiderClassName` in YAML
4. Run via `run_scrapy_spider()` directly (not through the scheduler, to
   avoid Twisted reactor conflicts with the main process)

---

## 5. Database Maintenance Runbook

### Check ingestion health

```sql
-- Latest batch per source
SELECT
    source_name,
    status,
    records_raw,
    records_clean,
    records_failed,
    finished_at
FROM ingestion_log
ORDER BY finished_at DESC
LIMIT 20;

-- Sources with failures in last 24h
SELECT source_name, error_message, finished_at
FROM ingestion_log
WHERE status != 'success'
  AND finished_at > NOW() - INTERVAL '24 hours';
```

### Monitor growth

```sql
-- Record counts per source
SELECT * FROM vw_source_stats;

-- Top items per category
SELECT source_name, title, score, rank_in_category
FROM vw_top_items_per_category
WHERE rank_in_category <= 3
ORDER BY category, rank_in_category;
```

### Purge old data

```sql
-- Delete clean_records older than 90 days (keep audit log)
DELETE FROM clean_records
WHERE ingested_at < NOW() - INTERVAL '90 days';

-- Vacuum after large deletes
VACUUM ANALYZE clean_records;
```

### Rebuild views

If transformer views become stale, re-run via Python:

```python
from db.connection import DatabasePool
from etl.transformer import run_transformations

pool = DatabasePool()
run_transformations(pool)
```

---

## 6. Scheduler Job Reference

| Source key | Interval | Type |
|---|---|---|
| `hacker_news` | 1h | BS4 |
| `books_to_scrape` | 6h | BS4 |
| `quotes_to_scrape` | 12h | BS4 |
| `arxiv_cs` | 12h | BS4 |
| `github_trending` | 6h | BS4 |
| `python_insider` | 12h | BS4 |
| `wikipedia_trending` | 24h | BS4 |
| `countries_api` | 48h | API |
| `openlibrary` | 24h | API |
| `worldbank` | 48h | API |
| `toscrape_jobs` | — | Scrapy (manual) |

### Adjusting intervals

Edit `interval_hours` for the source in `config/sources.yaml`.
No code changes required — the scheduler reads this file at startup.

### Running a job manually

```bash
python main.py --mode single --source hacker_news
```

---

## 7. ETL Stage Reference

### Cleaner (`etl/cleaner.py`)

| Function | Purpose |
|---|---|
| `clean_text()` | Strip HTML, fix entities, collapse whitespace |
| `normalise_url()` | Resolve relative URLs, canonicalise |
| `parse_datetime()` | Fuzzy date parsing → UTC datetime |
| `record_fingerprint()` | SHA-1 for deduplication |
| `clean_record()` | Full single-record cleaning |
| `clean_batch()` | Batch clean + deduplicate |

### Validator (`etl/validator.py`)

The `CleanRecord` Pydantic model enforces:

- `title`: required, min 1 char, max 500
- `url`: optional, max 2000 chars
- `tags`: list, capped at 20, each ≤ 100 chars
- `score`/`price`: coerced to float, negative → None
- At least one of url or title must be present

### Transformer SQL passes

1. **Category backfill** — fills blank `category` using source_name lookup
2. **Score normalisation** — min-max scales scores per source to `[0, 1]`
3. **Top items view** — `vw_top_items_per_category` (top 10 per category)
4. **Source stats view** — `vw_source_stats` (counts, averages, last run)

---

## 8. Troubleshooting & FAQ

### `psycopg2.OperationalError: could not connect to server`

Check: Is PostgreSQL running? Are the credentials in `.env` correct?

```bash
psql -h localhost -U postgres -d scraping_pipeline
```

### `ModuleNotFoundError` on import

Ensure you're running from the project root with the virtual environment
activated:

```bash
source venv/bin/activate
python main.py --mode batch
```

### Records not appearing in clean_records

A record may be silently skipped if:
- The title is empty or matches a junk pattern (N/A, null, —)
- The fingerprint already exists (deduplication)
- Pydantic validation failed — check `ingestion_log.records_failed`

```sql
SELECT * FROM ingestion_log ORDER BY finished_at DESC LIMIT 5;
```

### Scrapy `ReactorNotRestartable` error

Scrapy's Twisted reactor can only be started once per process. Run Scrapy
sources in a subprocess or separate process from the APScheduler loop.

### Rate limiting / 429 responses

Increase `REQUEST_DELAY` in `.env` (e.g. `REQUEST_DELAY=3.0`) and reduce
`CONCURRENT_REQUESTS`. Scrapy sources respect `AUTOTHROTTLE_ENABLED=True`.

---

## 9. Contributing Guidelines

1. **Branch naming**: `feature/source-name`, `fix/issue-description`
2. **Tests**: All new scrapers must include at least one `responses`-mocked test
3. **Selectors**: CSS selectors preferred over XPath for readability
4. **Logging**: Use `self.logger` (from `BaseScraper`) — no bare `print()`
5. **Secrets**: Never commit real credentials — use `.env` (gitignored)
6. **PR checklist**:
   - [ ] New source added to `config/sources.yaml`
   - [ ] Unit test added to `tests/test_scraper.py`
   - [ ] WIKI.md updated if architecture changed
   - [ ] `pytest tests/ -v` passes locally

---

*Last updated: 2025 | Maintained by Krish Bakriwala*
