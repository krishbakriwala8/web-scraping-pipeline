# 🕷️ Web Scraping Data Pipeline — Batch Ingestion System

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python)](https://www.python.org/)
[![Scrapy](https://img.shields.io/badge/Scrapy-2.11-green?logo=scrapy)](https://scrapy.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue?logo=postgresql)](https://www.postgresql.org/)
[![BeautifulSoup](https://img.shields.io/badge/BeautifulSoup-4.12-orange)](https://www.crummy.com/software/BeautifulSoup/)
[![License](https://img.shields.io/badge/License-MIT-lightgrey)](LICENSE)

An end-to-end, production-grade web scraping and batch data ingestion pipeline built with **BeautifulSoup**, **Scrapy**, **PostgreSQL**, and **APScheduler**. Supports 10+ configurable data sources with modular scraper architecture, ETL orchestration, schema validation, and automated scheduling.

---

## 📐 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                    BATCH INGESTION PIPELINE                         │
│                                                                     │
│  ┌───────────────┐    ┌─────────────────┐    ┌──────────────────┐  │
│  │  Data Sources │    │  Scraper Layer  │    │   ETL Pipeline   │  │
│  │               │    │                 │    │                  │  │
│  │ • News Sites  │───▶│ BeautifulSoup   │───▶│  HTML Parsing    │  │
│  │ • Job Boards  │    │ Static Scrapers │    │  Data Cleaning   │  │
│  │ • E-commerce  │    │                 │    │  Schema Valid.   │  │
│  │ • Research    │───▶│ Scrapy Spiders  │───▶│  SQL Transform   │  │
│  │ • APIs/RSS    │    │ Dynamic Scrapers│    │                  │  │
│  └───────────────┘    └─────────────────┘    └────────┬─────────┘  │
│                                                        │            │
│  ┌─────────────────────────────┐           ┌──────────▼──────────┐ │
│  │     APScheduler (Cron)      │           │   PostgreSQL DB      │ │
│  │  • Batch Job Orchestration  │           │  • Raw Data Tables   │ │
│  │  • Retry & Failure Handling │           │  • Cleaned Tables    │ │
│  │  • Source-level Intervals   │           │  • Aggregated Views  │ │
│  └─────────────────────────────┘           └─────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
web-scraping-pipeline/
│
├── scrapers/                    # Scraper layer
│   ├── __init__.py
│   ├── base_scraper.py          # Abstract base class for all scrapers
│   ├── bs4_scraper.py           # BeautifulSoup static page scrapers
│   ├── scrapy_spider.py         # Scrapy spider definitions
│   ├── scrapy_settings.py       # Scrapy engine configuration
│   └── source_registry.py      # Registry of all 10+ data sources
│
├── etl/                         # ETL pipeline
│   ├── __init__.py
│   ├── parser.py                # HTML parsing & field extraction
│   ├── cleaner.py               # Data cleaning & normalisation
│   ├── validator.py             # Schema validation (Pydantic)
│   └── transformer.py           # SQL-based transformations
│
├── db/                          # Database layer
│   ├── __init__.py
│   ├── connection.py            # PostgreSQL connection pool (psycopg2)
│   ├── models.py                # Table definitions & DDL
│   └── loader.py                # Bulk insert & upsert logic
│
├── scheduler/                   # Batch job scheduling
│   ├── __init__.py
│   └── batch_scheduler.py       # APScheduler job definitions
│
├── config/                      # Configuration
│   ├── sources.yaml             # Data source definitions (10+ sources)
│   └── settings.py              # App-wide settings (env-driven)
│
├── tests/                       # Unit & integration tests
│   ├── __init__.py
│   ├── test_scraper.py
│   ├── test_etl.py
│   └── test_db.py
│
├── docs/                        # Confluence-style wiki
│   └── WIKI.md
│
├── logs/                        # Log output directory
│
├── main.py                      # Pipeline entrypoint
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

---

## 🚀 Quick Start

### 1. Clone & Install

```bash
git clone https://github.com/krishbakriwala8/web-scraping-pipeline.git
cd web-scraping-pipeline

python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your PostgreSQL credentials
```

### 3. Initialize Database

```bash
python -c "from db.models import init_db; init_db()"
```

### 4. Run Pipeline

```bash
# Run all scrapers once (batch mode)
python main.py --mode batch

# Run scheduler (continuous / cron mode)
python main.py --mode schedule

# Run a single source
python main.py --mode single --source hacker_news
```

---

## ⚙️ Data Sources (10+)

| # | Source | Type | Scraper | Interval |
|---|--------|------|---------|----------|
| 1 | Hacker News | Tech News | BeautifulSoup | 1h |
| 2 | Books to Scrape | E-commerce | BeautifulSoup | 6h |
| 3 | Quotes to Scrape | Text/NLP | BeautifulSoup | 12h |
| 4 | Wikipedia (trending) | Knowledge | BeautifulSoup | 24h |
| 5 | ArXiv CS papers | Research | BeautifulSoup | 12h |
| 6 | GitHub Trending | Dev | BeautifulSoup | 6h |
| 7 | Toscrape Jobs | Jobs | Scrapy | 3h |
| 8 | Countries REST API | Geo/Data | Requests | 24h |
| 9 | OpenLibrary | Books | Requests | 24h |
| 10 | Python Insider Blog | Tech Blog | BeautifulSoup | 12h |
| 11 | WorldBank Open Data | Economics | Requests | 48h |

---

## 🗄️ Database Schema

```sql
-- Raw ingested data (append-only)
CREATE TABLE raw_scrape_data (
    id          SERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL,
    url         TEXT NOT NULL,
    raw_html    TEXT,
    scraped_at  TIMESTAMP DEFAULT NOW(),
    batch_id    UUID NOT NULL
);

-- Cleaned & validated records
CREATE TABLE clean_records (
    id           SERIAL PRIMARY KEY,
    source_name  VARCHAR(100),
    title        TEXT,
    url          TEXT UNIQUE,
    description  TEXT,
    author       VARCHAR(255),
    category     VARCHAR(100),
    tags         TEXT[],
    score        NUMERIC,
    published_at TIMESTAMP,
    ingested_at  TIMESTAMP DEFAULT NOW(),
    batch_id     UUID
);

-- Per-batch ingestion audit log
CREATE TABLE ingestion_log (
    id           SERIAL PRIMARY KEY,
    batch_id     UUID NOT NULL,
    source_name  VARCHAR(100),
    records_raw  INT,
    records_clean INT,
    records_failed INT,
    started_at   TIMESTAMP,
    finished_at  TIMESTAMP,
    status       VARCHAR(20)
);
```

---

## 🔄 ETL Flow

```
Raw HTML/JSON
     │
     ▼
[parser.py]        ← Field extraction using CSS selectors / XPath
     │
     ▼
[cleaner.py]       ← Strip HTML, normalise whitespace, parse dates, deduplicate
     │
     ▼
[validator.py]     ← Pydantic schema enforcement, type coercion, error capture
     │
     ▼
[transformer.py]   ← SQL-level enrichment: category tagging, score normalisation
     │
     ▼
[loader.py]        ← Bulk COPY / upsert into PostgreSQL, batch audit logging
```

---

## 📖 Documentation

See [`docs/WIKI.md`](docs/WIKI.md) for full Confluence-style team wiki including:
- Developer onboarding guide
- Adding a new scraper source
- Database maintenance runbook
- Scheduler job reference
- Troubleshooting & FAQ

---

## 🧪 Tests

```bash
pytest tests/ -v --tb=short
```

---

## 📜 License

MIT © Krish Bakriwala
