"""
scrapers/source_registry.py
============================
Factory and registry for all configured data sources.
Loads source definitions from config/sources.yaml and instantiates
the correct scraper type (bs4, scrapy, api) for each.
"""
import logging
from typing import Iterator

import yaml

from config.settings import PipelineSettings
from scrapers.base_scraper import BaseScraper, RawRecord, ScrapeResult
from scrapers.bs4_scraper import BS4Scraper

logger = logging.getLogger(__name__)


class APIScraper(BaseScraper):
    """
    Lightweight scraper for REST API sources that return JSON.
    No HTML parsing — direct JSON key extraction.
    """

    def scrape(self) -> list[RawRecord]:
        import requests

        try:
            response = requests.get(
                self.url,
                timeout=PipelineSettings.REQUEST_TIMEOUT,
                headers={"Accept": "application/json"},
            )
            response.raise_for_status()
            data = response.json()
        except Exception as exc:
            self.logger.error("API fetch failed for %s: %s", self.source_name, exc)
            return []

        return self._parse_json(data)

    def _parse_json(self, data) -> list[RawRecord]:
        """
        Flatten JSON responses from various REST APIs into RawRecord format.
        Each API source has its own structure; we normalise best-effort.
        """
        records = []

        # Handle list-of-dicts (e.g. restcountries, worldbank index 1)
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            # WorldBank wraps data at index 1
            items = data.get("works", data.get("docs", [data]))
            if isinstance(data, dict) and len(data) == 2 and isinstance(list(data.values())[1], list):
                items = list(data.values())[1]
        else:
            return records

        for item in items[:100]:
            if not isinstance(item, dict):
                continue

            # Best-effort field mapping across different API schemas
            title = (
                item.get("name", {}).get("common")
                or item.get("name", {}).get("official")
                or item.get("title")
                or item.get("subject")
                or item.get("value", "")
                or str(item.get("id", ""))
            )
            if not title:
                continue

            records.append(RawRecord(
                source_name=self.source_name,
                url=item.get("url", item.get("wikipedia", self.url)),
                title=str(title),
                description=str(item.get("description", item.get("note", ""))),
                author=str(item.get("authors", item.get("country", ""))),
                tags=[item.get("region", item.get("category", ""))],
                score=str(item.get("population", item.get("value", ""))),
                batch_id=self.batch_id,
                extra=item,
            ))

        return records


def load_sources() -> dict:
    """Load all source definitions from sources.yaml."""
    with open(PipelineSettings.SOURCES_CONFIG, "r") as f:
        config = yaml.safe_load(f)
    return config.get("sources", {})


def get_scraper(source_name: str) -> BaseScraper | None:
    """Instantiate and return the scraper for a given source name."""
    sources = load_sources()
    if source_name not in sources:
        logger.error("Unknown source: %s", source_name)
        return None

    source_config = sources[source_name]
    scraper_type = source_config.get("scraper_type", "bs4")

    if scraper_type == "bs4":
        return BS4Scraper(source_config)
    elif scraper_type == "api":
        return APIScraper(source_config)
    elif scraper_type == "scrapy":
        # Scrapy runs via run_scrapy_spider — not wrapped in BaseScraper here
        # to avoid Twisted reactor conflicts in the main process.
        return None
    else:
        logger.warning("Unknown scraper type '%s' for source %s", scraper_type, source_name)
        return None


def iter_all_scrapers() -> Iterator[tuple[str, BaseScraper | None]]:
    """
    Yield (source_name, scraper) pairs for every configured source.
    Scrapy sources yield (name, None) — caller handles them separately.
    """
    sources = load_sources()
    for name, config in sources.items():
        if config.get("scraper_type") == "scrapy":
            yield name, None
        else:
            yield name, get_scraper(name)


def run_all_scrapers() -> list[ScrapeResult]:
    """
    Run every non-Scrapy source sequentially.
    Returns a list of ScrapeResult objects for downstream ETL.
    """
    results: list[ScrapeResult] = []

    for source_name, scraper in iter_all_scrapers():
        if scraper is None:
            logger.info("Skipping Scrapy source '%s' (run separately)", source_name)
            continue
        result = scraper.run()
        results.append(result)

    return results
