"""
scrapers/bs4_scraper.py
=======================
BeautifulSoup-based scrapers for static HTML pages.
Handles paginated sources, retry on failure, and configurable CSS-selector
extraction using source definitions from config/sources.yaml.
"""
import logging
import time
from typing import Any

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config.settings import PipelineSettings
from scrapers.base_scraper import BaseScraper, RawRecord

logger = logging.getLogger(__name__)

# Initialise once — rotating user-agent for every request
_UA = UserAgent(fallback="Mozilla/5.0 (X11; Linux x86_64)")


def _get_ua() -> str:
    try:
        return _UA.random
    except Exception:
        return "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"


class BS4Scraper(BaseScraper):
    """
    Fetches HTML pages via requests and extracts structured data
    using BeautifulSoup CSS selectors defined in sources.yaml.

    Supports:
    - Single-page scraping
    - Paginated scraping (paginate: true, max_pages: N)
    - Automatic retry with exponential back-off
    - Rotating User-Agent headers
    """

    def __init__(self, source_config: dict):
        super().__init__(source_config)
        self.selectors: dict[str, str] = source_config.get("selectors", {})
        self.paginate: bool = source_config.get("paginate", False)
        self.max_pages: int = source_config.get("max_pages", 1)
        self.session = self._build_session()

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
        })
        return session

    @retry(
        stop=stop_after_attempt(PipelineSettings.MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=2, max=15),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True,
    )
    def _fetch(self, url: str) -> BeautifulSoup:
        """Download a page and return a parsed BeautifulSoup tree."""
        self.session.headers["User-Agent"] = _get_ua()
        response = self.session.get(
            url,
            timeout=PipelineSettings.REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        return BeautifulSoup(response.text, "lxml")

    def scrape(self) -> list[RawRecord]:
        pages = range(1, self.max_pages + 1) if self.paginate else [None]
        records: list[RawRecord] = []

        for page in pages:
            url = self.url.format(page=page) if page else self.url
            soup = self._fetch(url)          # raises on HTTP error → caught by BaseScraper.run()
            page_records = self._extract(soup, url)
            records.extend(page_records)
            self.logger.debug(
                "Page %s → %d records extracted from %s",
                page, len(page_records), url,
            )
            if self.paginate:
                time.sleep(PipelineSettings.REQUEST_DELAY)

        return records

    def _extract(self, soup: BeautifulSoup, page_url: str) -> list[RawRecord]:
        """
        Extract records from a parsed page using the CSS selectors
        defined for this source.
        """
        items_sel = self.selectors.get("items", "")
        if not items_sel:
            self.logger.warning("No 'items' selector configured for %s", self.source_name)
            return []

        items = soup.select(items_sel)
        records: list[RawRecord] = []

        for item in items:
            try:
                record = self._extract_single(item, page_url)
                if record:
                    records.append(record)
            except Exception as exc:
                self.logger.debug("Skipped item in %s: %s", self.source_name, exc)

        return records

    def _extract_single(self, item: Any, page_url: str) -> RawRecord | None:
        """Extract a single RawRecord from a BeautifulSoup Tag."""

        def _text(selector: str) -> str:
            if not selector:
                return ""
            el = item.select_one(selector)
            return el.get_text(strip=True) if el else ""

        def _attr(selector: str, attr: str = "href") -> str:
            if not selector:
                return ""
            el = item.select_one(selector)
            return el.get(attr, "") if el else ""

        def _all_text(selector: str) -> list[str]:
            if not selector:
                return []
            return [el.get_text(strip=True) for el in item.select(selector)]

        title = _text(self.selectors.get("title", ""))
        if not title:
            return None  # Skip items with no title

        # Resolve relative URLs
        raw_url = _attr(self.selectors.get("url", ""))
        if raw_url and raw_url.startswith("/"):
            from urllib.parse import urlparse
            parsed = urlparse(page_url)
            raw_url = f"{parsed.scheme}://{parsed.netloc}{raw_url}"

        return RawRecord(
            source_name=self.source_name,
            url=raw_url or page_url,
            title=title,
            description=_text(self.selectors.get("description", "")),
            author=_text(self.selectors.get("author", "")),
            tags=_all_text(self.selectors.get("tags", "")),
            score=_text(self.selectors.get("score", "")),
            batch_id=self.batch_id,
            extra={
                "category": self.category,
                "price": _text(self.selectors.get("price", "")),
                "rating": _text(self.selectors.get("rating", "")),
                "stars": _text(self.selectors.get("stars", "")),
            },
        )
