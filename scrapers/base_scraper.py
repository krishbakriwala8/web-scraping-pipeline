"""
scrapers/base_scraper.py
========================
Abstract base class that every scraper must implement.
Enforces a consistent interface across BeautifulSoup, Scrapy, and API scrapers.
"""
import logging
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class RawRecord:
    """A single raw record as extracted from a data source — not yet cleaned."""
    source_name: str
    url: str
    title: str = ""
    description: str = ""
    author: str = ""
    tags: list[str] = field(default_factory=list)
    score: str = ""
    extra: dict[str, Any] = field(default_factory=dict)
    scraped_at: datetime = field(default_factory=datetime.utcnow)
    batch_id: str = field(default_factory=lambda: str(uuid.uuid4()))


@dataclass
class ScrapeResult:
    """Outcome of a single scrape run."""
    source_name: str
    batch_id: str
    records: list[RawRecord]
    started_at: datetime
    finished_at: datetime
    success: bool
    error_message: str = ""

    @property
    def duration_seconds(self) -> float:
        return (self.finished_at - self.started_at).total_seconds()

    @property
    def record_count(self) -> int:
        return len(self.records)


class BaseScraper(ABC):
    """
    Abstract base scraper.

    All scrapers (BS4, Scrapy, API) inherit from this class.
    Subclasses must implement `scrape()` and return a list of RawRecord objects.

    Usage:
        class MyScraper(BaseScraper):
            def scrape(self) -> list[RawRecord]:
                ...
    """

    def __init__(self, source_config: dict):
        self.config = source_config
        self.source_name: str = source_config["name"]
        self.url: str = source_config["url"]
        self.category: str = source_config.get("category", "general")
        self.batch_id: str = str(uuid.uuid4())
        self.logger = logging.getLogger(
            f"{__name__}.{self.__class__.__name__}"
        )

    @abstractmethod
    def scrape(self) -> list[RawRecord]:
        """
        Execute the scraping logic for this source.
        Returns a list of RawRecord objects.
        Implementations must handle their own exceptions gracefully
        and return an empty list on failure rather than raising.
        """
        ...

    def run(self) -> ScrapeResult:
        """
        Public entrypoint. Wraps `scrape()` with timing, logging,
        and error capture. Callers should always use `run()`.
        """
        started_at = datetime.utcnow()
        self.logger.info("Starting scrape: %s [batch=%s]", self.source_name, self.batch_id)
        try:
            records = self.scrape()
            finished_at = datetime.utcnow()
            self.logger.info(
                "Completed scrape: %s — %d records in %.2fs",
                self.source_name, len(records),
                (finished_at - started_at).total_seconds(),
            )
            return ScrapeResult(
                source_name=self.source_name,
                batch_id=self.batch_id,
                records=records,
                started_at=started_at,
                finished_at=finished_at,
                success=True,
            )
        except Exception as exc:
            finished_at = datetime.utcnow()
            self.logger.error("Scrape failed: %s — %s", self.source_name, exc, exc_info=True)
            return ScrapeResult(
                source_name=self.source_name,
                batch_id=self.batch_id,
                records=[],
                started_at=started_at,
                finished_at=finished_at,
                success=False,
                error_message=str(exc),
            )
