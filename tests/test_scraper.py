"""
tests/test_scraper.py
=====================
Unit tests for the scraper layer.

Tests:
  - BaseScraper.run() wraps exceptions into ScrapeResult
  - BS4Scraper extracts records from mocked HTML responses
  - APIScraper parses JSON responses into RawRecord objects
  - source_registry returns the correct scraper type per source
"""
import uuid
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
import responses as responses_mock

from scrapers.base_scraper import BaseScraper, RawRecord, ScrapeResult
from scrapers.bs4_scraper import BS4Scraper
from scrapers.source_registry import APIScraper, get_scraper


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def hn_config():
    return {
        "name": "Hacker News",
        "url": "https://news.ycombinator.com/",
        "scraper_type": "bs4",
        "category": "tech_news",
        "interval_hours": 1,
        "selectors": {
            "items": "tr.athing",
            "title": "span.titleline > a",
            "url": "span.titleline > a",
        },
    }


@pytest.fixture
def books_config():
    return {
        "name": "Books to Scrape",
        "url": "https://books.toscrape.com/catalogue/page-{page}.html",
        "scraper_type": "bs4",
        "category": "ecommerce",
        "interval_hours": 6,
        "paginate": True,
        "max_pages": 2,
        "selectors": {
            "items": "article.product_pod",
            "title": "h3 > a",
            "price": "p.price_color",
        },
    }


MOCK_HN_HTML = """
<html><body><table>
  <tr class="athing">
    <td><span class="titleline"><a href="https://example.com/story1">Test Story One</a></span></td>
  </tr>
  <tr class="athing">
    <td><span class="titleline"><a href="https://example.com/story2">Test Story Two</a></span></td>
  </tr>
</table></body></html>
"""

MOCK_BOOKS_HTML = """
<html><body>
  <article class="product_pod">
    <h3><a href="/catalogue/book1.html" title="A Great Book">A Great Book</a></h3>
    <p class="price_color">£12.99</p>
  </article>
  <article class="product_pod">
    <h3><a href="/catalogue/book2.html" title="Another Book">Another Book</a></h3>
    <p class="price_color">£9.99</p>
  </article>
</body></html>
"""


# ---------------------------------------------------------------------------
# BaseScraper
# ---------------------------------------------------------------------------

class _AlwaysFailScraper(BaseScraper):
    def scrape(self):
        raise RuntimeError("Intentional test failure")


class _SuccessScraper(BaseScraper):
    def scrape(self):
        return [
            RawRecord(
                source_name=self.source_name,
                url="https://example.com",
                title="Test Record",
                batch_id=self.batch_id,
            )
        ]


def test_base_scraper_captures_exception():
    config = {"name": "Test", "url": "https://example.com"}
    scraper = _AlwaysFailScraper(config)
    result = scraper.run()
    assert isinstance(result, ScrapeResult)
    assert result.success is False
    assert "Intentional test failure" in result.error_message
    assert result.records == []


def test_base_scraper_success():
    config = {"name": "Test", "url": "https://example.com"}
    scraper = _SuccessScraper(config)
    result = scraper.run()
    assert result.success is True
    assert result.record_count == 1
    assert result.records[0].title == "Test Record"


def test_scrape_result_duration():
    result = ScrapeResult(
        source_name="Test",
        batch_id=str(uuid.uuid4()),
        records=[],
        started_at=datetime(2024, 1, 1, 12, 0, 0),
        finished_at=datetime(2024, 1, 1, 12, 0, 5),
        success=True,
    )
    assert result.duration_seconds == 5.0


# ---------------------------------------------------------------------------
# BS4Scraper
# ---------------------------------------------------------------------------

@responses_mock.activate
def test_bs4_scraper_extracts_records(hn_config):
    responses_mock.add(
        responses_mock.GET,
        "https://news.ycombinator.com/",
        body=MOCK_HN_HTML,
        status=200,
        content_type="text/html",
    )
    scraper = BS4Scraper(hn_config)
    result = scraper.run()
    assert result.success is True
    assert result.record_count == 2
    titles = [r.title for r in result.records]
    assert "Test Story One" in titles
    assert "Test Story Two" in titles


@responses_mock.activate
def test_bs4_scraper_handles_http_error(hn_config):
    responses_mock.add(
        responses_mock.GET,
        "https://news.ycombinator.com/",
        status=503,
    )
    scraper = BS4Scraper(hn_config)
    result = scraper.run()
    assert result.success is False
    assert result.record_count == 0


@responses_mock.activate
def test_bs4_scraper_paginated(books_config):
    for page in [1, 2]:
        url = f"https://books.toscrape.com/catalogue/page-{page}.html"
        responses_mock.add(
            responses_mock.GET, url,
            body=MOCK_BOOKS_HTML, status=200, content_type="text/html",
        )
    scraper = BS4Scraper(books_config)
    result = scraper.run()
    assert result.success is True
    # 2 books per page × 2 pages
    assert result.record_count == 4


# ---------------------------------------------------------------------------
# APIScraper
# ---------------------------------------------------------------------------

@responses_mock.activate
def test_api_scraper_list_response():
    config = {
        "name": "REST Countries",
        "url": "https://restcountries.com/v3.1/all",
        "scraper_type": "api",
        "category": "geo_data",
        "interval_hours": 48,
    }
    responses_mock.add(
        responses_mock.GET,
        "https://restcountries.com/v3.1/all",
        json=[
            {"name": {"common": "Germany"}, "region": "Europe", "population": 83000000},
            {"name": {"common": "France"},  "region": "Europe", "population": 67000000},
        ],
        status=200,
    )
    scraper = APIScraper(config)
    result = scraper.run()
    assert result.success is True
    assert result.record_count == 2
    assert result.records[0].title == "Germany"


# ---------------------------------------------------------------------------
# Source registry
# ---------------------------------------------------------------------------

def test_get_scraper_returns_bs4_for_hn():
    scraper = get_scraper("hacker_news")
    assert scraper is not None
    assert isinstance(scraper, BS4Scraper)


def test_get_scraper_returns_none_for_scrapy():
    # Scrapy sources return None — handled separately
    scraper = get_scraper("toscrape_jobs")
    assert scraper is None


def test_get_scraper_returns_none_for_unknown():
    scraper = get_scraper("nonexistent_source_xyz")
    assert scraper is None
