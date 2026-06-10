"""
scrapers/scrapy_spider.py
=========================
Scrapy-based spider for dynamic, multi-page crawls.
Uses CrawlerProcess to run spiders programmatically from the pipeline.
Items are collected via a custom pipeline into a shared list.
"""
import logging
import uuid
from datetime import datetime

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from config.settings import ScrapySettings, PipelineSettings
from scrapers.base_scraper import RawRecord, ScrapeResult

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Item Pipeline — collects scraped items into a Python list
# ---------------------------------------------------------------------------

class CollectorPipeline:
    """Scrapy item pipeline that accumulates items into a shared list."""

    def __init__(self, collector: list):
        self._collector = collector

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings.get("ITEM_COLLECTOR"))

    def process_item(self, item, spider):
        self._collector.append(dict(item))
        return item


# ---------------------------------------------------------------------------
# Scrapy Spiders
# ---------------------------------------------------------------------------

class QuotesSpider(scrapy.Spider):
    """
    Example Scrapy spider for quotes.toscrape.com.
    Demonstrates multi-page crawling with Scrapy's built-in follow-link mechanism.
    """
    name = "quotes_spider"
    start_urls = ["https://quotes.toscrape.com/"]

    custom_settings = {
        "ROBOTSTXT_OBEY": True,
        "DOWNLOAD_DELAY": PipelineSettings.REQUEST_DELAY,
        "LOG_LEVEL": "WARNING",
        "AUTOTHROTTLE_ENABLED": True,
    }

    def parse(self, response):
        for quote in response.css("div.quote"):
            yield {
                "title": quote.css("span.text::text").get(default="").strip(),
                "author": quote.css("small.author::text").get(default="").strip(),
                "tags": quote.css("div.tags a.tag::text").getall(),
                "url": response.url,
            }

        # Follow pagination links automatically
        next_page = response.css("li.next a::attr(href)").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)


class BooksSpider(scrapy.Spider):
    """
    Scrapy spider for books.toscrape.com.
    Demonstrates extracting structured product data across paginated catalogue.
    """
    name = "books_spider"
    start_urls = ["https://books.toscrape.com/catalogue/page-1.html"]

    custom_settings = {
        "ROBOTSTXT_OBEY": True,
        "DOWNLOAD_DELAY": PipelineSettings.REQUEST_DELAY,
        "LOG_LEVEL": "WARNING",
    }

    _RATING_MAP = {
        "One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5
    }

    def parse(self, response):
        for book in response.css("article.product_pod"):
            rating_class = book.css("p.star-rating::attr(class)").get("")
            rating_word = rating_class.replace("star-rating", "").strip()
            yield {
                "title": book.css("h3 > a::attr(title)").get(default="").strip(),
                "price": book.css("p.price_color::text").get(default="").strip(),
                "rating": self._RATING_MAP.get(rating_word, 0),
                "url": response.urljoin(
                    book.css("h3 > a::attr(href)").get(default="")
                ),
                "author": "",
                "tags": [],
            }

        next_page = response.css("li.next a::attr(href)").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)


# ---------------------------------------------------------------------------
# Scrapy Runner — programmatic execution from the pipeline
# ---------------------------------------------------------------------------

SPIDER_REGISTRY = {
    "QuotesSpider": QuotesSpider,
    "BooksSpider": BooksSpider,
}


def run_scrapy_spider(source_config: dict) -> ScrapeResult:
    """
    Run a named Scrapy spider programmatically.
    Returns a ScrapeResult compatible with the rest of the pipeline.
    """
    spider_class_name = source_config.get("spider_class", "QuotesSpider")
    spider_class = SPIDER_REGISTRY.get(spider_class_name)
    if spider_class is None:
        raise ValueError(f"Unknown spider class: {spider_class_name}")

    source_name = source_config["name"]
    batch_id = str(uuid.uuid4())
    collector: list[dict] = []
    started_at = datetime.utcnow()

    settings = get_project_settings()
    settings.setmodule("scrapers.scrapy_settings")
    settings.set("ITEM_COLLECTOR", collector)
    settings.set(
        "ITEM_PIPELINES",
        {"scrapers.scrapy_spider.CollectorPipeline": 300},
    )

    process = CrawlerProcess(settings)
    process.crawl(spider_class)
    process.start()  # Blocks until all spiders finish

    finished_at = datetime.utcnow()

    records = [
        RawRecord(
            source_name=source_name,
            url=item.get("url", ""),
            title=item.get("title", ""),
            author=item.get("author", ""),
            tags=item.get("tags", []),
            score=str(item.get("rating", "")),
            batch_id=batch_id,
            extra={k: v for k, v in item.items()
                   if k not in ("title", "url", "author", "tags")},
        )
        for item in collector
    ]

    logger.info(
        "Scrapy spider %s completed: %d records in %.2fs",
        spider_class_name, len(records),
        (finished_at - started_at).total_seconds(),
    )

    return ScrapeResult(
        source_name=source_name,
        batch_id=batch_id,
        records=records,
        started_at=started_at,
        finished_at=finished_at,
        success=True,
    )
