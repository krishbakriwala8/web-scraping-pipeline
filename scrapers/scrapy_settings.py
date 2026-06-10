"""
scrapers/scrapy_settings.py
============================
Scrapy project settings loaded by CrawlerProcess.
Mirrors values from config/settings.py for the Scrapy engine.
"""
from config.settings import ScrapySettings, PipelineSettings

BOT_NAME = "scraping_pipeline"
SPIDER_MODULES = ["scrapers"]
NEWSPIDER_MODULE = "scrapers"

USER_AGENT = ScrapySettings.USER_AGENT
ROBOTSTXT_OBEY = ScrapySettings.ROBOTSTXT_OBEY
DOWNLOAD_DELAY = ScrapySettings.DOWNLOAD_DELAY
CONCURRENT_REQUESTS = PipelineSettings.CONCURRENT_REQUESTS
CONCURRENT_REQUESTS_PER_DOMAIN = ScrapySettings.CONCURRENT_REQUESTS_PER_DOMAIN

AUTOTHROTTLE_ENABLED = ScrapySettings.AUTOTHROTTLE_ENABLED
AUTOTHROTTLE_START_DELAY = ScrapySettings.AUTOTHROTTLE_START_DELAY
AUTOTHROTTLE_MAX_DELAY = ScrapySettings.AUTOTHROTTLE_MAX_DELAY
AUTOTHROTTLE_TARGET_CONCURRENCY = 2.0

HTTPCACHE_ENABLED = ScrapySettings.HTTPCACHE_ENABLED
LOG_LEVEL = ScrapySettings.LOG_LEVEL

# Retry middleware
RETRY_ENABLED = True
RETRY_TIMES = PipelineSettings.MAX_RETRIES
RETRY_HTTP_CODES = [500, 502, 503, 504, 429]

# Default request headers
DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en",
}

REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"
