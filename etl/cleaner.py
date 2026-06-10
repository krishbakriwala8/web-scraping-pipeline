"""
etl/cleaner.py
==============
Data cleaning and normalisation stage of the ETL pipeline.

Receives raw RawRecord objects from the scraper layer and returns
cleaned dictionaries ready for schema validation.

Responsibilities:
  - Strip residual HTML tags from text fields
  - Normalise whitespace and encoding artefacts
  - Parse and standardise date/time strings
  - Normalise URLs (absolute, no trailing slash)
  - Deduplicate records within a batch by URL
  - Remove empty or junk records
"""
import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse, urlunparse

from dateutil import parser as dateutil_parser

from etl.parser import strip_html_tags, parse_score, parse_tags
from scrapers.base_scraper import RawRecord

logger = logging.getLogger(__name__)

# Patterns for detecting junk titles
_JUNK_PATTERNS = re.compile(
    r"^(n/?a|none|null|undefined|untitled|[–—\-]+|\.+)$", re.IGNORECASE
)


def clean_text(value: str) -> str:
    """Strip HTML, normalise unicode, collapse whitespace."""
    if not value:
        return ""
    text = strip_html_tags(value)
    # Fix common HTML entities that BeautifulSoup may not always decode
    text = (
        text.replace("&amp;", "&")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", '"')
            .replace("&#x27;", "'")
            .replace("\u200b", "")   # zero-width space
            .replace("\xa0", " ")    # non-breaking space
    )
    return re.sub(r"\s+", " ", text).strip()


def normalise_url(url: str) -> str:
    """Return a canonicalised absolute URL, or empty string if invalid."""
    if not url:
        return ""
    try:
        parsed = urlparse(url)
        if not parsed.scheme:
            url = "https://" + url
            parsed = urlparse(url)
        if not parsed.netloc:
            return ""
        # Drop fragment, strip trailing slash from path
        path = parsed.path.rstrip("/") or "/"
        return urlunparse((parsed.scheme, parsed.netloc, path,
                           parsed.params, parsed.query, ""))
    except Exception:
        return ""


def parse_datetime(raw: str) -> datetime | None:
    """
    Parse a date string into a UTC datetime.
    Returns None if parsing fails.
    """
    if not raw:
        return None
    try:
        dt = dateutil_parser.parse(raw, fuzzy=True)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def record_fingerprint(url: str, title: str) -> str:
    """SHA-1 fingerprint for deduplication — URL takes priority over title."""
    key = normalise_url(url) or clean_text(title)
    return hashlib.sha1(key.encode()).hexdigest()


def clean_record(raw: RawRecord) -> dict[str, Any] | None:
    """
    Clean a single RawRecord and return a dict ready for validation.
    Returns None if the record should be discarded (empty, junk, etc.).
    """
    title = clean_text(raw.title)
    if not title or _JUNK_PATTERNS.match(title):
        logger.debug("Discarding junk/empty title from %s", raw.source_name)
        return None

    url = normalise_url(raw.url)
    description = clean_text(raw.description)
    author = clean_text(raw.author)
    tags = parse_tags(raw.tags)
    score = parse_score(raw.score)

    extra = raw.extra or {}
    price_raw = extra.get("price", "")
    price = None
    if price_raw:
        m = re.search(r"[\d.,]+", str(price_raw).replace(",", ""))
        price = float(m.group()) if m else None

    return {
        "source_name": raw.source_name,
        "title": title[:500],               # enforce field length limits
        "url": url[:2000],
        "description": description[:2000],
        "author": author[:255],
        "category": extra.get("category", ""),
        "tags": tags[:20],
        "score": score,
        "price": price,
        "published_at": None,               # enriched later if available
        "ingested_at": datetime.utcnow(),
        "batch_id": raw.batch_id,
        "fingerprint": record_fingerprint(url, title),
    }


def clean_batch(records: list[RawRecord]) -> list[dict[str, Any]]:
    """
    Clean an entire batch of RawRecords.
    Deduplicates within the batch by fingerprint.
    Returns a list of clean dicts ready for the validation stage.
    """
    seen_fingerprints: set[str] = set()
    cleaned: list[dict[str, Any]] = []

    for raw in records:
        try:
            result = clean_record(raw)
        except Exception as exc:
            logger.warning("clean_record raised for %s: %s", raw.source_name, exc)
            continue

        if result is None:
            continue

        fp = result["fingerprint"]
        if fp in seen_fingerprints:
            logger.debug("Dedup: skipping duplicate '%s'", result["title"][:60])
            continue

        seen_fingerprints.add(fp)
        cleaned.append(result)

    logger.info(
        "Cleaning complete: %d raw → %d clean records",
        len(records), len(cleaned),
    )
    return cleaned
