"""
etl/parser.py
=============
HTML parsing utilities used after raw HTML has been fetched.
Provides helper functions for field extraction that scrapers can call,
and post-processing for raw records before they enter the cleaning stage.
"""
import logging
import re
from urllib.parse import urlparse, urljoin

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


def extract_text(html: str, selector: str) -> str:
    """Extract text from an HTML string using a CSS selector."""
    soup = BeautifulSoup(html, "lxml")
    el = soup.select_one(selector)
    return el.get_text(strip=True) if el else ""


def extract_links(html: str, base_url: str = "") -> list[str]:
    """
    Extract all <a href> links from an HTML string.
    Resolves relative URLs if base_url is provided.
    """
    soup = BeautifulSoup(html, "lxml")
    links = []
    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        if href.startswith("javascript:") or href == "#":
            continue
        if base_url and not href.startswith(("http://", "https://")):
            href = urljoin(base_url, href)
        links.append(href)
    return links


def strip_html_tags(text: str) -> str:
    """Remove all HTML tags from a string, preserving inner text."""
    clean = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", clean).strip()


def extract_domain(url: str) -> str:
    """Return the registered domain from a URL (e.g. 'github.com')."""
    try:
        return urlparse(url).netloc.lstrip("www.")
    except Exception:
        return ""


def parse_score(raw: str) -> float | None:
    """
    Parse a score string into a float.
    Handles common formats: '1,234 points', '42', '3.5k', etc.
    """
    if not raw:
        return None
    raw = raw.lower().replace(",", "").strip()
    match = re.search(r"[\d.]+", raw)
    if not match:
        return None
    value = float(match.group())
    if "k" in raw:
        value *= 1000
    elif "m" in raw:
        value *= 1_000_000
    return round(value, 2)


def parse_tags(raw_tags: list[str]) -> list[str]:
    """Normalise a list of raw tag strings: lowercase, strip, deduplicate."""
    seen = set()
    result = []
    for tag in raw_tags:
        normalised = tag.strip().lower().replace(" ", "-")
        if normalised and normalised not in seen:
            seen.add(normalised)
            result.append(normalised)
    return result
