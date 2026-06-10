"""
tests/test_etl.py
=================
Unit tests for the ETL pipeline stages: parser, cleaner, and validator.
"""
import pytest

from etl.cleaner import (
    clean_text,
    normalise_url,
    parse_datetime,
    record_fingerprint,
    clean_record,
    clean_batch,
)
from etl.parser import (
    strip_html_tags,
    extract_links,
    parse_score,
    parse_tags,
    extract_domain,
)
from etl.validator import CleanRecord, validate_batch
from scrapers.base_scraper import RawRecord


# ---------------------------------------------------------------------------
# Parser tests
# ---------------------------------------------------------------------------

def test_strip_html_tags():
    assert strip_html_tags("<b>Hello</b> <i>World</i>") == "Hello World"
    assert strip_html_tags("No tags here") == "No tags here"
    assert strip_html_tags("") == ""


def test_extract_links():
    html = '<a href="/page1">P1</a> <a href="https://example.com">E</a>'
    links = extract_links(html, base_url="https://site.com")
    assert "https://site.com/page1" in links
    assert "https://example.com" in links


def test_extract_links_skips_javascript():
    html = '<a href="javascript:void(0)">JS</a> <a href="#">Hash</a>'
    links = extract_links(html)
    assert links == []


def test_parse_score_plain_number():
    assert parse_score("42") == 42.0


def test_parse_score_with_label():
    assert parse_score("1,234 points") == 1234.0


def test_parse_score_k_suffix():
    assert parse_score("3.5k") == 3500.0


def test_parse_score_empty():
    assert parse_score("") is None
    assert parse_score(None) is None


def test_parse_tags_dedup_and_normalise():
    raw = ["Python", "python", "  Machine Learning  ", "AI"]
    result = parse_tags(raw)
    assert result.count("python") == 1
    assert "machine-learning" in result
    assert "ai" in result


def test_extract_domain():
    assert extract_domain("https://www.github.com/trending") == "github.com"
    assert extract_domain("https://arxiv.org/abs/123") == "arxiv.org"
    assert extract_domain("") == ""


# ---------------------------------------------------------------------------
# Cleaner tests
# ---------------------------------------------------------------------------

def test_clean_text_strips_html():
    assert clean_text("<p>Hello &amp; World</p>") == "Hello & World"


def test_clean_text_handles_empty():
    assert clean_text("") == ""
    assert clean_text(None) == ""


def test_normalise_url_relative():
    assert normalise_url("example.com/page") == "https://example.com/page"


def test_normalise_url_trailing_slash():
    assert normalise_url("https://example.com/page/") == "https://example.com/page"


def test_normalise_url_empty():
    assert normalise_url("") == ""


def test_parse_datetime_iso():
    dt = parse_datetime("2024-06-15T10:30:00")
    assert dt is not None
    assert dt.year == 2024
    assert dt.month == 6


def test_parse_datetime_fuzzy():
    dt = parse_datetime("June 15, 2024")
    assert dt is not None
    assert dt.month == 6


def test_parse_datetime_invalid():
    assert parse_datetime("not a date") is None
    assert parse_datetime("") is None


def test_clean_record_valid():
    raw = RawRecord(
        source_name="Hacker News",
        url="https://example.com/story",
        title="<b>A Great Story</b>",
        author="  John Doe  ",
        tags=["Tech", "tech", "AI"],
        score="142 points",
        batch_id="test-batch",
    )
    result = clean_record(raw)
    assert result is not None
    assert result["title"] == "A Great Story"
    assert result["author"] == "John Doe"
    assert result["score"] == 142.0
    assert result["tags"].count("tech") == 1


def test_clean_record_discards_empty_title():
    raw = RawRecord(
        source_name="Test",
        url="https://example.com",
        title="",
        batch_id="test-batch",
    )
    assert clean_record(raw) is None


def test_clean_record_discards_junk_title():
    for junk in ["N/A", "n/a", "null", "—", "-", "..."]:
        raw = RawRecord(
            source_name="Test",
            url="https://example.com",
            title=junk,
            batch_id="test-batch",
        )
        assert clean_record(raw) is None, f"Expected None for junk title: {junk!r}"


def test_clean_batch_deduplicates():
    records = [
        RawRecord(source_name="Test", url="https://example.com/1",
                  title="Title A", batch_id="b1"),
        RawRecord(source_name="Test", url="https://example.com/1",
                  title="Title A", batch_id="b1"),   # duplicate
        RawRecord(source_name="Test", url="https://example.com/2",
                  title="Title B", batch_id="b1"),
    ]
    result = clean_batch(records)
    assert len(result) == 2


def test_record_fingerprint_consistent():
    fp1 = record_fingerprint("https://example.com/page", "Title")
    fp2 = record_fingerprint("https://example.com/page", "Title")
    assert fp1 == fp2


# ---------------------------------------------------------------------------
# Validator tests
# ---------------------------------------------------------------------------

def _make_clean_dict(**overrides) -> dict:
    base = {
        "source_name": "Test Source",
        "title": "A Valid Title",
        "url": "https://example.com/article",
        "description": "Some description",
        "author": "Jane Doe",
        "category": "tech_news",
        "tags": ["python", "ai"],
        "score": 9.5,
        "price": None,
        "published_at": None,
        "batch_id": "batch-abc-123",
        "fingerprint": "a" * 40,
    }
    base.update(overrides)
    return base


def test_validate_batch_all_valid():
    records = [_make_clean_dict(), _make_clean_dict(title="Another Title", fingerprint="b" * 40)]
    result = validate_batch(records)
    assert result.valid_count == 2
    assert result.invalid_count == 0


def test_validate_batch_catches_missing_title():
    records = [_make_clean_dict(title="")]
    result = validate_batch(records)
    assert result.invalid_count == 1
    assert result.valid_count == 0


def test_validate_batch_coerces_score():
    records = [_make_clean_dict(score="7.5")]
    result = validate_batch(records)
    assert result.valid_count == 1
    assert result.valid[0].score == 7.5


def test_validate_batch_rejects_negative_score():
    records = [_make_clean_dict(score=-1)]
    result = validate_batch(records)
    # Negative score is coerced to None — record is still valid
    assert result.valid_count == 1
    assert result.valid[0].score is None


def test_validate_batch_truncates_tags():
    many_tags = [f"tag{i}" for i in range(50)]
    records = [_make_clean_dict(tags=many_tags)]
    result = validate_batch(records)
    assert result.valid_count == 1
    assert len(result.valid[0].tags) <= 20


def test_clean_record_model_fields():
    data = _make_clean_dict()
    record = CleanRecord.model_validate(data)
    assert record.source_name == "Test Source"
    assert record.title == "A Valid Title"
    assert record.category == "tech_news"
