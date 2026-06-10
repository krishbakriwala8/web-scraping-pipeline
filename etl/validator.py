"""
etl/validator.py
================
Schema validation using Pydantic v2.

Every cleaned record must pass through this stage before being loaded
into PostgreSQL. Invalid records are captured with their error reasons
and written to the ingestion_log rather than discarded silently.
"""
import logging
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, HttpUrl, field_validator, model_validator

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pydantic schema for a clean record
# ---------------------------------------------------------------------------

class CleanRecord(BaseModel):
    """
    Validated, typed representation of a scraped record.
    All fields map directly to columns in the clean_records table.
    """
    source_name: str = Field(..., min_length=1, max_length=100)
    title: str = Field(..., min_length=1, max_length=500)
    url: str = Field(default="", max_length=2000)
    description: str = Field(default="", max_length=2000)
    author: str = Field(default="", max_length=255)
    category: str = Field(default="", max_length=100)
    tags: list[str] = Field(default_factory=list)
    score: float | None = None
    price: float | None = None
    published_at: datetime | None = None
    ingested_at: datetime = Field(default_factory=datetime.utcnow)
    batch_id: str = Field(..., min_length=1)
    fingerprint: str = Field(..., min_length=1)

    @field_validator("tags", mode="before")
    @classmethod
    def validate_tags(cls, v):
        if not isinstance(v, list):
            return []
        return [str(t)[:100] for t in v if t][:20]

    @field_validator("score", "price", mode="before")
    @classmethod
    def coerce_numeric(cls, v):
        if v is None:
            return None
        try:
            result = float(v)
            return None if result < 0 else result
        except (TypeError, ValueError):
            return None

    @field_validator("url", mode="before")
    @classmethod
    def allow_empty_url(cls, v):
        # URLs may be empty for API-sourced records with no direct link
        return str(v) if v else ""

    @model_validator(mode="after")
    def ensure_identifier(self):
        """At least one of url or title must uniquely identify the record."""
        if not self.url and not self.title:
            raise ValueError("Record must have either a URL or a title.")
        return self

    class Config:
        str_strip_whitespace = True


# ---------------------------------------------------------------------------
# Validation runner
# ---------------------------------------------------------------------------

class ValidationResult:
    """Outcome of validating a batch of cleaned records."""

    def __init__(self):
        self.valid: list[CleanRecord] = []
        self.invalid: list[dict[str, Any]] = []   # {record, error}

    @property
    def valid_count(self) -> int:
        return len(self.valid)

    @property
    def invalid_count(self) -> int:
        return len(self.invalid)

    @property
    def total(self) -> int:
        return self.valid_count + self.invalid_count


def validate_batch(cleaned_records: list[dict[str, Any]]) -> ValidationResult:
    """
    Validate a list of cleaned record dicts against the CleanRecord schema.

    Valid records → ValidationResult.valid (list[CleanRecord])
    Invalid records → ValidationResult.invalid with error messages attached

    Args:
        cleaned_records: Output of etl.cleaner.clean_batch()

    Returns:
        ValidationResult with separated valid / invalid records
    """
    result = ValidationResult()

    for record in cleaned_records:
        try:
            validated = CleanRecord.model_validate(record)
            result.valid.append(validated)
        except Exception as exc:
            logger.debug(
                "Validation failed for '%s': %s",
                record.get("title", "?")[:60], exc,
            )
            result.invalid.append({"record": record, "error": str(exc)})

    logger.info(
        "Validation: %d valid, %d invalid out of %d records",
        result.valid_count, result.invalid_count, result.total,
    )
    return result
