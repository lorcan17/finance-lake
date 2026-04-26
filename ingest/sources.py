"""Storage-agnostic ingestion types.

A `SourceRef` identifies a document in any backend (Paperless doc id, S3 key,
local path). An `IngestResult` reports what `ingest_pdf` did so callers
(e.g. the Paperless adapter) can take follow-up actions like metadata PATCH.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Literal

SourceType = Literal["paperless", "s3", "gdrive", "local", "manual"]


@dataclass(frozen=True)
class SourceRef:
    type: SourceType
    id: str  # opaque id in that backend


@dataclass(frozen=True)
class IngestResult:
    was_finance_doc: bool
    was_new_row: bool
    bank: str | None = None
    bank_product: str | None = None
    holder: str | None = None
    last4: str | None = None
    period_start: date | None = None
    period_end: date | None = None
    sha256: str | None = None
    validation_issues: list[str] = field(default_factory=list)
