"""Local-filesystem adapter — used by `ingest_statements.py` rebuild + tests."""
from __future__ import annotations

from pathlib import Path

from ..sources import IngestResult, SourceRef


def fetch(source: SourceRef) -> Path:
    return Path(source.id)


def writeback(source: SourceRef, result: IngestResult) -> None:
    return None


def source_for(path: Path) -> SourceRef:
    return SourceRef(type="local", id=str(path.resolve()))
