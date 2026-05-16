"""Banking statement ingest — thin wrapper over statement-extract + bronze landing.

Parses a PDF bank/CC statement and simultaneously:
  1. Lands the raw file into bronze (via land()) for provenance + replay
  2. Inserts structured rows into bronze DuckDB tables (via ingest_pdf())

The two writes are independent — the file landing is always attempted even if
DuckDB insert fails, so raw files are never lost.
"""
from __future__ import annotations

from pathlib import Path

import duckdb

from foundry._telemetry import setup_meter
from ingest._lib.bronze import land
from ingest.core import ingest_pdf
from ingest.sources import IngestResult, SourceRef

_provider, _meter = setup_meter("foundry.ingest.banking")
_parse_counter = _meter.create_counter("banking.statements.parsed")
_row_counter = _meter.create_counter("banking.rows.inserted")
_error_counter = _meter.create_counter("banking.statements.errors")


def ingest_statement(
    pdf: Path,
    con: duckdb.DuckDBPyConnection,
    source_name: str = "manual",
) -> IngestResult:
    """Land a PDF into bronze files and insert parsed rows into DuckDB.

    source_name: the institution slug used in the bronze path, e.g. 'td', 'bmo',
    'amex'. Defaults to 'manual' for hand-dropped files where the bank will be
    auto-detected from the PDF content.
    """
    land("banking", source_name, pdf)
    source = SourceRef(type="local", id=str(pdf))
    result = ingest_pdf(pdf, source, con)

    labels = {"bank": result.bank or "unknown", "was_finance_doc": str(result.was_finance_doc)}
    _parse_counter.add(1, labels)
    if result.was_finance_doc and result.was_new_row:
        _row_counter.add(1, labels)
    if result.validation_issues:
        _error_counter.add(len(result.validation_issues), labels)

    return result
