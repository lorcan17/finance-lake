"""Banking inbox adapter.

Scans LAKE_ROOT/inbox/banking/ for PDFs and CSVs:
  - PDFs → full ingest via ingest_statement() (bronze file + DuckDB rows)
  - CSVs → bronze landing + raw rows into bronze.banking_csv_raw (validation only)

PDFs are the source of truth. CSVs are a forward-looking quality signal;
they are never promoted to fact_transactions. Gaps surface in
silver.reconciliation_gaps to drive statement-extract improvements.
"""
from __future__ import annotations

import csv as _csv
import logging
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb

from embed_enrich.duckdb_conn import connect
from foundry._telemetry import flush_and_shutdown, setup_meter
from ingest._lib.bronze import land
from ingest.banking.statements import ingest_statement

_log = logging.getLogger(__name__)

_provider, _meter = setup_meter("foundry.ingest.inbox.banking")
_pdf_counter = _meter.create_counter("inbox.banking.pdf.processed")
_csv_counter = _meter.create_counter("inbox.banking.csv.processed")
_error_counter = _meter.create_counter("inbox.banking.errors")

_CREATE_CSV_RAW = """
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE TABLE IF NOT EXISTS bronze.banking_csv_raw (
    source_file  VARCHAR,
    landed_at    TIMESTAMPTZ,
    row_date     DATE,
    description  VARCHAR,
    amount       DECIMAL(12, 2),
    account_hint VARCHAR
);
"""

# Common header name variants across Canadian bank CSV exports.
_DATE_COLS = {"date", "transaction date", "posted date", "trans. date"}
_DESC_COLS = {"description", "details", "transaction details", "memo", "narration"}
_AMOUNT_COLS = {"amount", "debit", "credit", "transaction amount", "cad$"}


def _inbox_dir() -> Path:
    lake = os.environ.get("LAKE_ROOT", Path.home() / ".local" / "share" / "foundry" / "lake")
    return Path(lake) / "inbox" / "banking"


def _normalise_header(headers: list[str]) -> tuple[str | None, str | None, str | None]:
    """Return (date_col, desc_col, amount_col) from a CSV header row."""
    lower = {h.lower().strip(): h for h in headers}
    date_col = next((lower[k] for k in _DATE_COLS if k in lower), None)
    desc_col = next((lower[k] for k in _DESC_COLS if k in lower), None)
    amount_col = next((lower[k] for k in _AMOUNT_COLS if k in lower), None)
    return date_col, desc_col, amount_col


def _ingest_csv(path: Path, con: duckdb.DuckDBPyConnection) -> int:
    """Land CSV to bronze and insert raw rows. Returns number of rows inserted."""
    land("banking", "csv", path)

    rows_inserted = 0
    landed_at = datetime.now(timezone.utc)
    account_hint = path.stem  # filename as a weak account hint

    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = _csv.DictReader(f)
        if reader.fieldnames is None:
            _log.warning("csv %s has no headers, skipping", path.name)
            return 0

        date_col, desc_col, amount_col = _normalise_header(list(reader.fieldnames))
        if not all([date_col, desc_col, amount_col]):
            _log.warning(
                "csv %s: could not identify required columns (date=%s desc=%s amount=%s)",
                path.name, date_col, desc_col, amount_col,
            )
            return 0

        for i, row in enumerate(reader):
            try:
                raw_date = row[date_col].strip()
                row_date = date.fromisoformat(raw_date) if raw_date else None
                raw_amount = row[amount_col].strip().replace(",", "").replace("$", "")
                amount = float(raw_amount) if raw_amount else None
                description = row[desc_col].strip() or None

                if row_date is None or amount is None:
                    continue

                con.execute(
                    "INSERT INTO bronze.banking_csv_raw VALUES (?, ?, ?, ?, ?, ?)",
                    [path.name, landed_at, row_date, description, amount, account_hint],
                )
                rows_inserted += 1
            except Exception:
                _log.warning("csv %s row %d: parse error, skipping", path.name, i + 1, exc_info=True)

    return rows_inserted


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    inbox = _inbox_dir()
    if not inbox.exists():
        _log.info("inbox/banking does not exist at %s — nothing to do", inbox)
        flush_and_shutdown(_provider)
        return 0

    pdfs = sorted(p for p in inbox.iterdir() if p.suffix.lower() == ".pdf")
    csvs = sorted(p for p in inbox.iterdir() if p.suffix.lower() == ".csv")

    if not pdfs and not csvs:
        _log.info("inbox/banking empty")
        flush_and_shutdown(_provider)
        return 0

    _log.info("inbox/banking: %d PDF(s), %d CSV(s)", len(pdfs), len(csvs))
    failed = 0
    con = connect()
    try:
        con.execute(_CREATE_CSV_RAW)

        for pdf in pdfs:
            try:
                result = ingest_statement(pdf, con)
                _log.info(
                    "pdf %s ingested (bank=%s new_row=%s issues=%d)",
                    pdf.name, result.bank or "unknown", result.was_new_row,
                    len(result.validation_issues),
                )
                pdf.unlink()
                _pdf_counter.add(1, {"bank": result.bank or "unknown"})
            except Exception:
                _log.exception("pdf %s failed — leaving in inbox", pdf.name)
                _error_counter.add(1, {"type": "pdf"})
                failed += 1

        for csv_path in csvs:
            try:
                n = _ingest_csv(csv_path, con)
                _log.info("csv %s landed (%d rows)", csv_path.name, n)
                csv_path.unlink()
                _csv_counter.add(1)
            except Exception:
                _log.exception("csv %s failed — leaving in inbox", csv_path.name)
                _error_counter.add(1, {"type": "csv"})
                failed += 1
    finally:
        con.close()
        flush_and_shutdown(_provider)

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
