"""Rebuild bronze.{bank,cc}_transactions from local PDF archive.

Walks ~/Documents/bank-statements/<owner>/<bank_product>/[<last4>/]*.pdf and
re-ingests every PDF via `ingest.core.ingest_pdf`. Drops + recreates the
bronze tables with the current schema (source_type, source_id, sha256,
validation_issues). Backs up `finance.duckdb` before any destructive op so
post-run row counts can be diffed against the prior run.

Note: bronze schema is the system of record but storage-cheap; we rebuild
rather than migrate. Backup gives us a comparison point if a parser change
introduces a regression.
"""
from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path

from embed_enrich.duckdb_conn import connect

from ingest.adapters import local
from ingest.core import ingest_pdf

STATEMENTS_ROOT = Path.home() / "Documents" / "bank-statements"


def _backup_duckdb() -> Path | None:
    """Copy finance.duckdb next to itself with a timestamp suffix."""
    db_path = Path.home() / ".local" / "share" / "finance-lake" / "finance.duckdb"
    if not db_path.exists():
        # New install — nothing to back up. Caller proceeds.
        return None
    ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    backup = db_path.with_suffix(f".duckdb.bak.{ts}")
    shutil.copy2(db_path, backup)
    return backup


def _reset_bronze(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    for tbl in (
        "bronze.bank_transactions",
        "bronze.cc_transactions",
        "bronze.bank_statements",
        "bronze.cc_statements",
    ):
        con.execute(f"DROP TABLE IF EXISTS {tbl}")

    con.execute("""
        CREATE TABLE bronze.bank_statements (
            sha256 VARCHAR,
            source_type VARCHAR,
            source_id VARCHAR,
            holder VARCHAR,
            bank VARCHAR,
            product VARCHAR,
            account_type VARCHAR,
            account_number VARCHAR,
            branch_name VARCHAR,
            transit_number VARCHAR,
            plan_name VARCHAR,
            period_start DATE,
            period_end DATE,
            opening_balance DOUBLE,
            total_deducted DOUBLE,
            total_added DOUBLE,
            closing_balance DOUBLE,
            validation_issues VARCHAR[],
            parsed_at TIMESTAMPTZ
        )
    """)
    con.execute("""
        CREATE TABLE bronze.cc_statements (
            sha256 VARCHAR,
            source_type VARCHAR,
            source_id VARCHAR,
            holder VARCHAR,
            bank VARCHAR,
            product VARCHAR,
            card_number_last4 VARCHAR,
            statement_date DATE,
            period_start DATE,
            period_end DATE,
            payment_due_date DATE,
            previous_balance DOUBLE,
            payments_and_credits DOUBLE,
            purchases_and_other_charges DOUBLE,
            new_installments DOUBLE,
            cash_advances DOUBLE,
            total_interest_charges DOUBLE,
            fees DOUBLE,
            total_balance DOUBLE,
            minimum_payment_due DOUBLE,
            credit_limit DOUBLE,
            available_credit DOUBLE,
            validation_issues VARCHAR[],
            parsed_at TIMESTAMPTZ,
            n_details INTEGER
        )
    """)
    con.execute("""
        CREATE TABLE bronze.bank_transactions (
            statement_sha256 VARCHAR,
            account_number VARCHAR,
            txn_date DATE,
            amount DOUBLE,
            raw_description VARCHAR,
            running_balance DOUBLE
        )
    """)
    con.execute("""
        CREATE TABLE bronze.cc_transactions (
            statement_sha256 VARCHAR,
            card_number VARCHAR,
            txn_date DATE,
            posting_date DATE,
            amount DOUBLE,
            raw_description VARCHAR,
            original_currency VARCHAR,
            original_amount DOUBLE,
            exchange_rate DOUBLE
        )
    """)


def main() -> None:
    backup = _backup_duckdb()
    if backup:
        print(f"backup: {backup}")
    else:
        print("backup: no existing db — skipping")

    con = connect()
    _reset_bronze(con)

    ok = 0
    skipped_nonfinance = 0
    failed: list[tuple[Path, str]] = []
    issues_count = 0

    for pdf in sorted(STATEMENTS_ROOT.rglob("*.pdf")):
        try:
            result = ingest_pdf(pdf, local.source_for(pdf), con)
        except Exception as e:
            failed.append((pdf, f"{type(e).__name__}: {e}"))
            continue
        if not result.was_finance_doc:
            skipped_nonfinance += 1
            continue
        ok += 1
        if result.validation_issues:
            issues_count += 1

    con.close()

    print(f"ingested: {ok} PDFs, {issues_count} with validation issues")
    print(f"skipped (not finance): {skipped_nonfinance}")
    if failed:
        print(f"failed: {len(failed)}")
        for pdf, err in failed[:20]:
            try:
                rel = pdf.relative_to(STATEMENTS_ROOT)
            except ValueError:
                rel = pdf
            print(f"  {rel}: {err}")
        if len(failed) > 20:
            print(f"  ... and {len(failed) - 20} more")


if __name__ == "__main__":
    main()
