"""Storage-agnostic PDF ingestion.

`ingest_pdf` is the only entry point that touches bronze. Adapters (Paperless,
local, S3...) just resolve a SourceRef to a local Path and call this.

Idempotency: sha256 of the file is the dedup key. If the same content has
already been ingested under any source_type, we skip insert and return
was_new_row=False.
"""
from __future__ import annotations

import hashlib
from pathlib import Path

from bank_pdf_extract import derive_metadata, detect_parser
from bank_pdf_extract.schema import (
    CreditCardHeader,
    DepositAccountHeader,
    MultiAccountDepositStatement,
)

from .sources import IngestResult, SourceRef


def ingest_pdf(file_path: Path, source: SourceRef, con) -> IngestResult:
    parser = detect_parser(file_path)
    if parser is None:
        return IngestResult(was_finance_doc=False, was_new_row=False)

    sha = _sha256(file_path)
    if _already_ingested(con, sha):
        result = parser.parse(file_path)
        holder, bank_product, last4 = derive_metadata(_header_for_metadata(result))
        return IngestResult(
            was_finance_doc=True,
            was_new_row=False,
            bank=_bank_of(result),
            bank_product=bank_product,
            holder=holder,
            last4=last4,
            sha256=sha,
            period_start=_period_start(result),
            period_end=_period_end(result),
        )

    result = parser.parse(file_path)
    issues = _validate(parser, result)

    if isinstance(result, MultiAccountDepositStatement):
        if not result.accounts:
            # Parsed but no accounts found — treat as not-a-finance-doc rather
            # than crashing on metadata derivation. Surfaces the parser bug
            # without blocking the rebuild.
            return IngestResult(
                was_finance_doc=False,
                was_new_row=False,
                sha256=sha,
                validation_issues=issues + ["multi_account_no_accounts"],
            )
        rows_inserted = _insert_multi_deposit(con, result, source, sha, issues)
        holder, bank_product, last4 = derive_metadata(result)
        return IngestResult(
            was_finance_doc=True,
            was_new_row=rows_inserted > 0,
            bank=result.bank,
            bank_product=bank_product,
            holder=holder,
            last4=last4,
            period_start=result.period_start,
            period_end=result.period_end,
            sha256=sha,
            validation_issues=issues,
        )

    header, details = result
    holder, bank_product, last4 = derive_metadata(header)
    if isinstance(header, CreditCardHeader):
        rows_inserted = _insert_cc(con, header, details, source, sha, issues)
    else:
        rows_inserted = _insert_bank(con, header, details, source, sha, issues)
    return IngestResult(
        was_finance_doc=True,
        was_new_row=rows_inserted > 0,
        bank=header.bank,
        bank_product=bank_product,
        holder=holder,
        last4=last4,
        period_start=header.period_start,
        period_end=header.period_end,
        sha256=sha,
        validation_issues=issues,
    )


# --- helpers ----------------------------------------------------------------

def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _already_ingested(con, sha: str) -> bool:
    for table in ("bronze.bank_transactions", "bronze.cc_transactions"):
        row = con.execute(
            f"SELECT 1 FROM {table} WHERE sha256 = ? LIMIT 1", [sha]
        ).fetchone()
        if row:
            return True
    return False


def _validate(parser, result) -> list[str]:
    if isinstance(result, MultiAccountDepositStatement):
        return parser.validate_internal(result)
    header, details = result
    return parser.validate_internal(header, details)


def _bank_of(result) -> str:
    if isinstance(result, MultiAccountDepositStatement):
        return result.bank
    header, _ = result
    return header.bank


def _period_start(result):
    if isinstance(result, MultiAccountDepositStatement):
        return result.period_start
    return result[0].period_start


def _period_end(result):
    if isinstance(result, MultiAccountDepositStatement):
        return result.period_end
    return result[0].period_end


def _header_for_metadata(result):
    if isinstance(result, MultiAccountDepositStatement):
        return result
    return result[0]


def _insert_cc(
    con, header: CreditCardHeader, details, source: SourceRef, sha: str, issues: list[str]
) -> int:
    if not details:
        return 0
    rows = [
        (
            header.account_holder,
            header.bank,
            d.card_number,
            d.transaction_date,
            d.posting_date,
            float(d.amount),
            d.description,
            d.original_currency,
            float(d.original_amount) if d.original_amount is not None else None,
            source.type,
            source.id,
            sha,
            issues,
        )
        for d in details
    ]
    con.executemany(
        """
        INSERT INTO bronze.cc_transactions
        (holder, bank, card_number, txn_date, posting_date, amount,
         raw_description, original_currency, original_amount,
         source_type, source_id, sha256, validation_issues)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def _insert_bank(
    con,
    header: DepositAccountHeader,
    details,
    source: SourceRef,
    sha: str,
    issues: list[str],
) -> int:
    if not details:
        return 0
    rows = [
        (
            header.account_holder,
            header.bank,
            header.account_number or d.account_number,
            d.posting_date,
            float(d.amount),
            d.description,
            float(d.running_balance),
            source.type,
            source.id,
            sha,
            issues,
        )
        for d in details
    ]
    con.executemany(
        """
        INSERT INTO bronze.bank_transactions
        (holder, bank, account_number, txn_date, amount, raw_description,
         running_balance, source_type, source_id, sha256, validation_issues)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def _insert_multi_deposit(
    con,
    stmt: MultiAccountDepositStatement,
    source: SourceRef,
    sha: str,
    issues: list[str],
) -> int:
    total = 0
    for sub in stmt.accounts:
        total += _insert_bank(con, sub.header, sub.details, source, sha, issues)
    return total
