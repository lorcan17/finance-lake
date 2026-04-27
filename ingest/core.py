"""Storage-agnostic PDF ingestion.

`ingest_pdf` is the only entry point that touches bronze. Adapters (Paperless,
local, S3...) just resolve a SourceRef to a local Path and call this.

Bronze grain:
- `bronze.{bank,cc}_statements` — one row per parsed statement header. Holds
  metadata, header totals, validation_issues, ingestion provenance. This is
  the system of record.
- `bronze.{bank,cc}_transactions` — detail grain, FK `statement_sha256`.

Idempotency: sha256(file) is the dedup key. If a statement with that sha is
already present we skip insert (transactions can't be partially inserted —
header insert and detail insert are atomic per call).
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path

from bank_pdf_extract import detect_parser
from bank_pdf_extract.schema import (
    CreditCardHeader,
    DepositAccountStatement,
    MultiAccountDepositStatement,
)

from .sources import IngestResult, SourceRef


def ingest_pdf(file_path: Path, source: SourceRef, con) -> IngestResult:
    ensure_bronze_schema(con)
    parser = detect_parser(file_path)
    if parser is None:
        return IngestResult(was_finance_doc=False, was_new_row=False)

    sha = _sha256(file_path)
    if _already_ingested(con, sha):
        return IngestResult(was_finance_doc=True, was_new_row=False, sha256=sha)

    result = parser.parse(file_path)
    issues = _validate(parser, result)
    parsed_at = datetime.now(timezone.utc)

    if isinstance(result, MultiAccountDepositStatement):
        if not result.accounts:
            return IngestResult(
                was_finance_doc=False,
                was_new_row=False,
                sha256=sha,
                validation_issues=issues + ["multi_account_no_accounts"],
            )
        for sub in result.accounts:
            _insert_bank_statement(con, sub, source, sha, issues, parsed_at)
            _insert_bank_details(con, sub, sha)
        first = result.accounts[0]
        return IngestResult(
            was_finance_doc=True,
            was_new_row=True,
            bank=result.bank,
            bank_product=f"{first.header.bank}_{first.header.product}",
            holder=first.header.account_holder,
            last4=_last4(first.header.account_number),
            period_start=result.period_start,
            period_end=result.period_end,
            sha256=sha,
            validation_issues=issues,
        )

    header, details = result
    if isinstance(header, CreditCardHeader):
        _insert_cc_statement(con, header, details, source, sha, issues, parsed_at)
        _insert_cc_details(con, details, sha)
        return IngestResult(
            was_finance_doc=True,
            was_new_row=True,
            bank=header.bank,
            bank_product=f"{header.bank}_{header.product}",
            holder=header.account_holder,
            last4=_last4(header.card_number_last4),
            period_start=header.period_start,
            period_end=header.period_end,
            sha256=sha,
            validation_issues=issues,
        )

    stmt = DepositAccountStatement(header=header, details=details)
    _insert_bank_statement(con, stmt, source, sha, issues, parsed_at)
    _insert_bank_details(con, stmt, sha)
    return IngestResult(
        was_finance_doc=True,
        was_new_row=True,
        bank=header.bank,
        bank_product=f"{header.bank}_{header.product}",
        holder=header.account_holder,
        last4=_last4(header.account_number),
        period_start=header.period_start,
        period_end=header.period_end,
        sha256=sha,
        validation_issues=issues,
    )


# --- helpers ----------------------------------------------------------------

def ensure_bronze_schema(con) -> None:
    """Create bronze schema + tables if missing. Idempotent."""
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze.bank_statements (
            sha256 VARCHAR, source_type VARCHAR, source_id VARCHAR,
            holder VARCHAR, bank VARCHAR, product VARCHAR,
            account_type VARCHAR, account_number VARCHAR,
            branch_name VARCHAR, transit_number VARCHAR, plan_name VARCHAR,
            period_start DATE, period_end DATE,
            opening_balance DOUBLE, total_deducted DOUBLE,
            total_added DOUBLE, closing_balance DOUBLE,
            validation_issues VARCHAR[], parsed_at TIMESTAMPTZ
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze.cc_statements (
            sha256 VARCHAR, source_type VARCHAR, source_id VARCHAR,
            holder VARCHAR, bank VARCHAR, product VARCHAR,
            card_number_last4 VARCHAR, statement_date DATE,
            period_start DATE, period_end DATE, payment_due_date DATE,
            previous_balance DOUBLE, payments_and_credits DOUBLE,
            purchases_and_other_charges DOUBLE, new_installments DOUBLE,
            cash_advances DOUBLE, total_interest_charges DOUBLE,
            fees DOUBLE, total_balance DOUBLE,
            minimum_payment_due DOUBLE, credit_limit DOUBLE,
            available_credit DOUBLE, validation_issues VARCHAR[],
            parsed_at TIMESTAMPTZ, n_details INTEGER
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze.bank_transactions (
            statement_sha256 VARCHAR, account_number VARCHAR,
            txn_date DATE, amount DOUBLE,
            raw_description VARCHAR, running_balance DOUBLE
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze.cc_transactions (
            statement_sha256 VARCHAR, card_number VARCHAR,
            txn_date DATE, posting_date DATE, amount DOUBLE,
            raw_description VARCHAR, original_currency VARCHAR,
            original_amount DOUBLE, exchange_rate DOUBLE
        )
    """)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _already_ingested(con, sha: str) -> bool:
    for table in ("bronze.bank_statements", "bronze.cc_statements"):
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


def _last4(account_id: str) -> str:
    digits = "".join(ch for ch in account_id if ch.isdigit())
    return digits[-4:] if len(digits) >= 4 else ""


def _insert_bank_statement(
    con,
    stmt: DepositAccountStatement,
    source: SourceRef,
    sha: str,
    issues: list[str],
    parsed_at: datetime,
) -> None:
    h = stmt.header
    con.execute(
        """
        INSERT INTO bronze.bank_statements
        (sha256, source_type, source_id, holder, bank, product, account_type,
         account_number, branch_name, transit_number, plan_name,
         period_start, period_end,
         opening_balance, total_deducted, total_added, closing_balance,
         validation_issues, parsed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            sha, source.type, source.id, h.account_holder, h.bank, h.product,
            h.account_type, h.account_number, h.branch_name, h.transit_number,
            h.plan_name, h.period_start, h.period_end,
            float(h.opening_balance), float(h.total_deducted),
            float(h.total_added), float(h.closing_balance),
            issues, parsed_at,
        ],
    )


def _insert_bank_details(con, stmt: DepositAccountStatement, sha: str) -> None:
    if not stmt.details:
        return
    rows = [
        (
            sha,
            stmt.header.account_number or d.account_number,
            d.posting_date,
            float(d.amount),
            d.description,
            float(d.running_balance),
        )
        for d in stmt.details
    ]
    con.executemany(
        """
        INSERT INTO bronze.bank_transactions
        (statement_sha256, account_number, txn_date, amount, raw_description,
         running_balance)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def _insert_cc_statement(
    con,
    h: CreditCardHeader,
    details,
    source: SourceRef,
    sha: str,
    issues: list[str],
    parsed_at: datetime,
) -> None:
    con.execute(
        """
        INSERT INTO bronze.cc_statements
        (sha256, source_type, source_id, holder, bank, product,
         card_number_last4, statement_date, period_start, period_end,
         payment_due_date, previous_balance, payments_and_credits,
         purchases_and_other_charges, new_installments, cash_advances,
         total_interest_charges, fees, total_balance, minimum_payment_due,
         credit_limit, available_credit,
         validation_issues, parsed_at, n_details)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            sha, source.type, source.id, h.account_holder, h.bank, h.product,
            h.card_number_last4, h.statement_date, h.period_start, h.period_end,
            h.payment_due_date,
            float(h.previous_balance), float(h.payments_and_credits),
            float(h.purchases_and_other_charges), float(h.new_installments),
            float(h.cash_advances), float(h.total_interest_charges),
            float(h.fees), float(h.total_balance), float(h.minimum_payment_due),
            float(h.credit_limit), float(h.available_credit),
            issues, parsed_at, len(details),
        ],
    )


def _insert_cc_details(con, details, sha: str) -> None:
    if not details:
        return
    rows = [
        (
            sha,
            d.card_number,
            d.transaction_date,
            d.posting_date,
            float(d.amount),
            d.description,
            d.original_currency,
            float(d.original_amount) if d.original_amount is not None else None,
            float(d.exchange_rate) if d.exchange_rate is not None else None,
        )
        for d in details
    ]
    con.executemany(
        """
        INSERT INTO bronze.cc_transactions
        (statement_sha256, card_number, txn_date, posting_date, amount,
         raw_description, original_currency, original_amount, exchange_rate)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
