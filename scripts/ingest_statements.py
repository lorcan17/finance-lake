"""Walk ~/Documents/bank-statements/<owner>/<kind>/*.pdf and ingest into bronze.

Uses statement-extract's parsers directly (editable dep). Internal validation
issues are logged but do not block ingest — bronze is immutable raw; dbt tests
catch problems downstream.
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from bank_pdf_extract.parsers import (
    amex_cobalt,
    bmo_credit_card,
    bmo_deposit_account,
    coast_capital_chequing,
    coast_capital_credit,
    eq_bank,
)

from embed_enrich.duckdb_conn import connect

STATEMENTS_ROOT = Path.home() / "Documents" / "bank-statements"

# Folder name → (parser module, destination kind).
FOLDER_MAP: dict[str, tuple[object, str]] = {
    "bmo_credit_card": (bmo_credit_card, "cc"),
    "bmo_deposit_account": (bmo_deposit_account, "bank"),
    "coast_capital_chequing": (coast_capital_chequing, "bank"),
    "coast_capital_credit": (coast_capital_credit, "cc"),
    "amex": (amex_cobalt, "cc"),
    "eq_bank_savings": (eq_bank, "bank"),
}


def iter_pdfs() -> Iterable[tuple[str, str, Path]]:
    for owner_dir in STATEMENTS_ROOT.iterdir():
        if not owner_dir.is_dir() or owner_dir.name.startswith("."):
            continue
        for kind_dir in owner_dir.iterdir():
            if not kind_dir.is_dir() or kind_dir.name not in FOLDER_MAP:
                continue
            for pdf in sorted(kind_dir.rglob("*.pdf")):
                yield owner_dir.name, kind_dir.name, pdf


def insert_cc(con, owner: str, bank: str, pdf: Path, header, details) -> None:
    rows = [
        (
            owner,
            bank,
            d.card_number,
            d.transaction_date,
            d.posting_date,
            float(d.amount),
            d.description,
            d.original_currency,
            float(d.original_amount) if d.original_amount is not None else None,
            str(pdf),
        )
        for d in details
    ]
    if not rows:
        return
    con.executemany(
        """
        INSERT INTO bronze.cc_transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def insert_bank(con, owner: str, bank: str, pdf: Path, header, details) -> None:
    acct = header.account_number if header else None
    rows = [
        (
            owner,
            bank,
            acct if acct else d.account_number,
            d.posting_date,
            float(d.amount),
            d.description,
            float(d.running_balance),
            str(pdf),
        )
        for d in details
    ]
    if not rows:
        return
    con.executemany(
        """
        INSERT INTO bronze.bank_transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def process_one(con, owner: str, kind: str, pdf: Path) -> tuple[int, str | None]:
    parser, dest = FOLDER_MAP[kind]
    try:
        res = parser.parse(pdf)
    except Exception as e:
        return 0, f"parse error: {e}"

    # Multi-account (Coast Capital chequing) returns MultiAccountDepositStatement.
    if hasattr(res, "accounts"):
        total = 0
        bank_name = res.bank
        for sub in res.accounts:
            insert_bank(con, owner, bank_name, pdf, sub.header, sub.details)
            total += len(sub.details)
        return total, None

    # Tuple (header, details) per cli._PARSERS behaviour.
    header, details = res
    bank_name = header.bank if hasattr(header, "bank") else kind
    if dest == "cc":
        insert_cc(con, owner, bank_name, pdf, header, details)
    else:
        insert_bank(con, owner, bank_name, pdf, header, details)
    return len(details), None


def main() -> None:
    con = connect()
    con.execute("DELETE FROM bronze.bank_transactions")
    con.execute("DELETE FROM bronze.cc_transactions")

    ok = 0
    failed: list[tuple[Path, str]] = []
    total_rows = 0

    for owner, kind, pdf in iter_pdfs():
        count, err = process_one(con, owner, kind, pdf)
        if err:
            failed.append((pdf, err))
            continue
        ok += 1
        total_rows += count

    con.close()

    print(f"parsed: {ok} PDFs, {total_rows} detail rows")
    if failed:
        print(f"failed: {len(failed)}")
        for pdf, err in failed[:20]:
            rel = pdf.relative_to(STATEMENTS_ROOT)
            print(f"  {rel}: {err}")
        if len(failed) > 20:
            print(f"  ... and {len(failed) - 20} more")


if __name__ == "__main__":
    main()
