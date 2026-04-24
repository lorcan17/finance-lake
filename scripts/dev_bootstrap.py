"""Mac-only: seed a local DuckDB with bronze data copied from OptiPlex.

- scp /var/lib/questrade-extract/questrade.db from OptiPlex
- ATTACH it as `legacy` and copy into bronze.questrade_snapshots
- Placeholder for bank/CC sample PDFs (wire once bank-cc-extract lands)

Not shipped to prod (ADR-003).
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from embed_enrich.duckdb_conn import connect

OPTIPLEX_HOST = "optiplex"
REMOTE_DB = "/var/lib/questrade-extract/questrade.db"
LOCAL_DATA = Path.home() / ".local" / "share" / "finance-lake"


def scp_questrade() -> Path:
    LOCAL_DATA.mkdir(parents=True, exist_ok=True)
    local = LOCAL_DATA / "questrade.db"
    subprocess.run(
        ["scp", f"{OPTIPLEX_HOST}:{REMOTE_DB}", str(local)],
        check=True,
    )
    return local


def seed_bronze(sqlite_path: Path) -> None:
    con = connect()
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("INSTALL sqlite")
    con.execute("LOAD sqlite")
    con.execute(f"ATTACH '{sqlite_path}' AS legacy (TYPE SQLITE)")
    con.execute("""
        CREATE OR REPLACE TABLE bronze.questrade_snapshots AS
        SELECT
            account_number,
            CAST(snapshot_date AS DATE) AS snapshot_date,
            symbol,
            symbol_id,
            description,
            currency,
            quantity,
            current_price,
            average_entry_price,
            current_market_value AS market_value,
            book_cost,
            open_pnl,
            fetched_at
        FROM legacy.questrade_positions
    """)
    con.execute("DETACH legacy")
    # Empty bank/cc stubs so dbt sources resolve until bank-cc-extract lands.
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze.bank_transactions (
            owner VARCHAR,
            bank VARCHAR,
            account_number VARCHAR,
            txn_date DATE,
            amount DOUBLE,
            raw_description VARCHAR,
            running_balance DOUBLE,
            source_pdf VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze.cc_transactions (
            owner VARCHAR,
            bank VARCHAR,
            card_number VARCHAR,
            txn_date DATE,
            posting_date DATE,
            amount DOUBLE,
            raw_description VARCHAR,
            original_currency VARCHAR,
            original_amount DOUBLE,
            source_pdf VARCHAR
        )
    """)
    con.close()


def main() -> None:
    path = scp_questrade()
    seed_bronze(path)
    print(f"seeded bronze.questrade_snapshots from {path}")


if __name__ == "__main__":
    main()
