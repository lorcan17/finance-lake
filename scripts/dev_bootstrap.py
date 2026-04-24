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
    # Assumes legacy.snapshots — adjust table name to whatever questrade-extract writes.
    con.execute("""
        CREATE OR REPLACE TABLE bronze.questrade_snapshots AS
        SELECT * FROM legacy.snapshots
    """)
    con.execute("DETACH legacy")
    con.close()


def main() -> None:
    path = scp_questrade()
    seed_bronze(path)
    print(f"seeded bronze.questrade_snapshots from {path}")


if __name__ == "__main__":
    main()
