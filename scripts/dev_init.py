"""Mac dev environment setup.

Creates the lake directory tree under ~/.local/share/foundry/lake/,
mirroring the prod layout on OptiPlex (/var/lib/foundry/lake/).

Run once after cloning, or any time to ensure dirs exist:
    uv run python scripts/dev_init.py
"""
from __future__ import annotations

import os
from pathlib import Path

LAKE_ROOT = Path(os.environ.get("LAKE_ROOT", Path.home() / ".local" / "share" / "foundry" / "lake"))

LAKE_DIRS = [
    LAKE_ROOT / "bronze",
    LAKE_ROOT / "silver",
    LAKE_ROOT / "inbox",
]


def main() -> None:
    for d in LAKE_DIRS:
        d.mkdir(parents=True, exist_ok=True)
        print(f"  ok  {d}")

    db = LAKE_ROOT / "silver" / "finance.duckdb"
    print()
    print(f"LAKE_ROOT       = {LAKE_ROOT}")
    print(f"FINANCE_DUCKDB  = {db}")
    print()
    if not db.exists():
        print("DuckDB not found — run scripts/dev_bootstrap.py to seed from OptiPlex,")
        print("or run `dbt run` against an empty DB to create the silver/gold tables.")
    else:
        print(f"DuckDB present ({db.stat().st_size // 1024} KB)")


if __name__ == "__main__":
    main()
