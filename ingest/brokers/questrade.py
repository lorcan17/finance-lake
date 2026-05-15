"""Questrade snapshot ingest — thin wrapper over questrade-extract + bronze landing.

questrade-extract writes a SQLite DB of position/account snapshots. This module
lands a copy of that DB into bronze for provenance, alongside the existing
DuckDB attach path used by the pipeline.

Usage (manual or from a systemd ExecStart):
    python -m ingest.brokers.questrade --db /path/to/questrade.db
"""
from __future__ import annotations

import argparse
from pathlib import Path

from ingest._lib.bronze import land


def ingest_snapshot(db_path: Path) -> Path:
    """Land a questrade SQLite snapshot into bronze. Returns destination path."""
    return land("brokers", "questrade", db_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Land a Questrade snapshot into bronze")
    parser.add_argument("--db", required=True, type=Path, help="Path to questrade.db")
    args = parser.parse_args()
    dest = ingest_snapshot(args.db)
    print(f"Landed: {dest}")


if __name__ == "__main__":
    main()
