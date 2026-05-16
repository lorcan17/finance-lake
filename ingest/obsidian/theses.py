"""Obsidian investment thesis ingest.

Scans a vault directory for markdown files under Investment Theses/,
parses YAML frontmatter, and upserts rows into bronze.investment_theses.

Designed for Mac dev use — run manually against the local vault.
On OptiPlex the same script runs via systemd, pointed at a synced vault.

Usage:
    uv run ingest-obsidian-theses ~/obsidian
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import yaml

from embed_enrich.duckdb_conn import connect
from foundry._telemetry import flush_and_shutdown, setup_meter

_log = logging.getLogger(__name__)

_provider, _meter = setup_meter("foundry.ingest.obsidian.theses")
_upserted = _meter.create_counter("obsidian.theses.upserted")
_errors = _meter.create_counter("obsidian.theses.errors")

_CREATE = """
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE TABLE IF NOT EXISTS bronze.investment_theses (
    thesis_id               VARCHAR PRIMARY KEY,
    ticker                  VARCHAR NOT NULL,
    status                  VARCHAR,
    opened                  DATE,
    closed                  DATE,
    price_at_open           DECIMAL(12, 4),
    buy_threshold           DECIMAL(12, 4),
    sell_threshold          DECIMAL(12, 4),
    stop_loss               DECIMAL(12, 4),
    confidence              DECIMAL(5, 4),
    horizon_months          INTEGER,
    expected_return_pct     DECIMAL(8, 2),
    dependencies            JSON,
    invalidation_conditions JSON,
    decisions               JSON,
    outcome                 JSON,
    source_file             VARCHAR,
    ingested_at             TIMESTAMPTZ
);
"""

_UPSERT = """
INSERT OR REPLACE INTO bronze.investment_theses VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)
"""


def _parse_frontmatter(path: Path) -> dict | None:
    """Extract YAML frontmatter from a markdown file. Returns None if absent."""
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---"):
        return None
    end = text.find("\n---", 3)
    if end == -1:
        return None
    try:
        return yaml.safe_load(text[3:end])
    except yaml.YAMLError:
        _log.warning("yaml parse error in %s", path.name)
        return None


def _ingest_vault(vault: Path, con: duckdb.DuckDBPyConnection) -> int:
    thesis_dir = vault / "Investment Theses"
    if not thesis_dir.exists():
        _log.warning("Investment Theses/ not found in %s", vault)
        return 0

    files = sorted(thesis_dir.glob("*.md"))
    if not files:
        _log.info("no thesis files found")
        return 0

    ingested_at = datetime.now(timezone.utc)
    count = 0

    for f in files:
        fm = _parse_frontmatter(f)
        if not fm:
            _log.debug("skipping %s — no frontmatter", f.name)
            continue

        thesis_id = fm.get("thesis_id") or f.stem
        ticker = fm.get("ticker")
        if not ticker:
            _log.warning("%s: missing ticker, skipping", f.name)
            continue

        import json
        try:
            con.execute(_UPSERT, [
                thesis_id,
                ticker,
                fm.get("status"),
                fm.get("opened"),
                fm.get("closed"),
                fm.get("price_at_open"),
                fm.get("buy_threshold"),
                fm.get("sell_threshold"),
                fm.get("stop_loss"),
                fm.get("confidence"),
                fm.get("horizon_months"),
                fm.get("expected_return_pct"),
                json.dumps(fm.get("dependencies") or []),
                json.dumps(fm.get("invalidation_conditions") or []),
                json.dumps(fm.get("decisions") or []),
                json.dumps(fm.get("outcome") or {}),
                str(f),
                ingested_at,
            ])
            _log.info("upserted %s (%s)", thesis_id, ticker)
            _upserted.add(1, {"ticker": ticker})
            count += 1
        except Exception:
            _log.exception("failed to upsert %s", f.name)
            _errors.add(1, {"ticker": ticker or "unknown"})

    return count


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    vault_arg = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("OBSIDIAN_VAULT")
    if not vault_arg:
        _log.error("usage: ingest-obsidian-theses <vault-path>")
        flush_and_shutdown(_provider)
        return 2

    vault = Path(vault_arg).expanduser().resolve()
    if not vault.exists():
        _log.error("vault not found: %s", vault)
        flush_and_shutdown(_provider)
        return 2

    con = connect()
    try:
        con.execute(_CREATE)
        n = _ingest_vault(vault, con)
        _log.info("done — %d thesis/theses ingested", n)
    finally:
        con.close()
        flush_and_shutdown(_provider)

    return 0


if __name__ == "__main__":
    sys.exit(main())
