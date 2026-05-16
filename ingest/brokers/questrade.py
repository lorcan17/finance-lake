"""Questrade ingest orchestrator — extract, land, and emit telemetry.

Calls questrade_extract.runner.run() to fetch the latest snapshot, lands
the resulting SQLite DB into bronze, then pushes OTel metrics.

This is the systemd ExecStart entrypoint — replaces the old direct call to
questrade_extract.runner so that all telemetry lives in Foundry.

Usage:
    python -m ingest.brokers.questrade
"""
from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

from foundry._telemetry import flush_and_shutdown, setup_meter
from ingest._lib.bronze import land

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


def ingest_snapshot(db_path: Path) -> Path:
    """Land a pre-existing questrade SQLite snapshot into bronze. Returns destination path."""
    return land("brokers", "questrade", db_path)


def main() -> None:
    from questrade_extract.runner import RunResult, run as qe_run

    provider, meter = setup_meter("foundry.ingest.questrade")
    run_duration = meter.create_histogram("questrade.run.duration_seconds", unit="s")
    run_status = meter.create_gauge("questrade.run.exit_status")
    rows_counter = meter.create_counter("questrade.rows_written")

    t0 = time.perf_counter()
    exit_code = 0

    try:
        result = qe_run()

        if result.success:
            landed = ingest_snapshot(Path(result.db_path))
            logger.info("Bronze: %s", landed)
            rows_counter.add(result.balances_written, {"table": "balances"})
            rows_counter.add(result.positions_written, {"table": "positions"})
        else:
            logger.error("Extract failed: %s", result.error)
            exit_code = 1

    except Exception:
        logger.exception("Unexpected error in questrade ingest")
        exit_code = 1

    finally:
        elapsed = time.perf_counter() - t0
        run_duration.record(elapsed)
        run_status.set(exit_code)
        flush_and_shutdown(provider)

    if exit_code:
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
