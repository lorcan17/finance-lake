import os
from pathlib import Path

import duckdb


def default_db_path() -> Path:
    env = os.environ.get("FINANCE_DUCKDB")
    if env:
        return Path(env)
    return Path.home() / ".local" / "share" / "finance-lake" / "finance.duckdb"


def connect(db_path: Path | None = None) -> duckdb.DuckDBPyConnection:
    path = db_path or default_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    con.execute("INSTALL vss")
    con.execute("LOAD vss")
    # Required for HNSW indexes on file-backed DBs (ADR-004).
    con.execute("SET hnsw_enable_experimental_persistence = true")
    return con
