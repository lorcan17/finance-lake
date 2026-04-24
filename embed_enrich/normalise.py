"""Raw description → embedding → ANN match → dim_merchants / review queue.

Threshold policy: cosine distance ≤ 0.15 auto-matches to existing merchant;
> 0.30 creates a new canonical merchant (uncategorised); in between goes to
review queue.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

import duckdb

from .client import DIMENSIONS, EmbeddingClient

AUTO_MATCH_MAX = 0.15
NEW_MERCHANT_MIN = 0.30


@dataclass
class Candidate:
    raw_description: str
    embedding: list[float]


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS silver.dim_merchants (
            merchant_id VARCHAR PRIMARY KEY,
            canonical_name VARCHAR,
            category_id VARCHAR,
            embedding FLOAT[{DIMENSIONS}],
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver.merchant_review_queue (
            queue_id VARCHAR PRIMARY KEY,
            raw_description VARCHAR,
            candidate_merchant_id VARCHAR,
            distance DOUBLE,
            created_at TIMESTAMP
        )
    """)
    # HNSW index — requires experimental persistence flag (set in duckdb_conn).
    con.execute("""
        CREATE INDEX IF NOT EXISTS dim_merchants_embedding_hnsw
        ON silver.dim_merchants USING HNSW (embedding)
        WITH (metric = 'cosine')
    """)


def unresolved_descriptions(con: duckdb.DuckDBPyConnection) -> list[str]:
    rows = con.execute("""
        SELECT DISTINCT raw_description
        FROM (
            SELECT raw_description FROM bronze.bank_transactions
            UNION ALL
            SELECT raw_description FROM bronze.cc_transactions
        )
        WHERE raw_description NOT IN (
            SELECT raw_description FROM silver.merchant_review_queue
        )
    """).fetchall()
    return [r[0] for r in rows if r[0]]


def nearest_merchant(
    con: duckdb.DuckDBPyConnection, embedding: list[float]
) -> tuple[str, float] | None:
    row = con.execute(
        """
        SELECT merchant_id, array_cosine_distance(embedding, ?::FLOAT[{dim}]) AS d
        FROM silver.dim_merchants
        ORDER BY d ASC
        LIMIT 1
        """.format(dim=DIMENSIONS),
        [embedding],
    ).fetchone()
    if row is None:
        return None
    return row[0], row[1]


def process(con: duckdb.DuckDBPyConnection, client: EmbeddingClient, batch: int = 100) -> None:
    ensure_schema(con)
    todo = unresolved_descriptions(con)
    if not todo:
        return
    now = datetime.now(UTC)
    for i in range(0, len(todo), batch):
        chunk = todo[i : i + batch]
        embeddings = client.embed(chunk)
        for desc, emb in zip(chunk, embeddings, strict=True):
            match = nearest_merchant(con, emb)
            if match and match[1] <= AUTO_MATCH_MAX:
                continue  # existing merchant is good; fact_transactions FK resolves later
            if match is None or match[1] >= NEW_MERCHANT_MIN:
                mid = str(uuid.uuid4())
                con.execute(
                    """
                    INSERT INTO silver.dim_merchants
                    VALUES (?, ?, 'uncategorised', ?, ?, ?)
                    """,
                    [mid, desc, emb, now, now],
                )
            else:
                con.execute(
                    """
                    INSERT INTO silver.merchant_review_queue VALUES (?, ?, ?, ?, ?)
                    """,
                    [str(uuid.uuid4()), desc, match[0], match[1], now],
                )
