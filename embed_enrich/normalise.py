"""Raw description → rule pre-pass → embedding → ANN match → dim_merchants / review queue.

Pipeline for each unresolved raw description:

1. Clean the string (strip processor prefixes, trailing city/province, phone
   numbers, long digit runs). Cleaned form is what gets embedded AND stored as
   canonical_name.
2. Deterministic rule pre-pass: substring match against silver.dim_category_rules
   (seeded from seeds/dim_category_rules.csv). If a rule matches, insert a new
   merchant with the resolved category_id and skip embeddings entirely.
3. Otherwise embed the cleaned string and ANN-match against dim_merchants.
   - cosine distance ≤ AUTO_MATCH_MAX (0.22): existing merchant is good; no write.
   - distance ≥ NEW_MERCHANT_MIN (0.35): insert as new uncategorised merchant.
   - in between: queue for human review.
"""

from __future__ import annotations

import csv
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from .client import DIMENSIONS, EmbeddingClient

AUTO_MATCH_MAX = 0.22
NEW_MERCHANT_MIN = 0.35

RULES_CSV = Path(__file__).parent.parent / "seeds" / "dim_category_rules.csv"

_PROVINCE = r"(?:AB|BC|MB|NB|NL|NS|NT|NU|ON|PE|QC|SK|YT)"
# Stripped in order. Each substitution is case-insensitive.
_CLEAN_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bUSD\s+[\d.]+@[\d.]+\b", re.I), " "),  # "USD 10@1.415"
    (re.compile(r"\b(?:SQ|TST|PP|BAM)\s*\*+", re.I), " "),  # processor prefixes
    (re.compile(r"\b\d{3}[-\s]?\d{3}[-\s]?\d{4}\b"), " "),  # phone numbers
    (re.compile(r"\b1[-\s]?8\d{2}[-\s]?\d{3}[-\s]?\d{4}\b"), " "),  # toll-free
    (re.compile(r"#\s*\d+"), " "),  # "#12345" store numbers
    (re.compile(r"\b\d{4,}\b"), " "),  # long digit runs
    (re.compile(rf"\b[A-Z][a-zA-Z'.\- ]+?\s+{_PROVINCE}\b"), " "),  # "Toronto ON"
    (re.compile(rf"\b{_PROVINCE}\b"), " "),  # bare province code
    (re.compile(r"[^\w&'.\- ]+"), " "),  # misc punctuation
    (re.compile(r"\s+"), " "),  # collapse whitespace
]


@dataclass(frozen=True)
class Rule:
    category_id: str
    pattern: str  # already lowercased
    priority: int


def clean(raw: str) -> str:
    """Normalise a raw transaction description for embedding / matching."""
    s = raw
    for pat, repl in _CLEAN_PATTERNS:
        s = pat.sub(repl, s)
    return s.strip().lower()


def load_rules(path: Path = RULES_CSV) -> list[Rule]:
    if not path.exists():
        return []
    rules: list[Rule] = []
    with path.open() as f:
        for row in csv.DictReader(f):
            rules.append(
                Rule(
                    category_id=row["category_id"],
                    pattern=row["pattern"].lower(),
                    priority=int(row["priority"]),
                )
            )
    rules.sort(key=lambda r: r.priority)
    return rules


def match_rule(cleaned: str, rules: list[Rule]) -> str | None:
    for rule in rules:
        if rule.pattern in cleaned:
            return rule.category_id
    return None


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


def _insert_merchant(
    con: duckdb.DuckDBPyConnection,
    canonical_name: str,
    category_id: str,
    embedding: list[float] | None,
    now: datetime,
) -> None:
    con.execute(
        """
        INSERT INTO silver.dim_merchants
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [str(uuid.uuid4()), canonical_name, category_id, embedding, now, now],
    )


def process(con: duckdb.DuckDBPyConnection, client: EmbeddingClient, batch: int = 100) -> None:
    ensure_schema(con)
    rules = load_rules()
    todo = unresolved_descriptions(con)
    if not todo:
        return
    now = datetime.now(UTC)

    existing_names: set[str] = {
        r[0]
        for r in con.execute("SELECT canonical_name FROM silver.dim_merchants").fetchall()
    }

    # Phase 1 — deterministic rule pre-pass on cleaned descriptions.
    # Everything that rules resolve skips the embedding call entirely.
    # Dedup on cleaned canonical_name so "safeway #123 toronto on" and
    # "safeway #456 mississauga on" collapse to a single merchant row.
    remaining: list[tuple[str, str]] = []  # (raw, cleaned)
    for raw in todo:
        cleaned = clean(raw)
        if not cleaned:
            continue
        if cleaned in existing_names:
            continue
        cat = match_rule(cleaned, rules)
        if cat is not None:
            _insert_merchant(con, cleaned, cat, None, now)
            existing_names.add(cleaned)
        else:
            remaining.append((raw, cleaned))

    # Phase 2 — embed what's left, ANN-match against existing merchants.
    for i in range(0, len(remaining), batch):
        chunk = remaining[i : i + batch]
        cleaned_chunk = [c for _, c in chunk]
        embeddings = client.embed(cleaned_chunk)
        for (raw, cleaned), emb in zip(chunk, embeddings, strict=True):
            match = nearest_merchant(con, emb)
            if match and match[1] <= AUTO_MATCH_MAX:
                continue
            if match is None or match[1] >= NEW_MERCHANT_MIN:
                _insert_merchant(con, cleaned, "uncategorised", emb, now)
            else:
                con.execute(
                    """
                    INSERT INTO silver.merchant_review_queue VALUES (?, ?, ?, ?, ?)
                    """,
                    [str(uuid.uuid4()), raw, match[0], match[1], now],
                )
