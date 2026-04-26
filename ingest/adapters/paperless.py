"""Paperless-ngx post-consume hook adapter.

Invoked by Paperless `PAPERLESS_POST_CONSUME_SCRIPT` after OCR completes.
Reads doc id + working path from env, ingests the PDF into bronze, and PATCHes
the Paperless document with derived metadata so its filename layout matches
`statement-extract`'s archive convention.

Env contract (Paperless-provided):
- DOCUMENT_ID                  : numeric Paperless doc id
- DOCUMENT_WORKING_PATH        : local path to the freshly-OCR'd PDF
- DOCUMENT_CONTENT             : pre-OCR'd text (unused for now; reserved for
                                 fast bank-detect optimisation)

Env contract (deployment-provided):
- PAPERLESS_URL                : base URL, e.g. http://localhost:28981
- PAPERLESS_API_TOKEN          : token minted in Paperless UI (optional —
                                 if unset, ingest still happens, PATCH is skipped)
- FINANCE_DUCKDB               : path to finance.duckdb
- DIM_HOLDERS_CSV              : path to dim_holders.csv seed (holder_raw,owner).
                                 Optional; if unset or holder absent, owner
                                 falls back to "_unowned" and the doc surfaces
                                 in silver.review_unmapped_holders.
"""
from __future__ import annotations

import csv
import os
import sys
from pathlib import Path

import httpx

from embed_enrich.duckdb_conn import connect

from ..core import ingest_pdf
from ..sources import IngestResult, SourceRef


def main() -> int:
    doc_id = os.environ.get("DOCUMENT_ID")
    working = os.environ.get("DOCUMENT_WORKING_PATH")
    if not doc_id or not working:
        print("paperless-hook: DOCUMENT_ID/DOCUMENT_WORKING_PATH not set", file=sys.stderr)
        return 2

    pdf = Path(working)
    source = SourceRef(type="paperless", id=str(doc_id))

    con = connect()
    try:
        result = ingest_pdf(pdf, source, con)
    finally:
        con.close()

    if not result.was_finance_doc:
        print(f"paperless-hook: doc {doc_id} not a finance statement, no-op")
        return 0

    print(
        f"paperless-hook: doc {doc_id} ingested "
        f"(bank={result.bank_product} last4={result.last4} "
        f"new_row={result.was_new_row} issues={len(result.validation_issues)})"
    )

    token = os.environ.get("PAPERLESS_API_TOKEN")
    base = os.environ.get("PAPERLESS_URL")
    if token and base:
        _patch_paperless(base, token, int(doc_id), result)
    else:
        print("paperless-hook: PAPERLESS_URL/API_TOKEN unset, skipping metadata PATCH")
    return 0


def _patch_paperless(base: str, token: str, doc_id: int, result: IngestResult) -> None:
    owner = _resolve_owner(result.holder or "")
    payload = {
        "title": _title_for(result),
        "created": result.period_end.isoformat() if result.period_end else None,
        "custom_fields": [
            {"field": _field_id("owner"), "value": owner},
            {"field": _field_id("last4"), "value": result.last4 or ""},
        ],
    }
    payload = {k: v for k, v in payload.items() if v is not None}

    headers = {"Authorization": f"Token {token}", "Accept": "application/json"}
    r = httpx.patch(
        f"{base.rstrip('/')}/api/documents/{doc_id}/",
        json=payload,
        headers=headers,
        timeout=15,
    )
    r.raise_for_status()


def _resolve_owner(holder: str) -> str:
    """Look up holder_raw → owner in the dim_holders.csv seed.

    Unmapped holders return "_unowned"; they surface in
    `silver.review_unmapped_holders` for triage and CSV update.
    """
    csv_path = os.environ.get("DIM_HOLDERS_CSV")
    if not csv_path or not Path(csv_path).exists():
        return "_unowned"
    with open(csv_path, newline="") as f:
        for row in csv.DictReader(f):
            if row.get("holder_raw") == holder:
                return row.get("owner") or "_unowned"
    return "_unowned"


def _title_for(result: IngestResult) -> str:
    parts = [result.bank_product or "statement"]
    if result.period_end:
        parts.append(result.period_end.isoformat())
    return " ".join(parts)


_FIELD_CACHE: dict[str, int] = {}


def _field_id(name: str) -> int:
    """Look up a custom field's numeric id by name; cached for the process.

    Resolved lazily on first PATCH so the hook works against a freshly-set-up
    Paperless instance regardless of field-id assignment order.
    """
    if name in _FIELD_CACHE:
        return _FIELD_CACHE[name]
    base = os.environ["PAPERLESS_URL"].rstrip("/")
    token = os.environ["PAPERLESS_API_TOKEN"]
    r = httpx.get(
        f"{base}/api/custom_fields/",
        headers={"Authorization": f"Token {token}", "Accept": "application/json"},
        timeout=15,
    )
    r.raise_for_status()
    for f in r.json()["results"]:
        _FIELD_CACHE[f["name"]] = f["id"]
    return _FIELD_CACHE[name]


if __name__ == "__main__":
    sys.exit(main())
