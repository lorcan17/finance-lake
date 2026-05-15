"""Bronze landing — the single entry point for writing raw files to the lake.

Every ingest script calls land() and nothing else. Path convention:
    $LAKE_ROOT/bronze/<domain>/<source>/<YYYY-MM-DD>/<filename>
    $LAKE_ROOT/bronze/<domain>/<source>/<YYYY-MM-DD>/<filename>.meta.json

LAKE_ROOT defaults to /var/lib/foundry/lake (prod) but is overridden via env
var for Mac dev (e.g. LAKE_ROOT=~/.local/share/foundry/lake).
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

LAKE_ROOT = Path(os.environ.get("LAKE_ROOT", "/var/lib/foundry/lake"))


def land(
    domain: str,
    source: str,
    artifact: Path,
    source_timestamp: datetime | None = None,
    extra: dict | None = None,
) -> Path:
    """Copy artifact into bronze with a provenance sidecar. Returns destination path.

    Idempotent by filename within a day — a second call with the same artifact
    overwrites the previous copy (sha256 in the sidecar will reflect the new file).
    For true dedup, check the sha256 before calling.
    """
    now = datetime.now(timezone.utc)
    target_dir = LAKE_ROOT / "bronze" / domain / source / now.strftime("%Y-%m-%d")
    target_dir.mkdir(parents=True, exist_ok=True)

    target = target_dir / artifact.name
    shutil.copy2(artifact, target)

    sha = _sha256(target)
    meta = {
        "source": f"{domain}/{source}",
        "ingested_at": now.isoformat(),
        "source_timestamp": source_timestamp.isoformat() if source_timestamp else None,
        "filename": artifact.name,
        "size_bytes": target.stat().st_size,
        "sha256": sha,
        "extra": extra or {},
    }
    target.with_suffix(target.suffix + ".meta.json").write_text(
        json.dumps(meta, indent=2)
    )
    return target


def sha256_of(path: Path) -> str:
    return _sha256(path)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()
