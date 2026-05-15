# foundry

Decision intelligence data lake for Project Foundry. Captures theses before outcomes are known, tracks results, measures calibration.

## Architecture

```
ingest/
  _lib/bronze.py        # land() — only function that writes to /lake/bronze/
  banking/statements.py # wraps statement-extract + land()
  brokers/questrade.py  # wraps questrade-extract + land()
  obsidian/vault.py     # lands markdown notes into bronze (future)
  core.py               # storage-agnostic PDF → DuckDB ingest (existing)
  adapters/             # Paperless, local, S3 adapters (existing)

dbt/                    # silver + gold models (lives in models/ + seeds/)
  silver/ledger/        # fact_transactions, dim_accounts, dim_merchants
  silver/investment/    # investment_theses, prediction_market_bets (future)
  gold/analytics/       # spending_by_category, cash_flow_monthly, net_worth_daily
  gold/calibration/     # calibration_by_domain, decision_vs_outcome (future)

embed_enrich/           # merchant normalisation via rules + OpenAI embeddings

openwebui_tools/        # finance_tools.py — DuckDB query tool for Open-WebUI
```

## Bronze layout

```
$LAKE_ROOT/bronze/<domain>/<source>/<YYYY-MM-DD>/<filename>
$LAKE_ROOT/bronze/<domain>/<source>/<YYYY-MM-DD>/<filename>.meta.json
```

- `LAKE_ROOT` defaults to `/var/lib/foundry/lake` (prod) or override via env var for Mac dev
- Every `.meta.json` contains `sha256`, `ingested_at`, `size_bytes`, `source`
- Never write to bronze directly — always call `ingest._lib.bronze.land()`

## DuckDB paths

- Dev (Mac): `~/.local/share/foundry/finance.duckdb`
- Prod (OptiPlex): `/var/lib/foundry/lake/silver/finance.duckdb`
- Set via `DBT_TARGET=prod` or `FINANCE_DUCKDB` env var

## Adding a new ingest source

1. Create `ingest/<domain>/<source>.py` — call `land(domain, source, path)` then insert into DuckDB if structured
2. Add a systemd timer/path unit in `nix/lake-ingest.nix` (exported by the flake)
3. If it needs a silver model, add under `models/silver/<domain>/`
4. Add ODCS contract YAML under `contracts/` (future)

## Key conventions

- `statement-extract` and `questrade-extract` are library deps — never copy their logic here
- Thesis-bearing data (investment notes, prediction market bets) must have: source, timestamp, confidence (0–1), thesis text
- All ingest scripts read `LAKE_ROOT` from env — never hardcode paths
- British English in comments and docs

## Testing

```bash
# Mac dbt run
dbt run --target dev

# Full pytest
pytest

# Verify a PDF lands correctly
LAKE_ROOT=/tmp/lake-test python -c "
from pathlib import Path
from ingest._lib.bronze import land
dest = land('banking', 'test', Path('some.pdf'))
print(dest)
"
```

## Deployed by

`nix-config/modules/optiplex/foundry.nix` — systemd services, secrets, Caddy.
`nix-config/flake.nix` — input pinned to `github:lorcan17/foundry`.
