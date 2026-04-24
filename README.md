# finance-lake

Silver + Gold layer and `embed_enrich` service for [Project Foundry](../../nix-config/projects/foundry/SPEC.md).

## Layout

- `models/silver/` — dbt models for curated layer (dim_accounts, dim_merchants, fact_transactions)
- `models/gold/` — dbt models for analytics (net_worth_daily, spending_by_category)
- `seeds/` — dim_categories, dim_budgets
- `embed_enrich/` — Python service: raw description → OpenAI embedding → ANN match → dim_merchants / review queue
- `scripts/dev_bootstrap.py` — scp OptiPlex SQLite + sample PDFs into local bronze (Mac dev only)

## Dev loop (Mac, per ADR-003)

```bash
direnv allow                              # fills OPENAI_API_KEY
uv sync
uv run python scripts/dev_bootstrap.py    # seeds bronze from OptiPlex
uv run python -m embed_enrich             # populates silver.dim_merchants
uv run dbt run --target dev
uv run pytest
```

## Targets

| Target | DuckDB path | Where |
|---|---|---|
| `dev` | `~/.local/share/finance-lake/finance.duckdb` | Mac |
| `prod` | `/var/lib/finance-lake/finance.duckdb` | OptiPlex |

## Notes

- `vss` extension is loaded in `on-run-start` with `hnsw_enable_experimental_persistence = true` (ADR-004).
- Embeddings via OpenAI `text-embedding-3-small` (ADR-002), 1536 dimensions.
