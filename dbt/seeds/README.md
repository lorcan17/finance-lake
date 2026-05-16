# Seeds

dbt seed files. Real seeds are **not committed** — they contain personal data
(budgets reveal income, merchant rules reveal location and shopping habits).

## Files

| File | Status | Purpose |
|---|---|---|
| `dim_categories.csv` | **committed** | Spend categories used by Gold models (generic taxonomy, no personal data) |
| `dim_budgets.csv` | gitignored | Per-category monthly budget targets (CAD) |
| `dim_category_rules.csv` | gitignored | Substring → category rules used by `embed_enrich` |
| `dim_account_normalization.csv` | gitignored | Friendly names + +/- signage correction |
| `*.example.csv` | committed | Generic templates showing the schema |

## Local development

```bash
# First-time setup: copy templates and personalise
cp seeds/dim_budgets.example.csv               seeds/dim_budgets.csv
cp seeds/dim_category_rules.example.csv        seeds/dim_category_rules.csv
cp seeds/dim_account_normalization.example.csv seeds/dim_account_normalization.csv
# Edit the .csv files (not the .example.csv) with your real data.
```

The personalised `*.csv` files are gitignored, so they will not be committed.

## Production (OptiPlex via nix-config)

The `finance-lake` systemd unit (in `modules/optiplex/foundry.nix`) populates
`seeds/` from `/var/lib/finance-lake/seeds/` before invoking `dbt seed`. Real
seeds live outside the repo on the host and are managed manually (or restored
from backup).
