# Seeds

dbt seed files. Real seeds are **not committed** — they contain personal data
(budgets reveal income, merchant rules reveal location and shopping habits).

## Files

| File | Status | Purpose |
|---|---|---|
| `dim_categories.csv` | gitignored | Spend categories used by Gold models |
| `dim_budgets.csv` | gitignored | Per-category monthly budget targets (CAD) |
| `dim_category_rules.csv` | gitignored | Substring → category rules used by `embed_enrich` |
| `*.example.csv` | committed | Generic templates showing the schema |

## Local development

```bash
# First-time setup: copy templates and personalise
cp seeds/dim_categories.example.csv     seeds/dim_categories.csv
cp seeds/dim_budgets.example.csv        seeds/dim_budgets.csv
cp seeds/dim_category_rules.example.csv seeds/dim_category_rules.csv
# Edit the .csv files (not the .example.csv) with your real data.
```

The personalised `*.csv` files are gitignored, so they will not be committed.

## Production (OptiPlex via nix-config)

The `finance-lake` systemd unit (in `modules/optiplex/foundry.nix`) populates
`seeds/` from `/var/lib/finance-lake/seeds/` before invoking `dbt seed`. Real
seeds live outside the repo on the host and are managed manually (or restored
from backup).
