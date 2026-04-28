"""
Finance Lake tools for OpenWebUI.

Install via Settings → Tools → + and paste this file.
Pair with claude-sonnet-4-6 or claude-opus-4-7 — smaller models miss the joins.

The model should call finance_describe() first in any new conversation to load
the schema, then use finance_sql() for all data queries.
"""

import os
import json
import duckdb
from typing import Any

DUCKDB_PATH = os.getenv("FINANCE_DUCKDB", "/var/lib/finance-lake/finance.duckdb")

SCHEMA_HINT = """
## Finance Lake — silver/gold schema

### silver.fact_transactions
- transaction_id VARCHAR (md5 surrogate)
- holder VARCHAR          -- raw name from statement header (e.g. "LORCAN TRAVERS")
- account_id VARCHAR      -- account/card number
- account_name VARCHAR    -- friendly name from dim_accounts
- account_kind VARCHAR    -- 'chequing' | 'savings' | 'credit_card'
- txn_date DATE
- amount DOUBLE           -- positive = income/deposit, negative = spend/payment
- raw_description VARCHAR
- clean_description VARCHAR
- merchant_id VARCHAR     -- FK to dim_merchants (nullable)
- source_system VARCHAR   -- 'bank' | 'credit_card'
- day_of_week_name VARCHAR
- is_weekend BOOLEAN
- is_transfer BOOLEAN     -- always false until transfer-matching lands

### silver.dim_merchants
- merchant_id VARCHAR
- canonical_name VARCHAR  -- normalised merchant name
- category_id VARCHAR     -- FK to dim_categories seed
- match_method VARCHAR    -- 'rule' | 'embedding' | 'manual'

### silver.dim_accounts
- account_id VARCHAR
- source_system VARCHAR
- friendly_name VARCHAR
- account_kind VARCHAR
- inversion_factor INT    -- 1 for bank, -1 for credit_card (flips CC sign convention)

### gold.spending_by_category
- month DATE              -- first day of month
- category_id VARCHAR
- spend DOUBLE            -- always positive (outflows only)
- txn_count BIGINT

### gold.cash_flow_monthly
- month DATE
- total_income DOUBLE
- total_spending DOUBLE
- net_savings DOUBLE
- savings_rate DOUBLE     -- 0–1 fraction

### gold.disposable_income
- month DATE
- total_income DOUBLE
- essential_spending DOUBLE   -- groceries + rent/mortgage + utilities
- disposable_income DOUBLE

### gold.net_worth_daily
- as_of_date DATE
- total_assets DOUBLE     -- Questrade portfolio market value only (v1)
- total_liabilities DOUBLE  -- always 0 until bank balance extraction lands
- net_worth DOUBLE

### Notes
- Amounts: negative = spend, positive = income (already inverted via inversion_factor)
- Date range: ~2021-12 → present
- Owners: lorcan, grace, joint (inferred via dim_holders seed, not directly in silver)
- Transfers not yet matched — avoid double-counting by filtering `where not is_transfer`
  (already applied in gold models)
"""


class Tools:
    def __init__(self):
        pass

    def finance_describe(self) -> str:
        """
        Returns the Finance Lake schema — column names, types, grain, and conventions.
        Call this at the start of any finance conversation so you understand the data
        before writing SQL. No arguments needed.
        """
        return SCHEMA_HINT

    def finance_sql(self, query: str) -> str:
        """
        Run a read-only SQL query against the Finance Lake DuckDB database and return
        the results as JSON. The database contains personal finance data: transactions,
        merchants, accounts, spending categories, net worth, and cash flow.

        Always call finance_describe() first if you haven't loaded the schema yet.

        Guidelines:
        - Use silver.fact_transactions for transaction-level questions
        - Use gold.spending_by_category for category spend summaries
        - Use gold.cash_flow_monthly for income/savings rate questions
        - Use gold.net_worth_daily for portfolio/wealth questions
        - Amounts: negative = spend, positive = income
        - Filter `where amount < 0` for spending; `where amount > 0` for income
        - Limit results to ≤ 200 rows unless the user asks for more

        :param query: A valid DuckDB SQL SELECT statement (read-only).
        :return: JSON array of result rows, or an error message.
        """
        query = query.strip()

        # Reject anything that isn't a read operation
        first_word = query.split()[0].upper() if query.split() else ""
        if first_word not in ("SELECT", "WITH", "SHOW", "DESCRIBE", "EXPLAIN"):
            return json.dumps({"error": "Only SELECT/WITH/SHOW/DESCRIBE queries are allowed."})

        try:
            con = duckdb.connect(DUCKDB_PATH, read_only=True)
            result_set = con.execute(query)
            cols = [d[0] for d in result_set.description]
            rows = result_set.fetchall()
            con.close()
        except Exception as e:
            return json.dumps({"error": str(e)})

        result = [dict(zip(cols, row)) for row in rows]
        return json.dumps(result, default=str, indent=2)
