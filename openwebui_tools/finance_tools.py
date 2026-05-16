"""
title: Finance Lake
author: lorcan
description: Query personal finance data (transactions, net worth, spending) via DuckDB.
version: 0.1.0
"""

import os
import json
import duckdb

DUCKDB_PATH = os.getenv("FINANCE_DUCKDB", "/var/lib/foundry/foundry.duckdb")

SCHEMA_HINT = """
## Finance Lake — silver/gold schema

### main_silver.fact_transactions
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

### main_silver.dim_merchants
- merchant_id VARCHAR
- canonical_name VARCHAR  -- normalised merchant name
- category_id VARCHAR     -- FK to dim_categories seed
- match_method VARCHAR    -- 'rule' | 'embedding' | 'manual'

### main_silver.dim_accounts
- account_id VARCHAR
- source_system VARCHAR
- friendly_name VARCHAR
- account_kind VARCHAR
- inversion_factor INT    -- 1 for bank, -1 for credit_card (flips CC sign convention)

### main_gold.spending_by_category
- month DATE              -- first day of month
- category_id VARCHAR
- spend DOUBLE            -- always positive (outflows only)
- txn_count BIGINT

### main_gold.cash_flow_monthly
- month DATE
- total_income DOUBLE
- total_spending DOUBLE
- net_savings DOUBLE
- savings_rate DOUBLE     -- 0–1 fraction

### main_gold.disposable_income
- month DATE
- total_income DOUBLE
- essential_spending DOUBLE   -- groceries + rent/mortgage + utilities
- disposable_income DOUBLE

### main_gold.net_worth_daily
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

    async def finance_describe(self) -> str:
        """
        Returns the Finance Lake schema — column names, types, grain, and conventions.
        Call this at the start of any finance conversation so you understand the data
        before writing SQL. No arguments needed.
        """
        return SCHEMA_HINT

    async def finance_sql(self, sql: str) -> str:
        """
        Run a read-only SQL query against the Finance Lake DuckDB database and return
        the results as JSON. The database contains personal finance data: transactions,
        merchants, accounts, spending categories, net worth, and cash flow.

        Always call finance_describe() first if you haven't loaded the schema yet.

        Guidelines:
        - Use main_silver.fact_transactions for transaction-level questions
        - Use main_gold.spending_by_category for category spend summaries
        - Use main_gold.cash_flow_monthly for income/savings rate questions
        - Use main_gold.net_worth_daily for portfolio/wealth questions
        - Amounts: negative = spend, positive = income
        - Filter `where amount < 0` for spending; `where amount > 0` for income
        - Limit results to ≤ 200 rows unless the user asks for more

        :param sql: A valid DuckDB SQL SELECT statement (read-only).
        :return: JSON array of result rows, or an error message.
        """
        sql = sql.strip()

        # Reject anything that isn't a read operation
        first_word = sql.split()[0].upper() if sql.split() else ""
        if first_word not in ("SELECT", "WITH", "SHOW", "DESCRIBE", "EXPLAIN"):
            return json.dumps({"error": "Only SELECT/WITH/SHOW/DESCRIBE queries are allowed."})

        try:
            con = duckdb.connect(DUCKDB_PATH, read_only=True)
            df = con.execute(sql).fetchdf()
            con.close()
            return df.to_json(orient="records", date_format="iso")
        except Exception as e:
            return json.dumps({"error": str(e)})
