{{ config(materialized='view') }}

-- Surface large uncategorised transactions (>= $5,000) that aren't flagged as transfers.
-- These need manual review — Interac receipts, large deposits, one-off spending.

select
    transaction_date,
    amount,
    account_name,
    merchant_name,
    raw_description,
    category_name,
    source_system
from {{ ref('semantic_transactions') }}
where not is_transfer
  and category_name = 'Uncategorised'
  and abs(amount) >= 5000
order by abs(amount) desc
