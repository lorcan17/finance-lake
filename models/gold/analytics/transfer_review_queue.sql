{{ config(materialized='view') }}

-- Surface large unmatched transactions (>= $500) that aren't flagged as transfers.
-- These are the primary suspects for inflating income/spending totals.

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
  and abs(amount) >= 500
order by abs(amount) desc
