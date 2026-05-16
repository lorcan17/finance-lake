{{ config(materialized='table') }}

-- Monthly spend per category. Categorisation now lives on fact_transactions
-- (override > merchant > substring rule > default) — see fact_transactions.sql.
-- Income (positive amounts) is excluded via amount < 0; transfers are
-- excluded via the is_transfer flag.

with txns as (
    select
        date_trunc('month', txn_date) as month,
        category_id,
        amount
    from {{ ref('fact_transactions') }}
    where amount < 0
      and not is_transfer
)

select
    month,
    category_id,
    sum(-amount) as spend,
    count(*) as txn_count
from txns
group by month, category_id
order by month desc, spend desc
