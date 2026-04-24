{{ config(materialized='table') }}

-- Monthly spend per category. Transfers are excluded once transfer-matching
-- lands; for now all outflows count. Income (positive amounts on bank) is
-- filtered out via amount < 0 convention.

with txns as (
    select
        t.transaction_id,
        date_trunc('month', t.txn_date) as month,
        t.amount,
        coalesce(m.category_id, 'uncategorised') as category_id
    from {{ ref('fact_transactions') }} t
    left join {{ ref('dim_merchants') }} m on m.merchant_id = t.merchant_id
    where t.amount < 0
)

select
    month,
    category_id,
    sum(-amount) as spend,
    count(*) as txn_count
from txns
group by month, category_id
order by month desc, spend desc
