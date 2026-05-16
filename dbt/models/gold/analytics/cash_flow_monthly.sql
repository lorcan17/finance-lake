{{ config(materialized='table') }}

-- Monthly income, spending, and savings rate.
-- Excludes transfers to avoid double-counting.

with semantic as (
    select * from {{ ref('semantic_transactions') }}
    where not is_transfer
)

select
    date_trunc('month', transaction_date) as month,
    sum(case when amount > 0 then amount else 0 end) as total_income,
    sum(case when amount < 0 then -amount else 0 end) as total_spending,
    sum(amount) as net_savings,
    case 
        when sum(case when amount > 0 then amount else 0 end) > 0 
        then sum(amount) / sum(case when amount > 0 then amount else 0 end)
        else null 
    end as savings_rate
from semantic
group by 1
order by 1 desc
