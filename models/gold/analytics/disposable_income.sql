{{ config(materialized='table') }}

-- Disposable income = Total Income - Essential Spending.
-- Essential categories: groceries, rent, utilities.

with semantic as (
    select * from {{ ref('semantic_transactions') }}
    where not is_transfer
),

monthly_summary as (
    select
        date_trunc('month', transaction_date) as month,
        sum(case when amount > 0 then amount else 0 end) as total_income,
        sum(case 
            when amount < 0 and category_name in ('Groceries', 'Rent / Mortgage', 'Utilities') 
            then -amount 
            else 0 
        end) as essential_spending
    from semantic
    group by 1
)

select
    month,
    total_income,
    essential_spending,
    total_income - essential_spending as disposable_income
from monthly_summary
order by month desc
