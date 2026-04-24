{{ config(materialized='view') }}

-- Data Completeness Review.
-- Identifies months where an active account is expected to have data but none was found.

with date_range as (
    select 
        min(date_trunc('month', start_date)) as min_month,
        date_trunc('month', current_date) as max_month
    from {{ ref('dim_accounts') }}
    where is_active
),

months as (
    -- Generate all months from min start_date to now
    with recursive month_gen as (
        select min_month as month from date_range
        union all
        select month + interval '1 month'
        from month_gen
        where month < (select max_month from date_range)
    )
    select month from month_gen
),

expected_accounts_per_month as (
    select
        m.month,
        a.account_id,
        a.friendly_name,
        a.account_kind
    from months m
    cross join {{ ref('dim_accounts') }} a
    where a.is_active
      and m.month >= date_trunc('month', a.start_date)
),

actual_data as (
    select
        date_trunc('month', txn_date) as month,
        account_id,
        count(*) as txn_count,
        count(distinct source_pdf) as statement_count
    from {{ ref('fact_transactions') }}
    group by 1, 2
)

select
    cast(e.month as timestamp) as month,
    e.friendly_name,
    e.account_kind,
    coalesce(a.txn_count, 0) as txn_count,
    coalesce(a.statement_count, 0) as statement_count,
    case 
        when a.txn_count is null or a.txn_count = 0 then 'MISSING'
        else 'PRESENT'
    end as status
from expected_accounts_per_month e
left join actual_data a on e.month = a.month and e.account_id = a.account_id
order by e.month desc, e.friendly_name
