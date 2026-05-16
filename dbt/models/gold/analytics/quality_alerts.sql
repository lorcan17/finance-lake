{{ config(materialized='view') }}

-- Quality Review: Detects potential duplicates and spending outliers.

with txns as (
    select 
        t.*,
        coalesce(m.category_id, 'uncategorised') as category_id
    from {{ ref('fact_transactions') }} t
    left join {{ ref('dim_merchants') }} m on t.merchant_id = m.merchant_id
),

duplicates as (
    select
        txn_date,
        amount,
        account_id,
        clean_description,
        count(*) as occurrence_count,
        array_agg(transaction_id) as txn_ids
    from txns
    group by 1, 2, 3, 4
    having count(*) > 1
),

category_stats as (
    select
        category_id,
        avg(abs(amount)) as avg_amount,
        stddev(abs(amount)) as stddev_amount
    from txns
    where amount < 0 and not is_transfer
    group by 1
)

select
    'POTENTIAL_DUPLICATE' as alert_type,
    t.txn_date,
    t.amount,
    t.account_name,
    t.raw_description,
    'Found ' || d.occurrence_count || ' identical transactions' as alert_message
from txns t
join duplicates d on t.txn_date = d.txn_date 
    and t.amount = d.amount 
    and t.account_id = d.account_id 
    and t.clean_description = d.clean_description

union all

select
    'SPENDING_OUTLIER' as alert_type,
    t.txn_date,
    t.amount,
    t.account_name,
    t.raw_description,
    'Amount is ' || round(abs(t.amount) / s.avg_amount, 1) || 'x higher than ' || t.category_id || ' average' as alert_message
from txns t
join category_stats s on t.category_id = s.category_id
where abs(t.amount) > (s.avg_amount + 3 * s.stddev_amount)
  and abs(t.amount) > 100 -- Ignore small noise
  and not t.is_transfer
