{{ config(materialized='table') }}

-- Total assets minus liabilities by day. v1: portfolio market value from
-- Questrade snapshots only; bank/CC balances join in once balance extraction
-- is added (currently transactions-only).

with portfolio_daily as (
    select
        snapshot_date as as_of_date,
        sum(market_value) as portfolio_value
    from {{ source('bronze', 'questrade_snapshots') }}
    group by snapshot_date
)

select
    as_of_date,
    portfolio_value as total_assets,
    cast(0 as double) as total_liabilities,
    portfolio_value as net_worth
from portfolio_daily
order by as_of_date
