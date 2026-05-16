{{ config(materialized='table') }}

-- Daily net worth = sum of bank account closing balances, forward-filled
-- between statement dates so we get a continuous daily series from the
-- earliest statement to today.
--
-- v2 design (2026-04-28): bank-only. Each statement contributes its
-- closing_balance to every day from period_end through the day before the
-- next statement's period_end (or current_date if it's the latest).
-- Questrade and credit cards are intentionally excluded — Questrade
-- snapshots are sparse and have their own page; CC balances aren't
-- liabilities here yet (covered in a follow-up).

with statement_balances as (
    select
        holder,
        bank,
        account_number,
        period_end as as_of_date,
        closing_balance
    from {{ source('bronze', 'bank_statements') }}
    where closing_balance is not null
),

-- Each (account, statement) gets its window: [period_end, next period_end).
-- For the latest statement, the window extends to current_date + 1 day.
account_windows as (
    select
        holder,
        bank,
        account_number,
        as_of_date as window_start,
        coalesce(
            lead(as_of_date) over (
                partition by holder, account_number
                order by as_of_date
            ),
            current_date + interval 1 day
        ) as window_end,
        closing_balance
    from statement_balances
),

date_spine as (
    select
        unnest(
            generate_series(
                (select min(as_of_date) from statement_balances)::date,
                current_date,
                interval 1 day
            )
        )::date as as_of_date
),

ffill as (
    select
        d.as_of_date,
        w.holder,
        w.bank,
        w.account_number,
        w.closing_balance
    from date_spine d
    inner join account_windows w
       on d.as_of_date >= w.window_start
      and d.as_of_date <  w.window_end
)

select
    as_of_date,
    sum(closing_balance) as total_assets,
    cast(0 as double) as total_liabilities,
    sum(closing_balance) as net_worth,
    count(distinct holder || '|' || account_number) as n_accounts_contributing
from ffill
group by as_of_date
order by as_of_date
