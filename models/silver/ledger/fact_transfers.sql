{{ config(materialized='table') }}

-- Matches internal transfers (e.g., CC payments, savings transfers).
-- Criteria:
-- 1. Opposite signs (one credit, one debit).
-- 2. Similar amounts (within 0.05).
-- 3. Dates within a 3-day window (10 for large txns).
-- 4. Different accounts (except for loops/cancellations).

with txns as (
    select * from {{ ref('fact_transactions') }}
),

candidate_matches as (
    select
        t1.transaction_id as debit_txn_id,
        t2.transaction_id as credit_txn_id,
        t1.amount as amount,
        abs(date_diff('day', t1.txn_date, t2.txn_date)) as day_gap,
        abs(t1.amount + t2.amount) as amount_gap
    from txns t1
    join txns t2 on
        t1.amount < 0
        and t2.amount > 0
        and abs(t1.amount + t2.amount) <= 0.05
        and (
            -- Case A: Cross-account transfer (Bank -> CC, Bank -> Savings)
            (t1.account_id != t2.account_id and (
                (abs(t1.amount) < 500 and abs(date_diff('day', t1.txn_date, t2.txn_date)) <= 3)
                OR
                (abs(t1.amount) >= 500 and abs(date_diff('day', t1.txn_date, t2.txn_date)) <= 10)
            ))
            OR
            -- Case B: Same-account loop (Cancellations, Reversals, Bill Payment Rejected)
            (t1.account_id = t2.account_id and abs(date_diff('day', t1.txn_date, t2.txn_date)) <= 7)
        )
),

-- Enforce 1:1 debit↔credit matching so a single txn cannot fan out in
-- semantic_transactions. Each side keeps only its closest-date partner.
matches as (
    select debit_txn_id, credit_txn_id, amount
    from (
        select
            *,
            row_number() over (
                partition by debit_txn_id order by day_gap, amount_gap, credit_txn_id
            ) as debit_rank,
            row_number() over (
                partition by credit_txn_id order by day_gap, amount_gap, debit_txn_id
            ) as credit_rank
        from candidate_matches
    )
    where debit_rank = 1 and credit_rank = 1
),

investment_transfers as (
    -- Case C: One-way investment transfers (external buckets we don't ingest).
    -- Restricted to outbound amounts — inbound dividends/refunds are real income.
    select
        transaction_id as debit_txn_id,
        cast(null as varchar) as credit_txn_id,
        amount
    from txns
    where amount < 0
      and (
          lower(raw_description) like '%questrade%'
          or lower(raw_description) like '%wealthsimple%'
          or lower(raw_description) like '%ws investment%'
          or lower(raw_description) like '%ws invest%'
      )
),

combined as (
    select debit_txn_id, credit_txn_id from matches
    union all
    select debit_txn_id, credit_txn_id from investment_transfers
)

select
    debit_txn_id as transaction_id,
    'transfer' as transfer_type,
    credit_txn_id as matched_transaction_id
from combined
where debit_txn_id is not null
union all
select
    credit_txn_id as transaction_id,
    'transfer' as transfer_type,
    debit_txn_id as matched_transaction_id
from combined
where credit_txn_id is not null
