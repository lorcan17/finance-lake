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

transfer_rules as (
    select pattern, direction from {{ ref('dim_transfer_rules') }}
),

one_way_transfers as (
    -- Case C: One-way outbound transfers to external accounts we don't ingest.
    -- Patterns are maintained in seeds/dim_transfer_rules.csv — edit there, not here.
    -- Inbound direction reserved for future use (e.g. flagging inbound wire receipts).
    -- Excludes transactions already paired in matches to avoid duplicates in combined.
    select distinct
        t.transaction_id as debit_txn_id,
        cast(null as varchar) as credit_txn_id,
        t.amount
    from txns t
    join transfer_rules r
      on t.amount < 0
      and r.direction = 'outbound'
      and t.clean_description like '%' || r.pattern || '%'
    where t.transaction_id not in (select debit_txn_id from matches)
),

combined as (
    select debit_txn_id, credit_txn_id from matches
    union all
    select debit_txn_id, credit_txn_id from one_way_transfers
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
