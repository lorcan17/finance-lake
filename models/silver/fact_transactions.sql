{{ config(materialized='table') }}

-- Unified transaction grain across bank + credit card. Investment activity
-- (Questrade) is not a transaction stream in v1 — positions only. Extend later.

with bank_numbered as (
    select
        *,
        row_number() over (
            partition by owner, account_number, txn_date, amount, raw_description, running_balance
            order by source_pdf
        ) as dup_seq
    from {{ source('bronze', 'bank_transactions') }}
),

bank as (
    select
        md5(concat_ws('|', owner, account_number, txn_date, amount, raw_description, running_balance, dup_seq)) as transaction_id,
        owner,
        account_number as account_id,
        txn_date,
        amount,
        raw_description,
        cast(null as varchar) as merchant_id,
        'bank' as source_system
    from bank_numbered
),

cc_numbered as (
    select
        *,
        row_number() over (
            partition by owner, card_number, txn_date, amount, raw_description
            order by posting_date, source_pdf
        ) as dup_seq
    from {{ source('bronze', 'cc_transactions') }}
),

cc as (
    select
        md5(concat_ws('|', owner, card_number, txn_date, amount, raw_description, dup_seq)) as transaction_id,
        owner,
        card_number as account_id,
        txn_date,
        amount,
        raw_description,
        cast(null as varchar) as merchant_id,
        'credit_card' as source_system
    from cc_numbered
)

select * from bank
union all select * from cc
