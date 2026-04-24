{{ config(materialized='table') }}

-- Unified transaction grain across bank + credit card. Investment activity
-- (Questrade) is not a transaction stream in v1 — positions only. Extend later.

with bank as (
    select
        md5(concat_ws('|', owner, account_number, txn_date, amount, raw_description)) as transaction_id,
        owner,
        account_number as account_id,
        txn_date,
        amount,
        raw_description,
        cast(null as varchar) as merchant_id,
        'bank' as source_system
    from {{ source('bronze', 'bank_transactions') }}
),

cc as (
    select
        md5(concat_ws('|', owner, card_number, txn_date, amount, raw_description)) as transaction_id,
        owner,
        card_number as account_id,
        txn_date,
        amount,
        raw_description,
        cast(null as varchar) as merchant_id,
        'credit_card' as source_system
    from {{ source('bronze', 'cc_transactions') }}
)

select * from bank
union all select * from cc
