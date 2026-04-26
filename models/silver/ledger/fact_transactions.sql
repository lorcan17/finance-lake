{{ config(materialized='table') }}

-- Unified transaction grain across bank + credit card. Investment activity
-- (Questrade) is not a transaction stream in v1 — positions only.
-- Holder is read from the statement header (joined via statement_sha256)
-- so detail rows stay slim.

with bank_with_holder as (
    select t.*, s.holder
    from {{ source('bronze', 'bank_transactions') }} t
    left join {{ source('bronze', 'bank_statements') }} s
      on t.statement_sha256 = s.sha256
     and t.account_number = s.account_number
),

bank_numbered as (
    select
        *,
        row_number() over (
            partition by holder, account_number, txn_date, amount, raw_description, running_balance
            order by statement_sha256
        ) as dup_seq
    from bank_with_holder
),

bank as (
    select
        md5(concat_ws('|', holder, account_number, txn_date, amount, raw_description, running_balance, dup_seq)) as transaction_id,
        holder,
        account_number as account_id,
        txn_date,
        amount,
        raw_description,
        cast(null as varchar) as merchant_id,
        'bank' as source_system,
        statement_sha256
    from bank_numbered
),

cc_with_holder as (
    select t.*, s.holder
    from {{ source('bronze', 'cc_transactions') }} t
    left join {{ source('bronze', 'cc_statements') }} s
      on t.statement_sha256 = s.sha256
),

cc_numbered as (
    select
        *,
        row_number() over (
            partition by holder, card_number, txn_date, amount, raw_description
            order by posting_date, statement_sha256
        ) as dup_seq
    from cc_with_holder
),

cc as (
    select
        md5(concat_ws('|', holder, card_number, txn_date, amount, raw_description, dup_seq)) as transaction_id,
        holder,
        card_number as account_id,
        txn_date,
        amount,
        raw_description,
        cast(null as varchar) as merchant_id,
        'credit_card' as source_system,
        statement_sha256
    from cc_numbered
),

unified as (
    select * from bank
    union all select * from cc
)

select
    u.transaction_id,
    u.holder,
    u.account_id,
    a.friendly_name as account_name,
    a.account_kind,
    u.txn_date,
    u.amount * a.inversion_factor as amount,
    u.raw_description,
    lower(regexp_replace(u.raw_description, '[^a-zA-Z0-9 ]', ' ', 'g')) as clean_description,
    u.merchant_id,
    u.source_system,
    dayname(u.txn_date) as day_of_week_name,
    case when dayofweek(u.txn_date) in (0, 6) then true else false end as is_weekend,
    false as is_transfer,
    u.statement_sha256
from unified u
left join {{ ref('dim_accounts') }} a on u.account_id = a.account_id
