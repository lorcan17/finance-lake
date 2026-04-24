{{ config(materialized='table') }}

-- Unified account list: Questrade (TFSA/RRSP/cash), bank, credit card.
-- First pass: derive from distinct account identifiers in each bronze source.

with questrade as (
    select distinct
        account_number as account_id,
        'questrade' as source_system,
        account_type as account_subtype,
        'investment' as account_kind
    from {{ source('bronze', 'questrade_snapshots') }}
),

bank as (
    select distinct
        account_number as account_id,
        'bank' as source_system,
        cast(null as varchar) as account_subtype,
        'bank' as account_kind
    from {{ source('bronze', 'bank_transactions') }}
),

cc as (
    select distinct
        card_number as account_id,
        'credit_card' as source_system,
        cast(null as varchar) as account_subtype,
        'credit_card' as account_kind
    from {{ source('bronze', 'cc_transactions') }}
)

select * from questrade
union all select * from bank
union all select * from cc
