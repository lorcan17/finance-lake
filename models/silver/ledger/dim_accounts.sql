{{ config(materialized='table') }}

-- Unified account list: Questrade (TFSA/RRSP/cash), bank, credit card.
-- First pass: derive from distinct account identifiers in each bronze source.

with questrade as (
    select distinct
        account_number as account_id,
        cast(null as varchar) as holder,
        'questrade' as source_system,
        cast(null as varchar) as account_subtype,
        'investment' as account_kind
    from {{ source('bronze', 'questrade_snapshots') }}
),

bank as (
    select distinct
        account_number as account_id,
        holder,
        'bank' as source_system,
        cast(null as varchar) as account_subtype,
        'bank' as account_kind
    from {{ source('bronze', 'bank_transactions') }}
),

cc as (
    select distinct
        card_number as account_id,
        holder,
        'credit_card' as source_system,
        cast(null as varchar) as account_subtype,
        'credit_card' as account_kind
    from {{ source('bronze', 'cc_transactions') }}
),

unified as (
    select * from questrade
    union all select * from bank
    union all select * from cc
),

deduped as (
    -- Joint cards yield one bronze row per supplementary holder name. Collapse
    -- to one row per (source_system, account_id) so the fact_transactions
    -- left-join doesn't fan out.
    select
        account_id,
        any_value(holder) as holder,
        source_system,
        any_value(account_subtype) as account_subtype,
        any_value(account_kind) as account_kind
    from unified
    group by account_id, source_system
)

select
    u.account_id,
    u.holder,
    u.source_system,
    u.account_subtype,
    u.account_kind,
    coalesce(n.friendly_name, u.account_id) as friendly_name,
    coalesce(n.inversion_factor, 1) as inversion_factor,
    coalesce(n.is_active, true) as is_active,
    coalesce(n.expected_frequency, 'monthly') as expected_frequency,
    n.start_date
from deduped u
left join {{ ref('dim_account_normalization') }} n on u.account_id = n.account_id
