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
        md5(concat_ws('|', statement_sha256, account_number, txn_date, amount)) as stable_id,
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
        md5(concat_ws('|', statement_sha256, card_number, txn_date, amount)) as stable_id,
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
),

-- Pre-compute clean_description so we can join dim_merchants and pattern-rules
-- on it without recomputing the regex.
unified_clean as (
    select
        u.*,
        lower(regexp_replace(u.raw_description, '[^a-zA-Z0-9 ]', ' ', 'g')) as clean_description
    from unified u
),

-- Transfer detection runs FIRST and skips later categorisation, because
-- naive substring rules misfire on "transfer" descriptions (e.g. the
-- 'nsf' rule matches "tra-NSF-er"). Detection sources, in order:
--   1. dim_transfer_rules patterns (curated outbound markers)
--   2. dim_category_rules where category='transfer'
--   3. broad "transfer" word-boundary catch-all
unified_with_transfer as (
    select
        u.*,
        (
            exists (
                select 1 from {{ ref('dim_transfer_rules') }} t
                where contains(u.clean_description, t.pattern)
            )
            or exists (
                select 1 from {{ ref('dim_category_rules') }} r
                where r.category_id = 'transfer'
                  and contains(u.clean_description, r.pattern)
            )
            or regexp_matches(u.clean_description, '(^|\s)transfer(\s|$|sent|received)')
            or regexp_matches(u.clean_description, '(^|\s)tf\d')
        ) as detected_transfer
    from unified_clean u
),

-- Substring fallback: for each non-transfer transaction, the
-- dim_category_rule with the lowest priority value (highest precedence)
-- whose pattern is contained in clean_description wins. Patterns that are
-- short enough to false-match inside other words (e.g. 'nsf') are excluded
-- here when their category is 'transfer' — those are handled above.
rule_matches as (
    select
        u.transaction_id,
        arg_min(r.category_id, r.priority) as rule_category
    from unified_with_transfer u
    inner join {{ ref('dim_category_rules') }} r
      on contains(u.clean_description, r.pattern)
    where not u.detected_transfer
      and r.category_id != 'transfer'
    group by u.transaction_id
)

select
    u.transaction_id,
    u.stable_id,
    u.holder,
    u.account_id,
    a.friendly_name as account_name,
    a.account_kind,
    u.txn_date,
    u.amount * a.inversion_factor as amount,
    u.raw_description,
    u.clean_description,
    m.merchant_id,
    u.source_system,
    dayname(u.txn_date) as day_of_week_name,
    case when dayofweek(u.txn_date) in (0, 6) then true else false end as is_weekend,
    -- Categorisation chain (override > transfer-detected > merchant > rule > default).
    -- Transfer detection comes second so it can preempt over-eager substring
    -- rules ('nsf' matching inside "transfer"). Manual override is still
    -- highest precedence — useful for one-off recategorisations.
    case
        when o.category_id is not null then o.category_id
        when u.detected_transfer       then 'transfer'
        when nullif(m.category_id, 'uncategorised') is not null then m.category_id
        when rm.rule_category is not null then rm.rule_category
        else 'uncategorised'
    end as category_id,
    case
        when o.category_id is not null then 'override'
        when u.detected_transfer       then 'transfer-detect'
        when nullif(m.category_id, 'uncategorised') is not null then 'merchant'
        when rm.rule_category is not null then 'rule'
        else 'default'
    end as category_source,
    coalesce(o.category_id = 'transfer', false) or coalesce(u.detected_transfer, false) as is_transfer,
    u.statement_sha256
from unified_with_transfer u
left join {{ ref('dim_accounts') }} a on u.account_id = a.account_id
left join {{ ref('dim_merchants') }} m on m.canonical_name = u.clean_description
left join {{ ref('dim_category_overrides') }} o on o.stable_id = u.stable_id
left join rule_matches rm on rm.transaction_id = u.transaction_id
