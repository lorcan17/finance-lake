{{ config(materialized='view') }}

-- Flattened "Semantic" view for AI/Natural Language queries.
-- Purpose: Provide a single wide table with human-readable labels and temporal context.

with transactions as (
    select * from {{ ref('fact_transactions') }}
),

merchants as (
    select * from {{ ref('dim_merchants') }}
),

categories as (
    select * from {{ ref('dim_categories') }}
),

transfers as (
    select * from {{ ref('fact_transfers') }}
),

accounts as (
    select * from {{ ref('dim_accounts') }}
)

select
    t.transaction_id,
    t.txn_date as transaction_date,
    t.day_of_week_name,
    t.is_weekend,
    t.amount,
    t.raw_description,
    t.clean_description,
    coalesce(m.canonical_name, t.clean_description) as merchant_name,
    coalesce(c.category_name, 'Uncategorised') as category_name,
    coalesce(pc.category_name, 'Uncategorised') as parent_category_name,
    t.account_name,
    t.account_kind,
    case when f.transaction_id is not null then true else false end as is_transfer,
    t.source_system
from transactions t
left join merchants m on t.clean_description = m.canonical_name
left join categories c on m.category_id = c.category_id
left join categories pc on c.parent_category_id = pc.category_id
left join transfers f on t.transaction_id = f.transaction_id
