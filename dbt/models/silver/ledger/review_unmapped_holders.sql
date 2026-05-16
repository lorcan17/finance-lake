{{ config(materialized='view') }}

-- Holder strings present in bronze that have no row in dim_holders seed.
-- Surfaces variants the Paperless hook will tag _unowned. Add a row to
-- ~/Documents/finance-lake-seeds/dim_holders.csv for each entry, then
-- `dbt seed` to make the mapping live.

with all_holders as (
    select distinct holder, 'bank' as source from {{ source('bronze', 'bank_statements') }}
    union
    select distinct holder, 'cc' as source from {{ source('bronze', 'cc_statements') }}
)

select
    a.holder as holder_raw,
    a.source,
    count(*) over (partition by a.holder) as variant_appearances
from all_holders a
left join {{ ref('dim_holders') }} d on a.holder = d.holder_raw
where d.holder_raw is null
order by a.holder
