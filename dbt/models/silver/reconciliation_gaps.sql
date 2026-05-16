{{ config(materialized='table') }}

-- Months present in CSV exports but absent from PDF-parsed transactions.
-- Gaps indicate a missing statement PDF or a parse failure in statement-extract.
-- Resolve by locating the PDF and re-ingesting; do not promote CSV rows to
-- fact_transactions — PDFs remain the source of truth.

with csv_months as (
    select
        account_hint,
        date_trunc('month', row_date) as month,
        count(*)                       as csv_row_count
    from {{ source('bronze', 'banking_csv_raw') }}
    group by 1, 2
),

pdf_months as (
    select
        account_id,
        date_trunc('month', txn_date) as month
    from {{ ref('fact_transactions') }}
    where source_system in ('bank', 'credit_card')
    group by 1, 2
)

select
    c.account_hint,
    c.month,
    c.csv_row_count,
    'missing_pdf' as gap_reason
from csv_months c
left join pdf_months p
    on  c.account_hint = p.account_id
    and c.month        = p.month
where p.month is null
order by c.account_hint, c.month
