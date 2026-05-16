{{ config(materialized='view') }}

-- Passthrough view over silver.dim_merchants, which is written directly by
-- the embed_enrich service (rule-pass + LLM-embedding categorisation).
-- A view (not a table) so embed_enrich's writes are immediately visible
-- to downstream models without needing dbt to re-run.

select
    merchant_id,
    canonical_name,
    category_id,
    embedding,
    created_at,
    updated_at
from {{ source('embed_enrich', 'dim_merchants') }}
