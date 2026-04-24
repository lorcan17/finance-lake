{{ config(
    materialized='incremental',
    unique_key='merchant_id',
    on_schema_change='append_new_columns'
) }}

-- Canonical merchants with embeddings. Populated by embed_enrich service,
-- which writes directly to this table's underlying storage. dbt materializes
-- the table structure and serves it downstream; embed_enrich is the source of
-- truth for rows.

select
    cast(null as varchar) as merchant_id,
    cast(null as varchar) as canonical_name,
    cast(null as varchar) as category_id,
    cast(null as float[1536]) as embedding,
    cast(null as timestamp) as created_at,
    cast(null as timestamp) as updated_at
where false
