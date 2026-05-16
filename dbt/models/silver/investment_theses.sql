{{ config(materialized='table') }}

-- Investment theses parsed from Obsidian vault frontmatter.
-- One row per thesis (not per ticker — a ticker can have multiple theses).
-- Joined downstream in gold.thesis_performance against broker trade data.

select
    thesis_id,
    ticker,
    status,
    opened,
    closed,
    price_at_open,
    buy_threshold,
    sell_threshold,
    stop_loss,
    confidence,
    horizon_months,
    expected_return_pct,

    -- Derived: days into thesis and whether horizon has passed
    current_date - opened                                   as days_open,
    (horizon_months * 30)                                   as horizon_days,
    current_date >= opened + interval (horizon_months * 30 || ' days')
                                                            as horizon_reached,

    -- Alert flags checked by the price-alert job
    status not in ('exited', 'invalidated')                 as is_active,

    dependencies,
    invalidation_conditions,
    decisions,
    outcome,
    source_file,
    ingested_at

from {{ source('bronze', 'investment_theses') }}
