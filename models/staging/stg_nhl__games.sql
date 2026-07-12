-- models/staging/stg_nhl__games.sql
-- Standardizes game data from the NHL API
-- Grain: one row per game; the raw loader appends re-extractions, so we keep
-- only the most recently loaded row per game (post-game corrections win).

with

source as (
    select *
    from {{ source('nhl_staging_data', 'games') }}
)

select *
from source
qualify row_number() over (partition by ID order by _loaded_at desc) = 1
