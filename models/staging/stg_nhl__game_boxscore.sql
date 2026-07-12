-- models/staging/stg_nhl__game_boxscore.sql
-- Standardizes game box score data from the NHL API
-- Grain: one row per game; the raw loader appends re-extractions, so we keep
-- only the most recently loaded row per game (final boxscores beat in-progress
-- snapshots captured mid-game).

with

source as (
    select *
    from {{ source('nhl_staging_data', 'game_boxscore') }}
)

select *
from source
qualify row_number() over (partition by ID order by _loaded_at desc) = 1
