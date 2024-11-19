with all_plays as (
    select *
    from {{ source('nhl_staging_data', 'play_by_play') }}
)

select 
    id, 
    season, 
    gamedate as date,
    plays
from all_plays
