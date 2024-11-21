-- models/marts/upcoming_games.sql
-- filtering every game of the season to only display upcoming games

with games as (
    select *
    from {{ ref('stg_nhl__season_schedules') }}
)

select *
from games
where game_state = 'FUT' and game_type = 2