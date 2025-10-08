-- models/marts/upcoming_games.sql
-- Filters every game of the season to only display upcoming games

with

games as (
    select *
    from {{ ref('stg_nhl__season_schedules') }}
    where 
        game_state = 'FUT' 
        and game_type = 2
),

teams as (
    select *
    from {{ ref('int__teams_basic_info') }}
)

select
    g.*,
    a.logo_url as away_logo,
    h.logo_url as home_logo
from games g
left join teams a
    on g.away_abv = a.team_abv
left join teams h
    on g.home_abv = h.team_abv
