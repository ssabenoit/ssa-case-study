-- models/analytics/mart_team_elo.sql
-- Elo rating history per team per game, presentation-ready.
-- Grain: one row per (game, team).

with elo as (
    select * from {{ source('nhl_analytics_engine', 'team_elo_daily') }}
)

select game_date, season, {{ season_display('season') }} as season_display,
       game_id, home_team as team_abv, home_elo_post as elo, true as was_home,
       p_home_win as p_win_pregame, home_won as won
from elo
union all
select game_date, season, {{ season_display('season') }},
       game_id, away_team, away_elo_post, false,
       1 - p_home_win, not home_won
from elo
