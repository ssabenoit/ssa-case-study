-- models/marts/mart_head_to_head.sql
-- Season series records between every pair of teams that met.
-- Grain: one row per (season, game_type, team, opponent).

with

team_games as (
    select
        tg.season_key as season,
        tg.game_type,
        tg.team_abv,
        opp.team_abv as opponent_abv,
        tg.game_result,
        tg.points_earned,
        tg.goals_for,
        tg.goals_against
    from {{ ref('fct_team_game_stats') }} tg
    left join {{ ref('dim_teams') }} opp
        on opp.team_key = tg.opponent_team_key
)

select
    season,
    {{ season_display('season') }} as season_display,
    game_type,
    team_abv,
    opponent_abv,
    count(*) as games_played,
    count_if(game_result = 'W') as wins,
    count_if(game_result = 'L') as losses,
    count_if(game_result = 'OTL') as ot_losses,
    concat(
        count_if(game_result = 'W'), '-',
        count_if(game_result = 'L'), '-',
        count_if(game_result = 'OTL')
    ) as record,
    sum(points_earned) as points_earned,
    sum(goals_for) as goals_for,
    sum(goals_against) as goals_against,
    sum(goals_for) - sum(goals_against) as goal_differential
from team_games
group by
    season,
    game_type,
    team_abv,
    opponent_abv
