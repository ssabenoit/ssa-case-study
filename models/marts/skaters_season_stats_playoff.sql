-- models/marts/skaters_season_stats_playoff.sql
-- Playoff stats for each skater (one row per season/player/team stint).
-- Same construction as skaters_season_stats_regular, playoff games only.

with

player_games as (
    select *
    from {{ ref('fct_player_game_stats') }}
    where game_type = 'playoff'
),

skater_season_stats as (
    select
        season_key as season,
        player_id,
        team_abv,
        player_name as name,
        count(*) as games_played,
        sum(goals) as goals,
        sum(assists) as assists,
        sum(hits) as hits,
        sum(shots) as shots,
        sum(penalty_minutes) as pim,
        sum(plus_minus) as plus_minus,
        sum(points) as points,
        sum(pp_goals) as pp_goals,
        sum(giveaways) as giveaways,
        sum(takeaways) as takeaways,
        sum(shifts) as shifts,
        sum(time_on_ice_seconds) as total_toi,
        avg(time_on_ice_seconds) as avg_toi,
        sum(primary_assists) as primary_assists,
        sum(secondary_assists) as secondary_assists,
        sum(pp_assists) as pp_assists,
        sum(sh_goals) as sh_goals,
        sum(sh_assists) as sh_assists,
        sum(empty_net_goals) as empty_net_goals,
        sum(game_winning_goals) as game_winning_goals,
        sum(overtime_goals) as overtime_goals,
        sum(faceoffs_won) as faceoffs_won,
        sum(faceoffs_lost) as faceoffs_lost
    from player_games
    group by
        season,
        player_id,
        team_abv,
        name
)

select
    ss.*,
    {{ safe_divide('ss.faceoffs_won::float', 'ss.faceoffs_won + ss.faceoffs_lost') }} as faceoff_pct,
    round({{ safe_divide('ss.points::float', 'ss.games_played') }}, 2) as points_per_game,
    round({{ safe_divide('ss.goals::float', 'ss.games_played') }}, 2) as goals_per_game,
    round({{ safe_divide('ss.assists::float', 'ss.games_played') }}, 2) as assists_per_game,
    round({{ safe_divide('ss.points * 3600.0', 'ss.total_toi') }}, 2) as points_per_60,
    round({{ safe_divide('ss.goals * 3600.0', 'ss.total_toi') }}, 2) as goals_per_60,
    {{ season_display('ss.season') }} as season_display,
    dp.first_name,
    dp.last_name,
    dp.primary_position_code as position,
    coalesce(
        dp.headshot_url,
        'https://assets.nhle.com/mugs/nhl/' || ss.season || '/' || ss.team_abv || '/' || ss.player_id || '.png'
    ) as headshot_url
from skater_season_stats ss
left join {{ ref('dim_players') }} dp
    on ss.player_id = dp.player_id
