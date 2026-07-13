-- models/marts/goalies_season_stats_playoff.sql
-- Playoff stats for each goalie (one row per season/goalie/team stint).
-- Same construction as goalies_season_stats_regular, playoff games only.

with

goalie_games as (
    select *
    from {{ ref('fct_goalie_game_stats') }}
    where game_type = 'playoff'
),

goalie_season_stats as (
    select
        season_key as season,
        player_id,
        min(player_name) as name,
        team_abv,
        count(*) as gp,
        count_if(is_starting_goalie) as starts,
        count_if(decision = 'W') as wins,
        count_if(decision = 'L') as losses,
        count_if(shutout_flag) as shutouts,
        count_if(quality_start) as quality_starts,
        sum(goals_against) as goals_against,
        sum(time_on_ice_seconds) as toi_seconds,
        sum(saves) as saves,
        sum(shots_faced) as shots_faced,
        sum(even_strength_goals_against) as even_goals_against,
        sum(even_strength_saves) as even_shots_saved,
        sum(even_strength_shots) as even_shots_against,
        sum(powerplay_goals_against) as pp_ga,
        sum(powerplay_saves) as pp_saves,
        sum(powerplay_shots) as pp_shots_against,
        sum(shorthanded_goals_against) as sh_goals_against,
        sum(shorthanded_saves) as sh_saves,
        sum(shorthanded_shots) as sh_shots_against,
        sum(penalty_minutes) as pim
    from goalie_games
    group by
        season,
        player_id,
        team_abv
)

select
    gs.*,
    round(gs.goals_against * 3600.0 / nullif(gs.toi_seconds, 0), 2) as gaa,
    {{ safe_divide('gs.saves::float', 'gs.shots_faced') }} as save_pct,
    {{ safe_divide('gs.even_shots_saved::float', 'gs.even_shots_against') }} as even_save_pct,
    {{ safe_divide('gs.pp_saves::float', 'gs.pp_shots_against') }} as pp_save_pct,
    {{ safe_divide('gs.sh_saves::float', 'gs.sh_shots_against') }} as sh_save_pct,
    round(gs.saves - (gs.shots_faced * {{ var('league_avg_save_pct') }}), 1) as goals_saved_above_average,
    {{ season_display('gs.season') }} as season_display,
    dp.first_name,
    dp.last_name,
    coalesce(
        dp.headshot_url,
        'https://assets.nhle.com/mugs/nhl/' || gs.season || '/' || gs.team_abv || '/' || gs.player_id || '.png'
    ) as headshot_url
from goalie_season_stats gs
left join {{ ref('dim_players') }} dp
    on gs.player_id = dp.player_id
