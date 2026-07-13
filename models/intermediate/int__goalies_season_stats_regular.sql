{{ config(materialized='table') }}

-- models/intermediate/int__goalies_season_stats_regular.sql
-- Compiles stats for each goalie for each regular season.
-- GAA is goals against per 60 minutes of ice time (not an average of
-- per-game GA), and save percentages use float division with honest nulls.

with

goalies_stats as (
    select
        *,
        extract(hour from toi) * 3600
            + extract(minute from toi) * 60
            + extract(second from toi) as toi_seconds
    from {{ ref("int__goalies_per_game_stats") }}
),

goalies_season_stats as (
    select
        season,
        player_id,
        name,
        team_abv,
        count(*) as gp,
        count(case when starter then 1 end) as starts,
        sum(goals_against) as goals_against,
        sum(toi_seconds) as toi_seconds,
        sum(shots_saved) as saves,
        sum(shots_against) as shots_faced,
        sum(even_goals_against) as even_goals_against,
        sum(even_shots_saved) as even_shots_saved,
        sum(even_shots_against) as even_shots_against,
        sum(pp_goals_against) as pp_ga,
        sum(pp_shots_saved) as pp_saves,
        sum(pp_shots_against) as pp_shots_against,
        sum(sh_goals_against) as sh_goals_against,
        sum(sh_shots_saved) as sh_saves,
        sum(sh_shots_against) as sh_shots_against,
        sum(pim) as pim
    from goalies_stats
    where
        game_type = 'regular'
        and (starter = true or result is not null)
    group by
        season,
        player_id,
        name,
        team_abv
)

select
    *,
    round(goals_against * 3600.0 / nullif(toi_seconds, 0), 2) as gaa,
    saves::float / nullif(shots_faced, 0) as save_pct,
    even_shots_saved::float / nullif(even_shots_against, 0) as even_save_pct,
    pp_saves::float / nullif(pp_shots_against, 0) as pp_save_pct,
    sh_saves::float / nullif(sh_shots_against, 0) as sh_save_pct
from goalies_season_stats
