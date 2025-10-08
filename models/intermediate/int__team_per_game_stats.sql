{{ config(materialized='table') }}

-- models/intermediate/int__team_per_game_stats.sql
-- Compiles per-game statistics for each team and each game they played

with

games as (
    select *
    from {{ ref("stg_nhl__game_boxscore") }}
),

summaries as (
    select *
    from {{ ref('stg_nhl__game_summaries') }}
),

skaters as (
    select *
    from {{ ref('int__skaters_per_game_stats') }}
),

goalies as (
    select *
    from {{ ref('int__goalies_per_game_stats') }}
),

goalies_per_game as (
    select
        game_id,
        team_abv, 
        season,
        game_type,
        sum(goals_against) as goals_against,
        sum(shots_saved) as saves,
        sum(shots_against) as shots_against,
        cast(sum(shots_saved) as int) / cast(sum(shots_against) as int) as save_pct,
        sum(pim) as pim,
        cast(sum(pp_shots_against) as int) as pp_shots_against,
        cast(sum(pp_shots_saved) as int) as pp_saves
    from goalies
    group by 
        game_id, 
        team_abv, 
        season, 
        game_type
)

select
    s.*,
    g.goals_against as goals_against_g,
    g.saves,
    g.shots_against,
    g.save_pct,
    g.pp_shots_against,
    g.pp_saves
from summaries s
left join goalies_per_game g
    on s.game_id = g.game_id
    and s.season = g.season
    and s.game_type = g.game_type
    and s.team_abv = g.team_abv
