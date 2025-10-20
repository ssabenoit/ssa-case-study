{{ config(materialized='table') }}

-- models/intermediate/int__team_season_stats_playoff.sql
-- Aggregates per-game data to get playoff season stats for each team

with

team_game_stats as (
    select *
    from {{ ref("int__team_per_game_stats") }}
),

teams_info as (
    select *
    from {{ ref("int__teams_basic_info") }}
),

team_stats as (
    select
        season,
        team_abv,
        count(*) as games_played,
        sum(goals) as goals,
        round(avg(goals), 2) as goals_per_game,
        round(avg(goals_against), 2) as goals_against_average,
        sum(goals_against) as goals_against,
        sum(shots) as shots,
        round(avg(shots), 2) as shots_per_game,
        sum(pp_goals) as pp_goals,
        sum(pp_attempts) as pp_attempts,
        sum(pp_goals) / sum(pp_attempts) as pp_pct,
        sum(pim) as pim,
        sum(pk_attempts) as pk_attempts,
        (sum(pk_attempts) - sum(pk_goals_against)) / sum(pk_attempts) as pk_pct,
        round(avg(pim), 2) as pim_per_game,
        sum(hits) as hits,
        round(avg(hits), 2) as hits_per_game,
        sum(blocks) as blocks,
        round(avg(blocks), 2) as blocks_per_game,
        sum(giveaways) as giveaways,
        round(avg(giveaways), 2) as giveaways_per_game,
        sum(takeaways) as takeaways,
        round(avg(takeaways), 2) as takeaways_per_game
    from team_game_stats
    where game_type = 'playoff'
    group by 
        season, 
        team_abv
)

select 
    s.*, 
    i.logo_url
from team_stats s 
left join teams_info i
    on s.team_abv = i.team_abv