{{ config(materialized='table') }}

-- models/intermediate/int__skaters_season_stats_regular.sql
-- Compiles stats for each skater for each regular season

with

skaters_info as (
    select *
    from {{ ref("int__all_skaters") }}
),

skater_stats as (
    select *
    from {{ ref("int__skaters_per_game_stats") }}
),

skater_season_stats as (
    select
        season,
        player_id,
        team_abv,
        name,
        count(*) as games_played,
        sum(goals) as goals,
        sum(assists) as assists,
        sum(hits) as hits,
        sum(shots) as shots,
        avg(faceoff_pct) as faceoff_pct,
        sum(pim) as pim,
        sum(plus_minus) as plus_minus,
        sum(points) as points,
        sum(pp_goals) as pp_goals,
        sum(giveaways) as giveaways,
        sum(takeaways) as takeaways,
        sum(shifts) as shifts,
        sum(toi) as total_toi,
        avg(toi) as avg_toi
    from skater_stats
    where game_type = 'regular'
    group by 
        season, 
        player_id, 
        name, 
        team_abv
)

select 
    ss.*,
    si.first_name,
    si.last_name,
    si.position
from skater_season_stats ss
left join skaters_info si 
    on ss.player_id = si.player_id