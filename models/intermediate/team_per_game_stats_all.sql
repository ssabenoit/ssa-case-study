-- models/intermediate/team_per_game_stats_all.sql
-- compile per game stats for each team and each game they played in the data
{{ config(materialized='table') }}

with games as (
    select *
    from {{ ref("stg_nhl__game_boxscore") }}
),

skaters as (
    select *
    from {{ ref('skaters_per_game_stats_all') }}
),

goalies as (
    select *
    from {{ ref('goalies_per_game_stats_all') }}
),

offense_per_game as (
    select
        game_id,
        team_abv, 
        season,
        game_type,
        -- count(*) as num_players,
        sum(goals) as goals,
        sum(assists) as assists,
        sum(hits) as hits,
        sum(shots) as shots,
        sum(pim) as pim,
        sum(points) as points,
        sum(pp_goals) as pp_goals,
        sum(blocks) as blocks,
        sum(giveaways) as giveaways,
        sum(takeaways) as takeaways

    from skaters
    group by game_id, team_abv, season, game_type
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
        cast(sum(shots_saved) as int)/cast(sum(shots_against) as int) as save_pct,
        sum(pim) as pim,
        cast(sum(pp_shots_against) as int) as pp_shots_against,
        cast(sum(pp_shots_saved) as int) as pp_saves,
        -- cast(sum(pp_shots_against) as int)/cast(sum(pp_shots_saved) as int) as pp_save_pct,
        
    from goalies
    group by game_id, team_abv, season, game_type
)

select
    o.*,
    g.goals_against,
    g.saves,
    g.shots_against,
    g.save_pct,
    o.pim + g.pim as total_pim,
    g.pp_shots_against,
    g.pp_saves
from offense_per_game o
left join goalies_per_game g
on o.game_id = g.game_id
and o.season = g.season
and o.game_type = g.game_type
and o.team_abv = g.team_abv

/*
no longer works due to summary

away_team_stats as (
    select
        season::STRING as season,
        id::INT as game_id,
        awayteam:abbrev::STRING as team_abv,
        awayteam:id::INT as team_id,
        'away' as type,
        case 
            when gametype = 2 then 'regular'
            when gametype = 3 then 'playoff'
            else 'other'
        end as game_type,
        awayteam:sog::INT as shots,
        awayteam:score::INT as goals,
        hometeam:score::INT as goals_against,
        summary:linescore:byPeriod[0]:away::INT as p1_goals,
        summary:linescore:byPeriod[1]:away::INT as p2_goals,
        summary:linescore:byPeriod[2]:away::INT as p3_goals,
        goals - p1_goals - p2_goals - p3_goals as ot_goals,
        summary:shotsByPeriod[0]:away::INT as p1_shots,
        summary:shotsByPeriod[1]:away::INT as p2_shots,
        summary:shotsByPeriod[2]:away::INT as p3_shots,
        shots - p1_shots - p2_shots - p3_shots as ot_shots,
        summary:teamGameStats[0]:awayValue::INT as sog,
        summary:teamGameStats[1]:awayValue::FLOAT as faceoff_pct,
        summary:teamGameStats[2]:awayValue::STRING as power_play,
        CAST(split_part(power_play, '/', 0) as INT) as pp_goals,
        CAST(split_part(power_play, '/', -1) as INT) as pp_attempts,
        summary:teamGameStats[3]:awayValue::FLOAT as pp_pct,
        summary:teamGameStats[4]:awayValue::INT as pim,
        summary:teamGameStats[5]:awayValue::INT as hits,
        summary:teamGameStats[6]:awayValue::INT as blocked_shots,
        summary:teamGameStats[7]:awayValue::INT as giveaways,
        summary:teamGameStats[8]:awayValue::INT as takeaways,
    from games
),

home_team_stats as (
    select
        season::STRING as season,
        id::INT as game_id,
        hometeam:abbrev::STRING as team_abv,
        hometeam:id::INT as team_id,
        'home' as type,
        case 
            when gametype = 2 then 'regular'
            when gametype = 3 then 'playoff'
            else 'other'
        end as game_type,
        hometeam:sog::INT as shots,
        hometeam:score::INT as goals,
        awayteam:score::INT as goals_against,
        summary:linescore:byPeriod[0]:home::INT as p1_goals,
        summary:linescore:byPeriod[1]:home::INT as p2_goals,
        summary:linescore:byPeriod[2]:home::INT as p3_goals,
        goals - p1_goals - p2_goals - p3_goals as ot_goals,
        summary:shotsByPeriod[0]:home::INT as p1_shots,
        summary:shotsByPeriod[1]:home::INT as p2_shots,
        summary:shotsByPeriod[2]:home::INT as p3_shots,
        shots - p1_shots - p2_shots - p3_shots as ot_shots,
        summary:teamGameStats[0]:homeValue::INT as sog,
        summary:teamGameStats[1]:homeValue::FLOAT as faceoff_pct,
        summary:teamGameStats[2]:homeValue::STRING as power_play,
        CAST(split_part(power_play, '/', 0) as INT) as pp_goals,
        CAST(split_part(power_play, '/', -1) as INT) as pp_attempts,
        summary:teamGameStats[3]:homeValue::FLOAT as pp_pct,
        summary:teamGameStats[4]:homeValue::INT as pim,
        summary:teamGameStats[5]:homeValue::INT as hits,
        summary:teamGameStats[6]:homeValue::INT as blocked_shots,
        summary:teamGameStats[7]:homeValue::INT as giveaways,
        summary:teamGameStats[8]:homeValue::INT as takeaways,
    from games
)

select * 
from home_team_stats
union all
select * 
from away_team_stats

*/