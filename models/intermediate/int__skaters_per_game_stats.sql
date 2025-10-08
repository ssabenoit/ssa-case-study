{{ config(materialized='table') }}

-- models/intermediate/int__skaters_per_game_stats.sql
-- Extracts individual skater stats per game from each game boxscore

with

games as (
    select *
    from {{ ref("stg_nhl__game_boxscore") }}
),

away_team_forwards as (
    select 
        id::int as game_id,
        season::int as season,
        awayteam:abbrev::string as team_abv,
        'away' as type,
        case 
            when gametype = 2 then 'regular'
            when gametype = 3 then 'playoff'
            else 'other'
        end as game_type,
        player.value:playerId::int as player_id,
        player.value:name:default::string as name,
        player.value:goals::int as goals,
        player.value:assists::int as assists,
        player.value:hits::int as hits,
        player.value:sog::int as shots,
        player.value:faceoffWinningPctg::float as faceoff_pct,
        player.value:pim::int as pim,
        player.value:plusMinus::int as plus_minus,
        player.value:points::int as points,
        player.value:powerPlayGoals::int as pp_goals,
        player.value:giveaways::int as giveaways,
        player.value:takeaways::int as takeaways,
        player.value:blockedShots::int as blocks,
        player.value:shifts::int as shifts,
        to_time(player.value:toi::string, 'MI:SS') as toi
    from
        games,
        lateral flatten(input => games.playerbygamestats, path => 'awayTeam.forwards') player
),

away_team_defense as (
    select 
        id::int as game_id,
        season::int as season,
        awayteam:abbrev::string as team_abv,
        'away' as type,
        case 
            when gametype = 2 then 'regular'
            when gametype = 3 then 'playoff'
            else 'other'
        end as game_type,
        player.value:playerId::int as player_id,
        player.value:name:default::string as name,
        player.value:goals::int as goals,
        player.value:assists::int as assists,
        player.value:hits::int as hits,
        player.value:sog::int as shots,
        player.value:faceoffWinningPctg::float as faceoff_pct,
        player.value:pim::int as pim,
        player.value:plusMinus::int as plus_minus,
        player.value:points::int as points,
        player.value:powerPlayGoals::int as pp_goals,
        player.value:giveaways::int as giveaways,
        player.value:takeaways::int as takeaways,
        player.value:blockedShots::int as blocks,
        player.value:shifts::int as shifts,
        to_time(player.value:toi::string, 'MI:SS') as toi
    from
        games,
        lateral flatten(input => games.playerbygamestats, path => 'awayTeam.defense') player
),

home_team_forwards as (
    select 
        id::int as game_id,
        season::int as season,
        hometeam:abbrev::string as team_abv,
        'home' as type,
        case 
            when gametype = 2 then 'regular'
            when gametype = 3 then 'playoff'
            else 'other'
        end as game_type,
        player.value:playerId::int as player_id,
        player.value:name:default::string as name,
        player.value:goals::int as goals,
        player.value:assists::int as assists,
        player.value:hits::int as hits,
        player.value:sog::int as shots,
        player.value:faceoffWinningPctg::float as faceoff_pct,
        player.value:pim::int as pim,
        player.value:plusMinus::int as plus_minus,
        player.value:points::int as points,
        player.value:powerPlayGoals::int as pp_goals,
        player.value:giveaways::int as giveaways,
        player.value:takeaways::int as takeaways,
        player.value:blockedShots::int as blocks,
        player.value:shifts::int as shifts,
        to_time(player.value:toi::string, 'MI:SS') as toi
    from
        games,
        lateral flatten(input => games.playerbygamestats, path => 'homeTeam.forwards') player
),

home_team_defense as (
    select 
        id::int as game_id,
        season::int as season,
        hometeam:abbrev::string as team_abv,
        'home' as type,
        case 
            when gametype = 2 then 'regular'
            when gametype = 3 then 'playoff'
            else 'other'
        end as game_type,
        player.value:playerId::int as player_id,
        player.value:name:default::string as name,
        player.value:goals::int as goals,
        player.value:assists::int as assists,
        player.value:hits::int as hits,
        player.value:sog::int as shots,
        player.value:faceoffWinningPctg::float as faceoff_pct,
        player.value:pim::int as pim,
        player.value:plusMinus::int as plus_minus,
        player.value:points::int as points,
        player.value:powerPlayGoals::int as pp_goals,
        player.value:giveaways::int as giveaways,
        player.value:takeaways::int as takeaways,
        player.value:blockedShots::int as blocks,
        player.value:shifts::int as shifts,
        to_time(player.value:toi::string, 'MI:SS') as toi
    from
        games,
        lateral flatten(input => games.playerbygamestats, path => 'homeTeam.defense') player
),

full_away_teams_stats as (
    select * 
    from away_team_defense
    union all
    select *
    from away_team_forwards
),

full_home_teams_stats as (
    select * 
    from home_team_defense
    union all
    select *
    from home_team_forwards
),

all_skaters_per_game_stats as (
    select *
    from full_away_teams_stats
    union all
    select *
    from full_home_teams_stats
)

select * 
from all_skaters_per_game_stats
