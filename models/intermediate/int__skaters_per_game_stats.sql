-- models/intermediate/int__skaters_per_game_stats.sql
-- extracts individual player stats per game from each game boxscore
-- each entry has the season, game-id, player_id and the associated stats
{{ config(materialized='table') }}

with games as (
    select *
    from {{ ref("stg_nhl__game_boxscore") }}
),

/*
skaters as (
    select *
    from {{ ref("int__all_skaters") }}
),
*/

away_team_forwards as (
    -- selects all the away team forwards statistics
    select 
        id::INT as game_id,
        season::INT as season,
        awayteam:abbrev::STRING as team_abv,
        'away' as type,
        player.value: playerId as player_id,
        player.value: name:default::STRING as name,
        -- player.value: position::STRING as position,
        player.value: goals::INT as goals,
        player.value: assists as assists,
        player.value: hits as hits,
        player.value: shots as shots,
        player.value: faceoffWinningPctg as faceoff_pct,
        player.value: pim as pim,
        player.value: plusMinus as plus_minus,
        player.value: points as points,
        player.value: powerPlayGoals as pp_goals,
        TO_TIME(player.value: toi::STRING, 'MI:SS') as toi,
    from
        games,
        LATERAL FLATTEN(input => games.playerbygamestats, path => 'awayTeam.forwards') player
),

away_team_defense as (
    -- all the away team defensemen statistics
    select 
        id::INT as game_id,
        season::INT as season,
        awayteam:abbrev::STRING as team_abv,
        'away' as type,
        player.value: playerId as player_id,
        player.value: name:default::STRING as name,
        -- player.value: position::STRING as position,
        player.value: goals::INT as goals,
        player.value: assists as assists,
        player.value: hits as hits,
        player.value: shots as shots,
        player.value: faceoffWinningPctg as faceoff_pct,
        player.value: pim as pim,
        player.value: plusMinus as plus_minus,
        player.value: points as points,
        player.value: powerPlayGoals as pp_goals,
        TO_TIME(player.value: toi::STRING, 'MI:SS') as toi,
    from
        games,
        LATERAL FLATTEN(input => games.playerbygamestats, path => 'awayTeam.defense') player
),

home_team_forwards as (
    -- selects all the home team forwards' statistics
    select 
        id::INT as game_id,
        season::INT as season,
        hometeam:abbrev::STRING as team_abv,
        'home' as type,
        player.value: playerId as player_id,
        player.value: name:default::STRING as name,
        -- player.value: position::STRING as position,
        player.value: goals::INT as goals,
        player.value: assists as assists,
        player.value: hits as hits,
        player.value: shots as shots,
        player.value: faceoffWinningPctg as faceoff_pct,
        player.value: pim as pim,
        player.value: plusMinus as plus_minus,
        player.value: points as points,
        player.value: powerPlayGoals as pp_goals,
        TO_TIME(player.value: toi::STRING, 'MI:SS') as toi,
    from
        games,
        LATERAL FLATTEN(input => games.playerbygamestats, path => 'homeTeam.forwards') player
),

home_team_defense as (
    -- selects all the home team defensemens' statistics
    select 
        id::INT as game_id,
        season::INT as season,
        hometeam:abbrev::STRING as team_abv,
        'home' as type,
        player.value: playerId as player_id,
        player.value: name:default::STRING as name,
        -- player.value: position::STRING as position,
        player.value: goals::INT as goals,
        player.value: assists as assists,
        player.value: hits as hits,
        player.value: shots as shots,
        player.value: faceoffWinningPctg as faceoff_pct,
        player.value: pim as pim,
        player.value: plusMinus as plus_minus,
        player.value: points as points,
        player.value: powerPlayGoals as pp_goals,
        TO_TIME(player.value: toi::STRING, 'MI:SS') as toi,
    from
        games,
        LATERAL FLATTEN(input => games.playerbygamestats, path => 'homeTeam.defense') player
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

select * from all_skaters_per_game_stats
