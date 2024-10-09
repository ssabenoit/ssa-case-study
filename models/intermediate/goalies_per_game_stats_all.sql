-- models/intermediate/goalies_per_game_stats_all.sql
-- compile per game stats for nhl goalies from individual box scores
{{ config(materialized='table') }}

with games as (
    select *
    from {{ ref("stg_nhl__game_boxscore") }}
),

away_team_goalies as (
    -- selects all the away team goalie statistics
    select 
        id::INT as game_id,
        season::INT as season,
        case 
            when gametype = 2 then 'regular'
            when gametype = 3 then 'playoff'
            else 'other'
        end as game_type,
        awayteam:abbrev::STRING as team_abv,
        'away' as type,
        player.value: playerId::INT as player_id,
        player.value: name:default::STRING as name,
        player.value: position::STRING as position,
        player.value: starter::BOOLEAN as starter,
        player.value: decision::STRING as result,
        player.value: goalsAgainst::INT as goals_against,
        player.value: savePctg::FLOAT as save_pct,
        player.value: saveShotsAgainst::STRING as shots_faced,
        CAST(split_part(player.value: saveShotsAgainst::STRING, '/', 1) as INT) as shots_saved,
        CAST(split_part(player.value: saveShotsAgainst::STRING, '/', -1) as INT) as shots_against,
        player.value: evenStrengthGoalsAgainst::INT as even_goals_against,
        player.value: evenStrengthShotsAgainst::STRING as even_shots_faced,
        CAST(split_part(player.value: evenStrengthShotsAgainst::STRING, '/', 1) as INT) as even_shots_saved,
        CAST(split_part(player.value: evenStrengthShotsAgainst::STRING, '/', -1) as INT) as even_shots_against,
        player.value: powerPlayGoalsAgainst::INT as pp_goals_against,
        player.value: powerPlayShotsAgainst::STRING as pp_shots_faced,
        CAST(split_part(player.value: powerPlayShotsAgainst::STRING, '/', 1) as INT) as pp_shots_saved,
        CAST(split_part(player.value: powerPlayShotsAgainst::STRING, '/', -1) as INT) as pp_shots_against,
        player.value: shorthandedGoalsAgainst::INT as sh_goals_against,
        player.value: shorthandedShotsAgainst::STRING as sh_shots_faced,
        CAST(split_part(player.value: shorthandedShotsAgainst::STRING, '/', 1) as INT) as sh_shots_saved,
        CAST(split_part(player.value: shorthandedShotsAgainst::STRING, '/', -1) as INT) as sh_shots_against,
        player.value: pim::INT as pim,
        TO_TIME(CAST(
            CAST(split_part(player.value: toi::STRING, ':', 0) as INT) * 60 + CAST(split_part(player.value: toi::STRING, ':', -1) as INT)
            as STRING)
        )as toi
        -- TO_TIME(player.value: toi::STRING, 'MI:SS') as toi
    from
        games,
        LATERAL FLATTEN(input => games.playerbygamestats, path => 'awayTeam.goalies') player
),

home_team_goalies as (
    -- selects all the home team goalie statistics
    select 
        id::INT as game_id,
        season::INT as season,
        case 
            when gametype = 2 then 'regular'
            when gametype = 3 then 'playoff'
            else 'other'
        end as game_type,
        hometeam:abbrev::STRING as team_abv,
        'home' as type,
        player.value: playerId::INT as player_id,
        player.value: name:default::STRING as name,
        player.value: position::STRING as position,
        player.value: starter::BOOLEAN as starter,
        player.value: decision::STRING as result,
        player.value: goalsAgainst::INT as goals_against,
        player.value: savePctg::FLOAT as save_pct,
        player.value: saveShotsAgainst::STRING as shots_faced,
        CAST(split_part(player.value: saveShotsAgainst::STRING, '/', 1) as INT) as shots_saved,
        CAST(split_part(player.value: saveShotsAgainst::STRING, '/', -1) as INT) as shots_against,
        player.value: evenStrengthGoalsAgainst::INT as even_goals_against,
        player.value: evenStrengthShotsAgainst::STRING as even_shots_faced,
        CAST(split_part(player.value: evenStrengthShotsAgainst::STRING, '/', 1) as INT) as even_shots_saved,
        CAST(split_part(player.value: evenStrengthShotsAgainst::STRING, '/', -1) as INT) as even_shots_against,
        player.value: powerPlayGoalsAgainst::INT as pp_goals_against,
        player.value: powerPlayShotsAgainst::STRING as pp_shots_faced,
        CAST(split_part(player.value: powerPlayShotsAgainst::STRING, '/', 1) as INT) as pp_shots_saved,
        CAST(split_part(player.value: powerPlayShotsAgainst::STRING, '/', -1) as INT) as pp_shots_against,
        player.value: shorthandedGoalsAgainst::INT as sh_goals_against,
        player.value: shorthandedShotsAgainst::STRING as sh_shots_faced,
        CAST(split_part(player.value: shorthandedShotsAgainst::STRING, '/', 1) as INT) as sh_shots_saved,
        CAST(split_part(player.value: shorthandedShotsAgainst::STRING, '/', -1) as INT) as sh_shots_against,
        player.value: pim::INT as pim,
        TO_TIME(CAST(
            CAST(split_part(player.value: toi::STRING, ':', 0) as INT) * 60 + CAST(split_part(player.value: toi::STRING, ':', -1) as INT)
            as STRING)
        )as toi
        --TO_TIME(player.value: toi::STRING, 'MI:SS') as toi,
    from
        games,
        LATERAL FLATTEN(input => games.playerbygamestats, path => 'homeTeam.goalies') player
),

all_goalies_per_game_stats as (
    select * 
    from home_team_goalies
    union all
    select *
    from away_team_goalies
)

select *
from all_goalies_per_game_stats
