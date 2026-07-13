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
        ID::int as game_id,
        SEASON::int as season,
        AWAYTEAM_ABBREV::string as team_abv,
        'away' as type,
        case
            when GAMETYPE = 2 then 'regular'
            when GAMETYPE = 3 then 'playoff'
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
        cast(split_part(player.value:toi::string, ':', 1) as int) * 60 + cast(split_part(player.value:toi::string, ':', 2) as int) as toi
    from
        games,
        lateral flatten(input => parse_json(PLAYERBYGAMESTATS_AWAYTEAM_FORWARDS)) player
),

away_team_defense as (
    select
        ID::int as game_id,
        SEASON::int as season,
        AWAYTEAM_ABBREV::string as team_abv,
        'away' as type,
        case
            when GAMETYPE = 2 then 'regular'
            when GAMETYPE = 3 then 'playoff'
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
        cast(split_part(player.value:toi::string, ':', 1) as int) * 60 + cast(split_part(player.value:toi::string, ':', 2) as int) as toi
    from
        games,
        lateral flatten(input => parse_json(PLAYERBYGAMESTATS_AWAYTEAM_DEFENSE)) player
),

home_team_forwards as (
    select
        ID::int as game_id,
        SEASON::int as season,
        HOMETEAM_ABBREV::string as team_abv,
        'home' as type,
        case
            when GAMETYPE = 2 then 'regular'
            when GAMETYPE = 3 then 'playoff'
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
        cast(split_part(player.value:toi::string, ':', 1) as int) * 60 + cast(split_part(player.value:toi::string, ':', 2) as int) as toi
    from
        games,
        lateral flatten(input => parse_json(PLAYERBYGAMESTATS_HOMETEAM_FORWARDS)) player
),

home_team_defense as (
    select
        ID::int as game_id,
        SEASON::int as season,
        HOMETEAM_ABBREV::string as team_abv,
        'home' as type,
        case
            when GAMETYPE = 2 then 'regular'
            when GAMETYPE = 3 then 'playoff'
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
        cast(split_part(player.value:toi::string, ':', 1) as int) * 60 + cast(split_part(player.value:toi::string, ':', 2) as int) as toi
    from
        games,
        lateral flatten(input => parse_json(PLAYERBYGAMESTATS_HOMETEAM_DEFENSE)) player
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
-- league games only: excludes All-Star / 4 Nations / preseason contamination
where game_id in (select game_id from {{ ref('int__league_games') }})
