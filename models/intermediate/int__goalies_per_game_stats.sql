{{ config(materialized='incremental', unique_key='game_id', incremental_strategy='delete+insert') }}

-- models/intermediate/int__goalies_per_game_stats.sql
-- Compiles per-game stats for NHL goalies from individual box scores

with

games as (
    select *
    from {{ ref("stg_nhl__game_boxscore") }}
    {% if is_incremental() %}
    -- high-watermark: only games loaded since the last run (corrections
    -- carry a newer _loaded_at, so they reprocess; delete+insert by game_id
    -- replaces the whole game's rows)
    where _loaded_at > (select coalesce(max(loaded_at), '1900-01-01') from {{ this }})
    {% endif %}
),

away_team_goalies as (
    select
        ID::int as game_id,
        _loaded_at as loaded_at,
        SEASON::int as season,
        case
            when GAMETYPE = 2 then 'regular'
            when GAMETYPE = 3 then 'playoff'
            else 'other'
        end as game_type,
        AWAYTEAM_ABBREV::string as team_abv,
        'away' as type,
        player.value:playerId::int as player_id,
        player.value:name:default::string as name,
        player.value:position::string as position,
        player.value:starter::boolean as starter,
        player.value:decision::string as result,
        player.value:goalsAgainst::int as goals_against,
        player.value:savePctg::float as save_pct,
        player.value:saveShotsAgainst::string as shots_faced,
        cast(split_part(player.value:saveShotsAgainst::string, '/', 1) as int) as shots_saved,
        cast(split_part(player.value:saveShotsAgainst::string, '/', -1) as int) as shots_against,
        player.value:evenStrengthGoalsAgainst::int as even_goals_against,
        player.value:evenStrengthShotsAgainst::string as even_shots_faced,
        cast(split_part(player.value:evenStrengthShotsAgainst::string, '/', 1) as int) as even_shots_saved,
        cast(split_part(player.value:evenStrengthShotsAgainst::string, '/', -1) as int) as even_shots_against,
        player.value:powerPlayGoalsAgainst::int as pp_goals_against,
        player.value:powerPlayShotsAgainst::string as pp_shots_faced,
        cast(split_part(player.value:powerPlayShotsAgainst::string, '/', 1) as int) as pp_shots_saved,
        cast(split_part(player.value:powerPlayShotsAgainst::string, '/', -1) as int) as pp_shots_against,
        player.value:shorthandedGoalsAgainst::int as sh_goals_against,
        player.value:shorthandedShotsAgainst::string as sh_shots_faced,
        cast(split_part(player.value:shorthandedShotsAgainst::string, '/', 1) as int) as sh_shots_saved,
        cast(split_part(player.value:shorthandedShotsAgainst::string, '/', -1) as int) as sh_shots_against,
        player.value:pim::int as pim,
        to_time(cast(
            cast(split_part(player.value:toi::string, ':', 0) as int) * 60 + cast(split_part(player.value:toi::string, ':', -1) as int)
            as string)
        ) as toi
    from
        games,
        lateral flatten(input => parse_json(PLAYERBYGAMESTATS_AWAYTEAM_GOALIES)) player
),

home_team_goalies as (
    select
        ID::int as game_id,
        _loaded_at as loaded_at,
        SEASON::int as season,
        case
            when GAMETYPE = 2 then 'regular'
            when GAMETYPE = 3 then 'playoff'
            else 'other'
        end as game_type,
        HOMETEAM_ABBREV::string as team_abv,
        'home' as type,
        player.value:playerId::int as player_id,
        player.value:name:default::string as name,
        player.value:position::string as position,
        player.value:starter::boolean as starter,
        player.value:decision::string as result,
        player.value:goalsAgainst::int as goals_against,
        player.value:savePctg::float as save_pct,
        player.value:saveShotsAgainst::string as shots_faced,
        cast(split_part(player.value:saveShotsAgainst::string, '/', 1) as int) as shots_saved,
        cast(split_part(player.value:saveShotsAgainst::string, '/', -1) as int) as shots_against,
        player.value:evenStrengthGoalsAgainst::int as even_goals_against,
        player.value:evenStrengthShotsAgainst::string as even_shots_faced,
        cast(split_part(player.value:evenStrengthShotsAgainst::string, '/', 1) as int) as even_shots_saved,
        cast(split_part(player.value:evenStrengthShotsAgainst::string, '/', -1) as int) as even_shots_against,
        player.value:powerPlayGoalsAgainst::int as pp_goals_against,
        player.value:powerPlayShotsAgainst::string as pp_shots_faced,
        cast(split_part(player.value:powerPlayShotsAgainst::string, '/', 1) as int) as pp_shots_saved,
        cast(split_part(player.value:powerPlayShotsAgainst::string, '/', -1) as int) as pp_shots_against,
        player.value:shorthandedGoalsAgainst::int as sh_goals_against,
        player.value:shorthandedShotsAgainst::string as sh_shots_faced,
        cast(split_part(player.value:shorthandedShotsAgainst::string, '/', 1) as int) as sh_shots_saved,
        cast(split_part(player.value:shorthandedShotsAgainst::string, '/', -1) as int) as sh_shots_against,
        player.value:pim::int as pim,
        to_time(cast(
            cast(split_part(player.value:toi::string, ':', 0) as int) * 60 + cast(split_part(player.value:toi::string, ':', -1) as int)
            as string)
        ) as toi
    from
        games,
        lateral flatten(input => parse_json(PLAYERBYGAMESTATS_HOMETEAM_GOALIES)) player
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
-- league games only: excludes All-Star / 4 Nations / preseason contamination
where game_id in (select game_id from {{ ref('int__league_games') }})
