-- models/staging/stg_nhl__game_summaries.sql
-- Team-level view of each game summary: one row per (game_id, team_id),
-- i.e. two rows per game (home and away).
--
-- Team counting stats (hits, blocks, PIM, power play, faceoffs) are NOT
-- available here: the loader does not land summary.teamGameStats as a column.
-- Those metrics are assembled in int__team_per_game_stats from the player
-- boxscore (int__skaters_per_game_stats) and play-by-play events
-- (int__game_penalties, int__game_faceoffs).

with

summaries as (
    select *
    from {{ source('nhl_staging_data', 'game_summaries') }}
    -- loader appends re-extractions; latest load per game wins
    qualify row_number() over (partition by ID order by _loaded_at desc) = 1
),

away_team as (
    select
        SEASON::string as season,
        ID::int as game_id,
        AWAYTEAM_ABBREV::string as team_abv,
        AWAYTEAM_ID::int as team_id,
        'away' as type,
        case
            when GAMETYPE = 1 then 'preseason'
            when GAMETYPE = 2 then 'regular'
            when GAMETYPE = 3 then 'playoff'
            else 'other'
        end as game_type,
        AWAYTEAM_SOG::int as shots,
        AWAYTEAM_SCORE::int as goals,
        HOMETEAM_SCORE::int as goals_against,
        AWAYTEAM_SOG::int as sog,
        GAMEDATE::date as game_date,
        STARTTIMEUTC::string as start_time_utc,
        VENUE_DEFAULT::string as venue,
        PERIODDESCRIPTOR_PERIODTYPE::string as last_period_type
    from summaries
),

home_team as (
    select
        SEASON::string as season,
        ID::int as game_id,
        HOMETEAM_ABBREV::string as team_abv,
        HOMETEAM_ID::int as team_id,
        'home' as type,
        case
            when GAMETYPE = 1 then 'preseason'
            when GAMETYPE = 2 then 'regular'
            when GAMETYPE = 3 then 'playoff'
            else 'other'
        end as game_type,
        HOMETEAM_SOG::int as shots,
        HOMETEAM_SCORE::int as goals,
        AWAYTEAM_SCORE::int as goals_against,
        HOMETEAM_SOG::int as sog,
        GAMEDATE::date as game_date,
        STARTTIMEUTC::string as start_time_utc,
        VENUE_DEFAULT::string as venue,
        PERIODDESCRIPTOR_PERIODTYPE::string as last_period_type
    from summaries
)

select *
from home_team
union all
select *
from away_team
