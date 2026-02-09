-- models/staging/stg_nhl__season_schedules.sql
-- Pulls every game past and scheduled within the scope of the data

with

games as (
    select *
    from {{ source("nhl_staging_data", "season_schedules") }}
)

select
    ID::int as id,
    VENUE_DEFAULT::string as venue,
    SEASON::int as season,
    AWAYTEAM_ABBREV::string as away_abv,
    AWAYTEAM_ID::int as away_id,
    HOMETEAM_ABBREV::string as home_abv,
    HOMETEAM_ID::int as home_id,
    GAMEDATE as game_date,
    GAMETYPE::int as game_type,
    GAMESTATE as game_state,
    WINNINGGOALIE_PLAYERID::int as winning_goalie_id,
    WINNINGGOALSCORER_PLAYERID::int as winning_scorer_id,
    NEUTRALSITE::boolean as neutral,
    STARTTIMEUTC as start_time_utc,
    EASTERNUTCOFFSET as eastern_offset,
    VENUETIMEZONE::string as venue_tz,
    SPECIALEVENT_NAME_DEFAULT::string as special_event
from games
qualify row_number() over (partition by ID order by ID desc) = 1
