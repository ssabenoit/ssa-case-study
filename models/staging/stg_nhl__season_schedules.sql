-- models/staging/stg_nhl__season_schedules.sql
-- Pulls every game past and scheduled within the scope of the data

with

games as (
    select *
    from {{ source("nhl_staging_data", "season_schedules") }}
)

select
    id::int as id,
    venue:default::string as venue,
    season::int as season,
    awayteam:abbrev::string as away_abv,
    awayteam:id::int as away_id,
    hometeam:abbrev::string as home_abv,
    hometeam:id::int as home_id,
    gamedate as game_date,
    gametype::int as game_type,
    gamestate as game_state,
    winninggoalie:playerId::int as winning_goalie_id,
    winninggoalscorer:playerId::int as winning_scorer_id,
    neutralsite::boolean as neutral,
    starttimeutc as start_time_utc,
    easternutcoffset as eastern_offset,
    venuetimezone::string as venue_tz,
    specialevent:name::string as special_event
from games
