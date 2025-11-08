-- models/intermediate/int__basic_games_info.sql
-- Compiles basic information for all games in the database

with

raw_games as (
    select *
    from {{ ref("stg_nhl__games") }}
)

select
    "id"::int as game_id,
    "gameDate" as date,
    "season"::int as season,
    parse_json("venue"):default::string as venue,
    to_timestamp("startTimeUTC", 'YYYY-MM-DDTHH24:MI:SSZ') as start_time_utc,
    "venueTimezone" as venue_tz,
    "venueUTCOffset"::string as venue_utc_offset,
    "easternUTCOffset"::string as eastern_utc_offset,
    "neutralSite"::boolean as is_neutral
from raw_games
