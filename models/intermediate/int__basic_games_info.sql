-- models/intermediate/int__basic_games_info.sql
-- Compiles basic information for all games in the database

with

raw_games as (
    select *
    from {{ ref("stg_nhl__games") }}
)

select
    ID::int as game_id,
    GAMEDATE as date,
    SEASON::int as season,
    VENUE_DEFAULT::string as venue,
    to_timestamp(STARTTIMEUTC, 'YYYY-MM-DDTHH24:MI:SSZ') as start_time_utc,
    VENUETIMEZONE as venue_tz,
    VENUEUTCOFFSET::string as venue_utc_offset,
    EASTERNUTCOFFSET::string as eastern_utc_offset,
    NEUTRALSITE::boolean as is_neutral
from raw_games
