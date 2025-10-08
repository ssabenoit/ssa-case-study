-- models/intermediate/int__basic_games_info.sql
-- Compiles basic information for all games in the database

with

raw_games as (
    select *
    from {{ ref("stg_nhl__games") }}
)

select
    id::int as game_id,
    date as date,
    season::int as season,
    venue:default::string as venue,
    to_timestamp(starttimeutc, 'YYYY-MM-DDTHH24:MI:SSZ') as start_time_utc,
    venuetimezone as venue_tz,
    venueutcoffset::string as venue_utc_offset,
    easternutcoffset::string as eastern_utc_offset,
    neutralsite::boolean as is_neutral
from raw_games
