-- models/intermediate/int__basic_games_info.sql
-- neatly compile the basic information for all the games in the db

with raw_games as (
    select *
    from {{ ref("stg_nhl__games") }}
)

select
    id::INT as game_id,
    date,
    season::INT as season,
    venue:default::STRING as venue,
    TO_TIMESTAMP(starttimeutc, 'YYYY-MM-DDTHH24:MI:SSZ') AS start_time_utc,
    venuetimezone as venue_tz,
    venueutcoffset::STRING as venue_utc_offset,
    easternutcoffset::STRING as eastern_utc_offset,
    neutralsite::BOOLEAN as is_neutral
from raw_games