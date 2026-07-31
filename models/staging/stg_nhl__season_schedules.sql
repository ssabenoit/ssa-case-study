-- models/staging/stg_nhl__season_schedules.sql
-- Pulls every game past and scheduled within the scope of the data
-- Grain: one row per game; the schedule is re-extracted and full-replaced,
-- so we keep only the most recently loaded row per game.
--
-- The table's columns vary with the schedule's era: a future-only schedule
-- (e.g. next season published in July) has no winner or special-event
-- columns yet. Guard the volatile ones so full replaces never break staging.

{% set schedule_source = source("nhl_staging_data", "season_schedules") %}
{% set available_columns = adapter.get_columns_in_relation(schedule_source)
       | map(attribute="name") | map("upper") | list %}

with

games as (
    select *
    from {{ schedule_source }}
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
    {{ 'WINNINGGOALIE_PLAYERID::int' if 'WINNINGGOALIE_PLAYERID' in available_columns else 'null::int' }} as winning_goalie_id,
    {{ 'WINNINGGOALSCORER_PLAYERID::int' if 'WINNINGGOALSCORER_PLAYERID' in available_columns else 'null::int' }} as winning_scorer_id,
    NEUTRALSITE::boolean as neutral,
    STARTTIMEUTC as start_time_utc,
    EASTERNUTCOFFSET as eastern_offset,
    VENUETIMEZONE::string as venue_tz,
    {{ 'SPECIALEVENT_NAME_DEFAULT::string' if 'SPECIALEVENT_NAME_DEFAULT' in available_columns else 'null::string' }} as special_event
from games
qualify row_number() over (partition by ID order by _loaded_at desc) = 1
