-- models/staging/stg_nhl__season_schedules.sql
-- Pulls every game past and scheduled within the scope of the data

with

games as (
    select
        *,
        parse_json("venue") as venue_json,
        parse_json("awayTeam") as awayTeam_json,
        parse_json("homeTeam") as homeTeam_json,
        parse_json("winningGoalie") as winningGoalie_json,
        parse_json("winningGoalScorer") as winningGoalScorer_json,
        parse_json("specialEvent") as specialEvent_json
    from {{ source("nhl_staging_data", "season_schedules") }}
)

select
    "id"::int as id,
    venue_json:default::string as venue,
    "season"::int as season,
    awayTeam_json:abbrev::string as away_abv,
    awayTeam_json:id::int as away_id,
    homeTeam_json:abbrev::string as home_abv,
    homeTeam_json:id::int as home_id,
    "gameDate" as game_date,
    "gameType"::int as game_type,
    "gameState" as game_state,
    winningGoalie_json:playerId::int as winning_goalie_id,
    winningGoalScorer_json:playerId::int as winning_scorer_id,
    "neutralSite"::boolean as neutral,
    "startTimeUTC" as start_time_utc,
    "easternUTCOffset" as eastern_offset,
    "venueTimezone"::string as venue_tz,
    specialEvent_json:name::string as special_event
from games
