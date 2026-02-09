-- models/staging/stg_nhl__play_by_play.sql
-- Standardizes play-by-play event data from the NHL API
-- Data is already flattened to one row per event

with

source as (
    select *
    from {{ source('nhl_staging_data', 'play_by_play') }}
)

select
    GAME_ID::int as game_id,
    EVENTID::int as event_id,
    HOMETEAMDEFENDINGSIDE::string as home_side,
    PERIODDESCRIPTOR_NUMBER::int as period,
    PERIODDESCRIPTOR_PERIODTYPE::string as period_type,
    SITUATIONCODE::string as situation_code,
    SORTORDER::int as sort_order,
    TIMEINPERIOD::string as time_in_period,
    split_part(TIMEINPERIOD, ':', 1)::int * 60 + split_part(TIMEINPERIOD, ':', -1)::int as elapsed_seconds,
    TIMEREMAINING::string as time_remaining,
    TYPECODE::int as type_code,
    DETAILS_EVENTOWNERTEAMID::int as play_team_id,
    TYPEDESCKEY::string as description,
    DETAILS_XCOORD::float as x_pos,
    DETAILS_YCOORD::float as y_pos,
    DETAILS_ZONECODE::string as zone_code,
    DETAILS_SHOTTYPE::string as shot_type,
    DETAILS_REASON::string as penalty_reason,
    DETAILS_DURATION::int as penalty_duration,
    DETAILS_SCORINGPLAYERID::int as scoring_player_id,
    DETAILS_ASSIST1PLAYERID::int as assist1_player_id,
    DETAILS_ASSIST2PLAYERID::int as assist2_player_id,
    DETAILS_SHOOTINGPLAYERID::int as shooting_player_id,
    DETAILS_GOALIEINNETID::int as goalie_in_net_id,
    DETAILS_HITTINGPLAYERID::int as hitting_player_id,
    DETAILS_HITTEEPLAYERID::int as hittee_player_id,
    DETAILS_BLOCKINGPLAYERID::int as blocking_player_id,
    DETAILS_COMMITTEDBYPLAYERID::int as committed_by_player_id,
    DETAILS_DRAWNBYPLAYERID::int as drawn_by_player_id,
    DETAILS_AWAYSCORE::int as away_score,
    DETAILS_HOMESCORE::int as home_score,
    DETAILS_AWAYSOG::int as away_sog,
    DETAILS_HOMESOG::int as home_sog
from source
qualify row_number() over (partition by GAME_ID, EVENTID order by SORTORDER) = 1
