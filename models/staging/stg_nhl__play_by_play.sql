with all_plays as (
    select *
    from {{ source('nhl_staging_data', 'play_by_play') }}
)

select 
    id::int as id, 
    season::int as season, 
    -- gamedate as date,
    -- gametype::int as game_type,
    awayteam:id::int as away_id,
    awayteam:abbrev::string as away_abv,
    hometeam:id::int as home_id,
    hometeam:abbrev::string as home_abv,
    -- plays.value as full_play,
    plays.value:eventId::int as event_id,
    plays.value:homeTeamDefendingSide::string as home_side,
    plays.value:periodDescriptor.number::int as period,
    plays.value:periodDescriptor.periodType::string as period_type,
    plays.value:situationCode::int as situation_code,
    plays.value:sortOrder::int as sort_order,
    plays.value:timeInPeriod::string as time_in_period,
    split_part(time_in_period, ':', 1)::int * 60 + split_part(time_in_period, ':', -1)::int as elapsed_seconds,
    plays.value:timeRemaining::string as time_remaining,
    plays.value:typeCode::int as type_code,
    plays.value:details.eventOwnerTeamId::int as play_team_id,
    case
        when play_team_id = home_id then home_abv
        when play_team_id = away_id then away_abv
        else null
    end as play_team_abv,
    plays.value:typeDescKey::string as description,
    plays.value:details.xCoord::int as x_pos,
    plays.value:details.yCoord::int as y_pos,
    plays.value:details.zoneCode::string as zone_code,
    plays.value:details as full_details
    -- count(*)
from all_plays,
lateral flatten (input => all_plays.plays) plays
