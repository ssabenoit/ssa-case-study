-- models/staging/stg_nhl__shift_charts.sql
-- One row per shift record. typeCode 517 = an actual player shift; other
-- type codes are shift-chart annotations (e.g. goals) and are excluded here.

with

source as (
    select *
    from {{ source('nhl_staging_data', 'shift_charts') }}
)

select
    ID::int as shift_id,
    GAMEID::int as game_id,
    PLAYERID::int as player_id,
    TEAMID::int as team_id,
    TEAMABBREV::string as team_abv,
    FIRSTNAME::string as first_name,
    LASTNAME::string as last_name,
    PERIOD::int as period,
    SHIFTNUMBER::int as shift_number,
    STARTTIME::string as start_time,
    ENDTIME::string as end_time,
    {{ parse_toi('STARTTIME::string') }} as start_seconds,
    {{ parse_toi('ENDTIME::string') }} as end_seconds,
    {{ parse_toi('DURATION::string') }} as duration_seconds,
    _loaded_at
from source
where TYPECODE = 517
    and DURATION is not null
qualify row_number() over (partition by ID order by _loaded_at desc) = 1
