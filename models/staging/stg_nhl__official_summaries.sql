-- models/staging/stg_nhl__official_summaries.sql
-- The league's own per-season skater aggregates, used as a continuous
-- parity/validation source for our derived stats. One row per
-- (season, game type, player), latest load wins.

with

source as (
    select *
    from {{ source('nhl_staging_data', 'official_skater_summary') }}
)

select
    SEASONID::int as season,
    GAMETYPEID::int as game_type_id,
    PLAYERID::int as player_id,
    SKATERFULLNAME::string as player_name,
    TEAMABBREVS::string as team_abvs,
    POSITIONCODE::string as position,
    GAMESPLAYED::int as games_played,
    GOALS::int as goals,
    ASSISTS::int as assists,
    POINTS::int as points,
    PLUSMINUS::int as plus_minus,
    PENALTYMINUTES::int as pim,
    SHOTS::int as shots,
    GAMEWINNINGGOALS::int as game_winning_goals,
    OTGOALS::int as ot_goals,
    EVGOALS::int as ev_goals,
    PPGOALS::int as pp_goals,
    SHGOALS::int as sh_goals,
    PPPOINTS::int as pp_points,
    SHPOINTS::int as sh_points,
    FACEOFFWINPCT::float as faceoff_win_pct,
    SHOOTINGPCT::float as shooting_pct,
    TIMEONICEPERGAME::float as toi_per_game_seconds,
    _loaded_at
from source
qualify row_number() over (
    partition by SEASONID, GAMETYPEID, PLAYERID
    order by _loaded_at desc
) = 1
