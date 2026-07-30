{{ config(materialized='incremental', unique_key='game_key', incremental_strategy='delete+insert') }}

-- models/analytics/int__shot_features.sql
-- Feature vector for every unblocked shot attempt (goal / shot-on-goal /
-- missed-shot), shared by xG training (analytics/train_xg.py) and scoring
-- (fct_shots_xg) so the two can never drift.
-- Blocked shots are excluded (recorded coordinates are the block location,
-- not the shot origin). Shootout attempts are excluded.

with

shots as (
    select
        p.play_key,
        p.game_key,
        p.season_key,
        p.date_key,
        p.event_type_name,
        p.event_team_key,
        p.primary_player_key as shooter_player_key,
        p.goalie_player_key,
        p.period_number,
        p.game_elapsed_seconds,
        p.shot_distance,
        p.shot_angle,
        coalesce(p.shot_type, 'unknown') as shot_type,
        p.strength_state,
        p.is_empty_net,
        p.zone_code,
        p.loaded_at,
        -- context from the immediately preceding event in the game
        lag(p.event_type_name) over (partition by p.game_key order by p.game_elapsed_seconds, p.sort_order) as prev_event,
        lag(p.event_team_key) over (partition by p.game_key order by p.game_elapsed_seconds, p.sort_order) as prev_event_team,
        lag(p.game_elapsed_seconds) over (partition by p.game_key order by p.game_elapsed_seconds, p.sort_order) as prev_elapsed,
        lag(p.zone_code) over (partition by p.game_key order by p.game_elapsed_seconds, p.sort_order) as prev_zone
    from {{ ref('fct_plays') }} p
    where p.period_category != 'Shootout'
    {% if is_incremental() %}
        and p.game_key in (
            select game_key from {{ ref('fct_plays') }}
            where loaded_at > (select coalesce(max(loaded_at), '1900-01-01') from {{ this }})
        )
    {% endif %}
)

select
    play_key as shot_key,
    game_key,
    season_key,
    date_key,
    event_team_key as team_key,
    shooter_player_key,
    goalie_player_key,
    (event_type_name = 'goal')::int as is_goal,
    (event_type_name in ('goal', 'shot-on-goal'))::int as is_on_net,
    shot_distance,
    ln(shot_distance + 1) as log_shot_distance,
    shot_angle,
    abs(shot_angle) as shot_angle_abs,
    shot_type,
    (strength_state = 'PP')::int as is_power_play,
    (strength_state = 'SH')::int as is_shorthanded,
    is_empty_net::int as is_empty_net,
    -- rebound: a shot by the same team within 3 seconds of a prior shot
    case
        when prev_event in ('shot-on-goal', 'missed-shot', 'blocked-shot')
            and prev_event_team = event_team_key
            and game_elapsed_seconds - prev_elapsed <= 3
        then 1 else 0
    end as is_rebound,
    -- rush proxy: play arrived from outside the offensive zone within 6s
    case
        when prev_zone in ('N', 'D')
            and game_elapsed_seconds - prev_elapsed <= 6
        then 1 else 0
    end as is_rush,
    period_number,
    game_elapsed_seconds,
    loaded_at
from shots
where event_type_name in ('goal', 'shot-on-goal', 'missed-shot')
    and shot_distance is not null
    and shot_angle is not null
    -- empty-net attempts are excluded from the xG model entirely: they are
    -- a different physical problem (no goalie) and wreck calibration
    and not is_empty_net
