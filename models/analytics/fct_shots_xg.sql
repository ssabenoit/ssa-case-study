{{ config(materialized='incremental', unique_key='game_key', incremental_strategy='delete+insert') }}

-- models/analytics/fct_shots_xg.sql
-- Expected goals per unblocked, non-empty-net shot attempt. Scoring applies
-- the trained logistic coefficients (seeds/xg_coefficients.csv, produced by
-- analytics/train_xg.py) to the exact features in int__shot_features —
-- training and serving share one feature model by construction.
-- xg_raw is the model output; season-calibrated xG lives downstream
-- (int__xg_season_calibration + marts) to absorb era/logging drift.

with

shots as (
    select *
    from {{ ref('int__shot_features') }}
    {% if is_incremental() %}
    where loaded_at > (select coalesce(max(loaded_at), '1900-01-01') from {{ this }})
    {% endif %}
),

coefficients as (
    select feature, coefficient
    from {{ ref('xg_coefficients') }}
),

-- one row per (shot, feature) with the feature's numeric value
unpivoted as (
    select shot_key, 'shot_distance' as feature, shot_distance as value from shots
    union all select shot_key, 'log_shot_distance', log_shot_distance from shots
    union all select shot_key, 'shot_angle_abs', shot_angle_abs from shots
    union all select shot_key, 'is_power_play', is_power_play from shots
    union all select shot_key, 'is_shorthanded', is_shorthanded from shots
    union all select shot_key, 'is_rebound', is_rebound from shots
    union all select shot_key, 'is_rush', is_rush from shots
    union all select shot_key, 'shot_type_' || shot_type, 1 from shots
),

linear_term as (
    select
        u.shot_key,
        sum(u.value * c.coefficient) as z_features
    from unpivoted u
    inner join coefficients c
        on c.feature = u.feature
    group by u.shot_key
),

intercept as (
    select coefficient as b0
    from coefficients
    where feature = 'intercept'
)

select
    s.shot_key,
    s.game_key,
    s.season_key,
    s.date_key,
    s.team_key,
    s.shooter_player_key,
    s.goalie_player_key,
    s.is_goal,
    s.is_on_net,
    s.shot_type,
    s.shot_distance,
    s.shot_angle,
    s.is_power_play,
    s.is_shorthanded,
    s.is_rebound,
    s.is_rush,
    s.period_number,
    s.game_elapsed_seconds,
    1 / (1 + exp(-(i.b0 + coalesce(lt.z_features, 0)))) as xg_raw,
    s.loaded_at
from shots s
left join linear_term lt
    on lt.shot_key = s.shot_key
cross join intercept i
