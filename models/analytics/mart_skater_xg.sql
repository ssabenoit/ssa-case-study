-- models/analytics/mart_skater_xg.sql
-- Skater shot quality per season: expected vs actual goals, finishing.
-- Grain: one row per (season, shooter). Uses season-calibrated xG.

with

shots as (
    select
        x.season_key as season,
        x.shooter_player_key as player_id,
        x.is_goal,
        x.xg_raw * c.calibration_factor as xg
    from {{ ref('fct_shots_xg') }} x
    inner join {{ ref('int__xg_season_calibration') }} c
        on c.season_key = x.season_key
    where x.shooter_player_key is not null
)

select
    s.season,
    {{ season_display('s.season') }} as season_display,
    s.player_id,
    dp.full_name,
    dp.primary_position_code as position,
    count(*) as shot_attempts,
    sum(s.is_goal) as goals,
    round(sum(s.xg), 2) as expected_goals,
    round(sum(s.is_goal) - sum(s.xg), 2) as goals_above_expected,
    round({{ safe_divide('sum(s.is_goal)::float', 'count(*)') }}, 4) as conversion_rate,
    round({{ safe_divide('sum(s.xg)', 'count(*)') }}, 4) as avg_shot_quality
from shots s
left join {{ ref('dim_players') }} dp
    on dp.player_id = s.player_id
group by s.season, s.player_id, dp.full_name, dp.primary_position_code
