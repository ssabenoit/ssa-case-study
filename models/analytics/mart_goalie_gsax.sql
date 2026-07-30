-- models/analytics/mart_goalie_gsax.sql
-- Goals Saved Above Expected: expected goals on shots the goalie faced
-- (on-net attempts only) minus actual goals allowed. The upgrade from
-- league-average GSAA — each goalie is judged against the quality of the
-- shots they actually faced.
-- Grain: one row per (season, goalie).

with

on_net as (
    select
        x.season_key as season,
        x.goalie_player_key as player_id,
        x.is_goal,
        x.xg_raw * c.calibration_factor_on_net as xg
    from {{ ref('fct_shots_xg') }} x
    inner join {{ ref('int__xg_season_calibration') }} c
        on c.season_key = x.season_key
    where x.goalie_player_key is not null
        -- on-net = goals + shots-on-goal (missed shots carry no goalie test)
        and x.is_on_net = 1
)

select
    o.season,
    {{ season_display('o.season') }} as season_display,
    o.player_id,
    dp.full_name,
    count(*) as shots_faced,
    sum(o.is_goal) as goals_against,
    round(sum(o.xg), 2) as expected_goals_against,
    round(sum(o.xg) - sum(o.is_goal), 2) as goals_saved_above_expected,
    round({{ safe_divide('(sum(o.xg) - sum(o.is_goal))', 'count(*) / 100') }}, 3) as gsax_per_100_shots
from on_net o
left join {{ ref('dim_players') }} dp
    on dp.player_id = o.player_id
group by o.season, o.player_id, dp.full_name
