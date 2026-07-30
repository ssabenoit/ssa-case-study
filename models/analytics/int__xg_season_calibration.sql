-- models/analytics/int__xg_season_calibration.sql
-- Per-season xG calibration factor: actual goals / raw predicted goals.
-- Absorbs era and event-logging drift (e.g. rebound-flag density jumped
-- ~30% in 2025-26 when PBP logging got denser). Completed seasons use their
-- exact factor; a season with fewer than 200 games borrows the latest
-- complete season's factor until it has enough of its own data.

with

by_season as (
    select
        season_key,
        count(distinct game_key) as games,
        sum(is_goal) as actual_goals,
        sum(xg_raw) as predicted_goals,
        sum(is_goal) / nullif(sum(xg_raw), 0) as raw_factor,
        sum(is_goal) / nullif(sum(case when is_on_net = 1 then xg_raw end), 0) as raw_factor_on_net
    from {{ ref('fct_shots_xg') }}
    group by season_key
),

latest_mature as (
    select raw_factor as fallback_factor, raw_factor_on_net as fallback_factor_on_net
    from by_season
    where games >= 200
    qualify row_number() over (order by season_key desc) = 1
)

select
    b.season_key,
    b.games,
    b.actual_goals,
    round(b.predicted_goals, 1) as predicted_goals,
    case
        when b.games >= 200 then b.raw_factor
        else (select fallback_factor from latest_mature)
    end as calibration_factor,
    case
        when b.games >= 200 then b.raw_factor_on_net
        else (select fallback_factor_on_net from latest_mature)
    end as calibration_factor_on_net
from by_season b
