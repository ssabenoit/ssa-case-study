-- models/analytics/mart_team_xg.sql
-- Team shot quality per season: xG for/against, xG share.
-- Grain: one row per (season, team).

with

shots as (
    select
        x.season_key,
        x.game_key,
        x.team_key,
        x.is_goal,
        x.xg_raw * c.calibration_factor as xg
    from {{ ref('fct_shots_xg') }} x
    inner join {{ ref('int__xg_season_calibration') }} c
        on c.season_key = x.season_key
    where x.team_key is not null
),

-- attempts against = the other team's attempts in the same game
per_team as (
    select
        s.season_key as season,
        t.team_key,
        sum(case when s.team_key = t.team_key then s.xg end) as xg_for,
        sum(case when s.team_key != t.team_key then s.xg end) as xg_against,
        sum(case when s.team_key = t.team_key then s.is_goal end) as goals_for
    from shots s
    inner join (select distinct season_key, game_key, team_key from shots) t
        on t.game_key = s.game_key
    group by s.season_key, t.team_key
)

select
    p.season,
    {{ season_display('p.season') }} as season_display,
    dt.team_abv,
    dt.team_name,
    round(p.xg_for, 1) as xg_for,
    round(p.xg_against, 1) as xg_against,
    round({{ safe_divide('p.xg_for', 'p.xg_for + p.xg_against') }}, 4) as xg_share,
    p.goals_for,
    round(p.goals_for - p.xg_for, 1) as goals_above_expected
from per_team p
left join {{ ref('dim_teams') }} dt
    on dt.team_key = p.team_key
