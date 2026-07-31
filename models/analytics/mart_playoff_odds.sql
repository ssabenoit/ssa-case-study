-- models/analytics/mart_playoff_odds.sql
-- Monte Carlo season odds, full run history (the "odds over time" series)
-- plus an is_latest flag for current-state cards.

select
    s.run_date,
    s.season,
    {{ season_display('s.season') }} as season_display,
    s.team as team_abv,
    dt.team_name,
    s.exp_points,
    s.playoff_odds,
    s.division_odds,
    s.presidents_odds,
    s.cup_odds,
    (s.run_date = max(s.run_date) over (partition by s.season)) as is_latest
from {{ source('nhl_analytics_engine', 'sim_team_season') }} s
left join {{ ref('dim_teams') }} dt
    on dt.team_abv = s.team
