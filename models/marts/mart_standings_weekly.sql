-- models/marts/mart_standings_weekly.sql
-- Weekly standings snapshots: every Monday plus each season's final date.
-- Purpose-built for time-series dashboard cards (bump chart, points race):
-- ~27 weeks x 32 teams ≈ 900 rows per season, safely under the 2,000-row
-- Metabase public/embed API cap that silently truncates daily-grain charts.

with

standings as (
    select *
    from {{ ref('int__standings_by_day') }}
),

season_final_dates as (
    select
        season,
        max(date) as final_date
    from standings
    group by season
)

select
    s.date,
    s.season,
    {{ season_display('s.season') }} as season_display,
    s.team_abv,
    s.team_name,
    s.division,
    s.conference,
    s.games_played,
    s.points,
    s.point_pct,
    s.wins,
    s.losses,
    s.ot_losses,
    s.regulation_plus_ot_wins as row_wins,
    s.goals_for,
    s.goals_against,
    s.goal_diff,
    s.div_sequence as division_rank,
    s.conf_sequence as conference_rank,
    s.league_sequence as league_rank,
    s.wc_sequence as wildcard_rank,
    (s.date = fd.final_date) as is_season_final,
    weekofyear(s.date) as week_of_year
from standings s
inner join season_final_dates fd
    on fd.season = s.season
where dayofweekiso(s.date) = 1  -- Mondays
    or s.date = fd.final_date
