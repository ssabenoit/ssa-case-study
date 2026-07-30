-- models/analytics/mart_aging_curves.sql
-- League-average production by age and position (qualified seasons only):
-- the reference curve individual trajectories are compared against.

select
    position,
    season_age,
    count(*) as player_seasons,
    round(avg(points_per_game), 3) as avg_points_per_game,
    round(avg(goals_per_game), 3) as avg_goals_per_game,
    round(avg(points_per_60), 3) as avg_points_per_60,
    round(avg(toi_minutes_per_game), 1) as avg_toi_minutes
from {{ ref('mart_player_seasons') }}
where games_played >= 20
    and season_age between 18 and 42
    and position in ('C', 'L', 'R', 'D')
group by position, season_age
having count(*) >= 25
