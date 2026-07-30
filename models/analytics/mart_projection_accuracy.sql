-- models/analytics/mart_projection_accuracy.sql
-- The engine grades itself: every run-dated game projection joined to the
-- actual outcome once the game completes. Brier score and calibration by
-- probability bucket, per run month. Empty until projected games are played.

with graded as (
    select
        p.run_date,
        p.game_id,
        p.p_home_win,
        (t.goals > t.goals_against) as home_won
    from {{ source('nhl_analytics_engine', 'game_projections') }} p
    inner join {{ ref('int__team_per_game_stats') }} t
        on t.game_id = p.game_id and t.type = 'home'
)

select
    date_trunc('month', run_date) as run_month,
    count(*) as games_graded,
    round(avg(power(p_home_win - home_won::int, 2)), 5) as brier_score,
    round(avg(p_home_win), 4) as avg_predicted_home_win,
    round(avg(home_won::int), 4) as actual_home_win_rate,
    round(avg(case when p_home_win >= 0.5 then home_won::int
                   else 1 - home_won::int end), 4) as favorite_hit_rate
from graded
group by run_month
