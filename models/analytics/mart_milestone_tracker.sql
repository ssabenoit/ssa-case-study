-- models/analytics/mart_milestone_tracker.sql
-- Career milestone watch for active players: current totals within the
-- loaded era plus official career totals from the landing feed, distance
-- to round-number milestones, and pace to reach them.

with career as (
    select
        p.player_id,
        dp.full_name,
        dp.current_team_abv,
        sum(p.goals) as era_goals,
        sum(p.points) as era_points,
        count(*) as era_games
    from {{ ref('fct_player_game_stats') }} p
    left join {{ ref('dim_players') }} dp on dp.player_id = p.player_id
    where p.game_type = 'regular'
    group by p.player_id, dp.full_name, dp.current_team_abv
),

with_landing as (
    select
        c.*,
        l.career_regular_games as official_career_games,
        pr.proj_goals as next_season_proj_goals,
        pr.proj_points as next_season_proj_points
    from career c
    left join {{ ref('stg_nhl__player_landing') }} l on l.player_id = c.player_id
    left join {{ ref('mart_player_projections') }} pr on pr.player_id = c.player_id
)

select
    player_id,
    full_name,
    current_team_abv,
    era_goals,
    era_points,
    era_games,
    official_career_games,
    ceil(era_goals / 100) * 100 as next_goal_milestone,
    ceil(era_goals / 100) * 100 - era_goals as goals_to_milestone,
    ceil(era_points / 100) * 100 as next_point_milestone,
    ceil(era_points / 100) * 100 - era_points as points_to_milestone,
    next_season_proj_goals,
    next_season_proj_points,
    case
        when next_season_proj_goals > 0
        then round((ceil(era_goals / 100) * 100 - era_goals) / (next_season_proj_goals / 82.0), 0)
    end as projected_games_to_goal_milestone
from with_landing
where era_games >= 100
