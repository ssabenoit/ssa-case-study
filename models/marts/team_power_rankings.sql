{{ config(materialized='table') }}

-- models/marts/team_power_rankings.sql
-- Power rankings incorporating various team performance metrics with recency weighting
-- Enhanced to handle early season by blending with previous season's final rankings

with

teams as (
    select
        team_id,
        team_abv,
        team_name
    from {{ ref('int__teams_basic_info') }}
),

current_season as (
    select max(season) as season
    from {{ ref('int__current_standings') }}
),

previous_season as (
    select max(season) as season
    from {{ ref('standings_by_day') }}
    where season < (select season from current_season)
),

current_standings as (
    select
        team_abv,
        team,
        points,
        wins as total_wins,
        losses,
        ot_losses,
        games_played,
        point_pct
    from {{ ref('int__current_standings') }}
),

-- Get previous season's final rankings
previous_season_final as (
    select
        team_abv,
        points,
        point_pct,
        goals_for,
        goals_against,
        goal_diff,
        games_played,
        row_number() over (order by point_pct desc, wins desc, goal_diff desc) as final_rank
    from {{ ref('standings_by_day') }}
    where 
        season = (select season from previous_season)
        and date = (
            select max(date) 
            from {{ ref('standings_by_day') }} 
            where season = (select season from previous_season)
        )
),

-- Calculate previous season's power metrics for carryover
previous_season_metrics as (
    select
        psf.team_abv,
        psf.final_rank,
        psf.point_pct as prev_point_pct,
        psf.goal_diff / nullif(psf.games_played, 0) as prev_goal_diff_per_game,
        psf.goals_for / nullif(psf.games_played, 0) as prev_goals_per_game,
        psf.goals_against / nullif(psf.games_played, 0) as prev_goals_against_per_game,
        -- Convert rank to a score (best team = 100, worst = 0)
        100.0 * (32 - psf.final_rank) / 31.0 as prev_power_score
    from previous_season_final psf
),

team_stats as (
    select
        ts.team_abv,
        ts.goals,
        ts.goals_against,
        shots,
        case 
            when cs.games_played > 0 then ts.goals / cs.games_played 
            else 0 
        end as goals_per_game,
        case 
            when cs.games_played > 0 then ts.goals_against / cs.games_played 
            else 0 
        end as goals_against_per_game,
        case 
            when cs.games_played > 0 then (ts.goals - ts.goals_against) / cs.games_played 
            else 0 
        end as goal_differential_per_game,
        case 
            when cs.games_played > 0 then ts.shots / cs.games_played 
            else 0 
        end as shots_per_game,
        ts.pp_pct,
        pk_pct
    from {{ ref('team_season_stats_regular') }} ts
    inner join current_standings cs
        on ts.team_abv = cs.team_abv
    where
        ts.season = (select season from current_season)
),

daily_standings as (
    select
        date,
        team_abv,
        points,
        wins as total_wins,
        losses,
        ot_losses,
        games_played,
        case 
            when games_played > 0 then (wins + ot_losses) / games_played 
            else 0 
        end as point_pct
    from {{ ref('standings_by_day') }}
    where season = (select season from current_season)
),

last_10_games as (
    select
        team_abv,
        avg(point_pct) as last_10_points_pct,
        count(*) as games_in_sample
    from (
        select
            team_abv,
            date,
            point_pct,
            row_number() over (partition by team_abv order by date desc) as game_recency_rank
        from daily_standings
        where games_played > 0
    )
    where game_recency_rank <= 10
    group by team_abv
),

recent_games as (
    select
        g.date as game_date,
        ts.team_abv,
        case 
            when g.away_abv = ts.team_abv then 
                case
                    when ts.goals > ts.goals_against then 'win'
                    when ts.goals < ts.goals_against then 'loss'
                    else 'tie'
                end
            else
                case
                    when ts.goals < ts.goals_against then 'win'
                    when ts.goals > ts.goals_against then 'loss'
                    else 'tie'
                end
        end as result,
        ts.goals - ts.goals_against as goal_differential
    from {{ ref('int__all_games') }} g
    inner join {{ ref('int__team_per_game_stats') }} ts
        on g.id = ts.game_id
    where
        g.date >= dateadd('day', -30, current_date())
        and g.season = (select season from current_season)
),

last_5_games as (
    select
        team_abv,
        sum(case when result = 'win' then 1 else 0 end) as recent_wins,
        sum(case when result = 'loss' then 1 else 0 end) as recent_losses,
        sum(goal_differential) as recent_goal_differential,
        count(*) as games_in_sample
    from (
        select
            team_abv,
            result,
            goal_differential,
            row_number() over (partition by team_abv order by game_date desc) as game_recency_rank
        from recent_games
    )
    where game_recency_rank <= 5
    group by team_abv
),

opponent_strength as (
    select
        team_abv,
        avg(opponent_points_pct) as opponent_avg_points_pct,
        count(*) as games_played_for_sos
    from (
        select 
            g.home_abv as team_abv,
            g.away_abv as opponent_abv,
            cs.point_pct as opponent_points_pct
        from {{ ref('int__all_games') }} g
        inner join current_standings cs
            on g.away_abv = cs.team_abv
        where g.season = (select season from current_season)
        
        union all
        
        select 
            g.away_abv as team_abv,
            g.home_abv as opponent_abv,
            cs.point_pct as opponent_points_pct
        from {{ ref('int__all_games') }} g
        inner join current_standings cs
            on g.home_abv = cs.team_abv
        where g.season = (select season from current_season)
    )
    group by team_abv
),

team_metrics as (
    select
        t.team_id,
        t.team_abv,
        t.team_name,
        cs.points,
        cs.point_pct,
        cs.games_played,
        ts.goals_per_game,
        ts.goals_against_per_game,
        ts.goal_differential_per_game,
        ts.shots_per_game,
        ts.pp_pct,
        ts.pk_pct,
        coalesce(l10.last_10_points_pct, cs.point_pct, 0) as last_10_points_pct,
        coalesce(l10.games_in_sample, 0) as last_10_games_count,
        coalesce(l5.recent_wins, 0) as recent_wins,
        coalesce(l5.recent_losses, 0) as recent_losses,
        coalesce(l5.recent_goal_differential, 0) as recent_goal_differential,
        coalesce(l5.games_in_sample, 0) as last_5_games_count,
        coalesce(os.opponent_avg_points_pct, 0.500) as opponent_avg_points_pct,
        -- Previous season metrics
        psm.prev_power_score,
        psm.prev_point_pct,
        psm.prev_goal_diff_per_game,
        psm.prev_goals_per_game,
        psm.prev_goals_against_per_game
    from teams t
    inner join current_standings cs
        on t.team_abv = cs.team_abv
    inner join team_stats ts
        on t.team_abv = ts.team_abv
    left join last_10_games l10
        on t.team_abv = l10.team_abv
    left join last_5_games l5
        on t.team_abv = l5.team_abv
    left join opponent_strength os
        on t.team_abv = os.team_abv
    left join previous_season_metrics psm
        on t.team_abv = psm.team_abv
),

-- Current season calculations (only for teams with games played)
current_normalized_metrics as (
    select
        team_id,
        team_abv,
        team_name,
        games_played,
        case 
            when games_played > 0 then
                (point_pct - min(point_pct) over()) / nullif((max(point_pct) over() - min(point_pct) over()), 0) * 100
            else 0
        end as normalized_points_pct,
        
        case 
            when last_5_games_count >= 3 then
                (recent_wins - recent_losses) / nullif(last_5_games_count, 0) * 100
            else 0
        end as normalized_point_change_trend,
        
        case 
            when games_played > 0 then
                (goal_differential_per_game - min(goal_differential_per_game) over()) / 
                nullif((max(goal_differential_per_game) over() - min(goal_differential_per_game) over()), 0) * 100
            else 0
        end as normalized_goal_differential,
        
        case 
            when games_played > 0 then
                (goals_per_game - min(goals_per_game) over()) / 
                nullif((max(goals_per_game) over() - min(goals_per_game) over()), 0) * 100
            else 0
        end as normalized_goals_for,
        
        case 
            when games_played > 0 then
                100 - ((goals_against_per_game - min(goals_against_per_game) over()) / 
                nullif((max(goals_against_per_game) over() - min(goals_against_per_game) over()), 0) * 100)
            else 0
        end as normalized_goals_against,
        
        case 
            when games_played > 0 then
                (shots_per_game - min(shots_per_game) over()) / 
                nullif((max(shots_per_game) over() - min(shots_per_game) over()), 0) * 100
            else 0
        end as normalized_shot_metric,
        
        case 
            when games_played > 0 then
                (pp_pct - min(pp_pct) over()) / 
                nullif((max(pp_pct) over() - min(pp_pct) over()), 0) * 100
            else 0
        end as normalized_pp_pct,
        
        case 
            when games_played > 0 then
                (pk_pct - min(pk_pct) over()) / 
                nullif((max(pk_pct) over() - min(pk_pct) over()), 0) * 100
            else 0
        end as normalized_pk_pct,
        
        case 
            when games_played > 0 then
                (opponent_avg_points_pct - min(opponent_avg_points_pct) over()) / 
                nullif((max(opponent_avg_points_pct) over() - min(opponent_avg_points_pct) over()), 0) * 100
            else 50  -- Neutral schedule strength when no games
        end as normalized_schedule_strength,
        
        case 
            when last_10_games_count > 0 then
                (last_10_points_pct - min(last_10_points_pct) over()) / 
                nullif((max(last_10_points_pct) over() - min(last_10_points_pct) over()), 0) * 100
            else 0
        end as normalized_last_10_performance,
        
        case 
            when last_5_games_count > 0 then
                (recent_goal_differential - min(recent_goal_differential) over()) / 
                nullif((max(recent_goal_differential) over() - min(recent_goal_differential) over()), 0) * 100
            else 0
        end as normalized_recent_goal_differential
    from team_metrics
),

-- Previous season calculations (using last season's final metrics)
previous_normalized_metrics as (
    select
        team_id,
        team_abv,
        team_name,
        coalesce(prev_power_score, 50) as prev_power_score,  -- Default to middle ranking if no previous season
        
        -- For teams without previous season data, use league average (50)
        coalesce(
            (prev_point_pct - min(prev_point_pct) over()) / 
            nullif((max(prev_point_pct) over() - min(prev_point_pct) over()), 0) * 100,
            50
        ) as prev_normalized_points_pct,
        
        coalesce(
            (prev_goal_diff_per_game - min(prev_goal_diff_per_game) over()) / 
            nullif((max(prev_goal_diff_per_game) over() - min(prev_goal_diff_per_game) over()), 0) * 100,
            50
        ) as prev_normalized_goal_differential,
        
        coalesce(
            (prev_goals_per_game - min(prev_goals_per_game) over()) / 
            nullif((max(prev_goals_per_game) over() - min(prev_goals_per_game) over()), 0) * 100,
            50
        ) as prev_normalized_goals_for,
        
        coalesce(
            100 - ((prev_goals_against_per_game - min(prev_goals_against_per_game) over()) / 
            nullif((max(prev_goals_against_per_game) over() - min(prev_goals_against_per_game) over()), 0) * 100),
            50
        ) as prev_normalized_goals_against
    from team_metrics
),

-- Combine current and previous season metrics with blending
blended_metrics as (
    select
        c.team_id,
        c.team_abv,
        c.team_name,
        c.games_played,
        
        -- Calculate blend weight based on games played
        -- At 0 games: 100% previous season
        -- At 20+ games: 100% current season
        -- Linear blend in between
        case
            when c.games_played >= 20 then 1.0
            when c.games_played = 0 then 0.0
            else c.games_played / 20.0
        end as current_weight,
        
        case
            when c.games_played >= 20 then 0.0
            when c.games_played = 0 then 1.0
            else 1.0 - (c.games_played / 20.0)
        end as previous_weight,
        
        -- Current season metrics
        c.normalized_points_pct as curr_points_pct,
        c.normalized_point_change_trend as curr_trend,
        c.normalized_goal_differential as curr_goal_diff,
        c.normalized_goals_for as curr_goals_for,
        c.normalized_goals_against as curr_goals_against,
        c.normalized_shot_metric as curr_shots,
        c.normalized_pp_pct as curr_pp,
        c.normalized_pk_pct as curr_pk,
        c.normalized_schedule_strength as curr_sos,
        c.normalized_last_10_performance as curr_last10,
        c.normalized_recent_goal_differential as curr_recent_gd,
        
        -- Previous season metrics
        p.prev_power_score,
        p.prev_normalized_points_pct as prev_points_pct,
        p.prev_normalized_goal_differential as prev_goal_diff,
        p.prev_normalized_goals_for as prev_goals_for,
        p.prev_normalized_goals_against as prev_goals_against
        
    from current_normalized_metrics c
    inner join previous_normalized_metrics p
        on c.team_id = p.team_id
),

power_rankings as (
    select
        team_id,
        team_abv,
        team_name,
        games_played,
        current_weight,
        previous_weight,
        
        -- Blended components
        -- Points component: blend current and previous
        (current_weight * curr_points_pct + previous_weight * prev_points_pct) as points_component,
        
        -- Goal component: blend current and previous
        (current_weight * ((curr_goal_diff * 0.5) + (curr_goals_for * 0.3) + (curr_goals_against * 0.2)) +
         previous_weight * ((prev_goal_diff * 0.5) + (prev_goals_for * 0.3) + (prev_goals_against * 0.2))) as goal_component,
        
        -- Advanced stats: use current if available, otherwise neutral
        case
            when games_played >= 5 then
                (curr_shots * 0.4) + (curr_pp * 0.3) + (curr_pk * 0.3)
            else
                50  -- Neutral value for early season
        end as advanced_stats_component,
        
        -- Schedule strength: only meaningful with games played
        case
            when games_played >= 5 then curr_sos
            else 50
        end as schedule_component,
        
        -- Momentum: only use when enough games played
        case
            when games_played >= 10 then
                (curr_last10 * 0.6) + (curr_recent_gd * 0.4)
            when games_played >= 5 then
                (curr_trend * 0.5) + (curr_recent_gd * 0.5)
            else
                50  -- Neutral momentum at season start
        end as momentum_component,
        
        current_date() as ranking_date
    from blended_metrics
),

final_rankings as (
    select
        team_id,
        team_abv,
        team_name,
        games_played,
        current_weight,
        previous_weight,
        points_component,
        goal_component,
        advanced_stats_component,
        schedule_component,
        momentum_component,
        
        -- Calculate final power score with adjusted weights for early season
        case
            when games_played = 0 then
                -- Pure previous season ranking
                (points_component * 0.35) +
                (goal_component * 0.35) +
                (50 * 0.30)  -- Neutral for other components
                
            when games_played < 5 then
                -- Heavy emphasis on basic metrics, less on advanced
                (points_component * 0.35) +
                (goal_component * 0.30) +
                (advanced_stats_component * 0.15) +
                (schedule_component * 0.10) +
                (momentum_component * 0.10)
                
            when games_played < 10 then
                -- Gradual shift to full formula
                (points_component * 0.32) +
                (goal_component * 0.28) +
                (advanced_stats_component * 0.18) +
                (schedule_component * 0.12) +
                (momentum_component * 0.10)
                
            else
                -- Full formula after 10 games
                (points_component * 0.30) +
                (goal_component * 0.25) +
                (advanced_stats_component * 0.20) +
                (schedule_component * 0.15) +
                (momentum_component * 0.10)
        end as power_score,
        
        ranking_date
    from power_rankings
)

select
    ranking_date,
    team_id,
    team_abv,
    team_name,
    power_score,
    row_number() over (order by power_score desc) as power_rank,
    points_component,
    goal_component,
    advanced_stats_component,
    schedule_component,
    momentum_component,
    games_played,
    round(current_weight * 100, 1) as current_season_weight_pct,
    round(previous_weight * 100, 1) as previous_season_weight_pct
from final_rankings
order by power_rank