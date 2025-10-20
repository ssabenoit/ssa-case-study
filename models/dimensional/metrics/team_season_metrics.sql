{{ config(materialized='table') }}

-- models/dimensional/metrics/team_season_metrics.sql
-- Current season team performance metrics with league rankings

with

current_season as (
    select max(season_key) as season_key
    from {{ ref('fct_team_game_stats') }}
    where game_date <= current_date()
),

team_game_stats as (
    select
        tgs.*,
        dt.team_abv,
        dt.team_name,
        dt.division,
        dt.conference
    from {{ ref('fct_team_game_stats') }} tgs
    inner join {{ ref('dim_teams') }} dt
        on tgs.team_key = dt.team_key
    cross join current_season cs
    where tgs.season_key = cs.season_key
        and lower(tgs.game_type) = 'regular'
),

team_aggregates as (
    select
        team_key,
        team_abv,
        team_name,
        division,
        conference,
        
        -- Basic counts
        count(*) as games_played,
        sum(case when game_result = 'W' then 1 else 0 end) as wins,
        sum(case when game_result = 'L' then 1 else 0 end) as regulation_losses,
        sum(case when game_result = 'OTL' then 1 else 0 end) as ot_losses,
        
        -- Goals metrics
        sum(goals_for) as total_goals_for,
        sum(goals_against) as total_goals_against,
        avg(goals_for) as goals_per_game,
        avg(goals_against) as goals_against_per_game,
        
        -- Shots metrics
        sum(shots_for) as total_shots_for,
        sum(shots_against) as total_shots_against,
        avg(shots_for) as shots_per_game,
        avg(shots_against) as shots_against_per_game,
        
        -- Physical play metrics
        sum(hits) as total_hits,
        sum(blocks) as total_blocks,
        avg(hits) as hits_per_game,
        avg(blocks) as blocks_per_game,
        
        -- Special teams raw data
        sum(powerplay_goals) as total_pp_goals,
        sum(powerplay_opportunities) as total_pp_opportunities,
        sum(shorthanded_goals_against) as pk_goals_against,
        sum(times_shorthanded) as total_times_shorthanded,
        
        -- Faceoff metrics
        sum(faceoff_wins) as total_faceoff_wins,
        sum(faceoff_wins + faceoff_losses) as total_faceoff_attempts,
        
        -- Penalty metrics
        sum(penalty_minutes) as total_penalty_minutes,
        avg(penalty_minutes) as penalty_minutes_per_game,
        
        -- Giveaways/Takeaways
        sum(giveaways) as total_giveaways,
        sum(takeaways) as total_takeaways,
        avg(giveaways) as giveaways_per_game,
        avg(takeaways) as takeaways_per_game
        
    from team_game_stats
    group by
        team_key,
        team_abv,
        team_name,
        division,
        conference
),

team_metrics_calculated as (
    select
        *,
        
        -- Calculate percentages
        case 
            when total_pp_opportunities > 0 
            then round(100.0 * total_pp_goals / total_pp_opportunities, 2)
            else 0 
        end as power_play_pct,
        
        case 
            when total_times_shorthanded > 0 
            then round(100.0 * (total_times_shorthanded - pk_goals_against) / total_times_shorthanded, 2)
            else 100
        end as penalty_kill_pct,
        
        case
            when total_faceoff_attempts > 0
            then round(100.0 * total_faceoff_wins / total_faceoff_attempts, 2)
            else 50
        end as faceoff_win_pct,
        
        -- Calculate shooting percentage
        case
            when total_shots_for > 0
            then round(100.0 * total_goals_for / total_shots_for, 2)
            else 0
        end as shooting_pct,
        
        case
            when total_shots_against > 0
            then round(100.0 * (total_shots_against - total_goals_against) / total_shots_against, 2)
            else 100
        end as save_pct,
        
        -- Points calculation
        wins * 2 + ot_losses as points,
        
        -- Points percentage
        round(100.0 * (wins * 2 + ot_losses) / (games_played * 2), 3) as points_pct
        
    from team_aggregates
),

-- Calculate league rankings for each metric
team_rankings as (
    select
        *,
        
        -- Offensive rankings (higher is better)
        rank() over (order by goals_per_game desc) as goals_per_game_rank,
        rank() over (order by power_play_pct desc) as power_play_rank,
        rank() over (order by shots_per_game desc) as shots_per_game_rank,
        rank() over (order by shooting_pct desc) as shooting_pct_rank,
        
        -- Defensive rankings (lower is better, so rank ascending)
        rank() over (order by goals_against_per_game asc) as goals_against_rank,
        rank() over (order by penalty_kill_pct desc) as penalty_kill_rank,
        rank() over (order by shots_against_per_game asc) as shots_against_rank,
        rank() over (order by save_pct desc) as save_pct_rank,
        
        -- Physical/Possession rankings (higher is better)
        rank() over (order by hits_per_game desc) as hits_per_game_rank,
        rank() over (order by blocks_per_game desc) as blocks_per_game_rank,
        rank() over (order by faceoff_win_pct desc) as faceoff_rank,
        rank() over (order by takeaways_per_game desc) as takeaways_rank,
        rank() over (order by giveaways_per_game asc) as giveaways_rank,
        
        -- Overall rankings
        rank() over (order by points desc, wins desc, goals_per_game - goals_against_per_game desc) as league_rank,
        rank() over (partition by conference order by points desc, wins desc) as conference_rank,
        rank() over (partition by division order by points desc, wins desc) as division_rank,
        
        -- Calculate percentile rankings (0-100, where 100 is best)
        round(100.0 * percent_rank() over (order by goals_per_game), 1) as goals_per_game_percentile,
        round(100.0 * percent_rank() over (order by goals_against_per_game desc), 1) as goals_against_percentile,
        round(100.0 * percent_rank() over (order by power_play_pct), 1) as power_play_percentile,
        round(100.0 * percent_rank() over (order by penalty_kill_pct), 1) as penalty_kill_percentile,
        round(100.0 * percent_rank() over (order by hits_per_game), 1) as hits_percentile,
        round(100.0 * percent_rank() over (order by blocks_per_game), 1) as blocks_percentile
        
    from team_metrics_calculated
)

select
    -- Team identifiers
    team_key,
    team_abv,
    team_name,
    division,
    conference,
    
    -- Basic stats
    games_played,
    wins,
    regulation_losses,
    ot_losses,
    points,
    points_pct,
    league_rank,
    conference_rank,
    division_rank,
    
    -- Core metrics with rankings (requested metrics)
    round(goals_per_game, 2) as goals_per_game,
    goals_per_game_rank,
    goals_per_game_percentile,
    
    round(goals_against_per_game, 2) as goals_against_average,
    goals_against_rank,
    goals_against_percentile,
    
    round(hits_per_game, 1) as hits_per_game,
    hits_per_game_rank,
    hits_percentile,
    
    round(blocks_per_game, 1) as blocks_per_game,
    blocks_per_game_rank,
    blocks_percentile,
    
    power_play_pct,
    power_play_rank,
    power_play_percentile,
    
    penalty_kill_pct,
    penalty_kill_rank,
    penalty_kill_percentile,
    
    -- Additional useful metrics
    round(shots_per_game, 1) as shots_per_game,
    shots_per_game_rank,
    
    round(shots_against_per_game, 1) as shots_against_per_game,
    shots_against_rank,
    
    shooting_pct,
    shooting_pct_rank,
    
    save_pct,
    save_pct_rank,
    
    faceoff_win_pct,
    faceoff_rank,
    
    round(takeaways_per_game, 1) as takeaways_per_game,
    takeaways_rank,
    
    round(giveaways_per_game, 1) as giveaways_per_game,
    giveaways_rank,
    
    round(penalty_minutes_per_game, 1) as penalty_minutes_per_game,
    
    -- Goal differential
    round(goals_per_game - goals_against_per_game, 2) as goal_diff_per_game,
    total_goals_for - total_goals_against as goal_differential,
    
    -- Special teams opportunities
    total_pp_opportunities,
    total_pp_goals,
    total_times_shorthanded,
    
    -- Timestamp
    current_timestamp() as last_updated
    
from team_rankings
order by league_rank