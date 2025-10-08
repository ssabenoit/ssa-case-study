{{ config(materialized='table') }}

-- models/dimensional/facts/fct_standings_daily.sql
-- Daily standings snapshot fact table

with

standings_base as (
    select *
    from {{ ref('standings_by_day') }}
),

-- Calculate last 10 games stats
standings_with_lag as (
    select
        *,
        lag(wins) over (partition by team_abv, season order by date) as prev_wins,
        lag(losses) over (partition by team_abv, season order by date) as prev_losses,
        lag(ot_losses) over (partition by team_abv, season order by date) as prev_ot_losses
    from standings_base
),

last_10_games as (
    select
        s1.date,
        s1.team_abv,
        s1.season,
        -- Count wins/losses/OT losses in last 10 games
        sum(case 
            when s2.date > dateadd(day, -14, s1.date) 
                and s2.wins > coalesce(s2.prev_wins, 0)
            then 1 else 0 
        end) as last_10_wins,
        sum(case 
            when s2.date > dateadd(day, -14, s1.date)
                and s2.losses > coalesce(s2.prev_losses, 0)
            then 1 else 0 
        end) as last_10_losses,
        sum(case 
            when s2.date > dateadd(day, -14, s1.date)
                and s2.ot_losses > coalesce(s2.prev_ot_losses, 0)
            then 1 else 0 
        end) as last_10_ot_losses
    from standings_base s1
    inner join standings_with_lag s2
        on s1.team_abv = s2.team_abv
        and s1.season = s2.season
        and s2.date <= s1.date
        and s2.date > dateadd(day, -14, s1.date)
    group by 
        s1.date, 
        s1.team_abv, 
        s1.season
),

-- Calculate streaks
streaks as (
    select
        date,
        team_abv,
        season,
        -- Determine current streak
        case
            when wins > lag(wins, 1, 0) over (partition by team_abv, season order by date) then 'W'
            when losses > lag(losses, 1, 0) over (partition by team_abv, season order by date) then 'L'
            when ot_losses > lag(ot_losses, 1, 0) over (partition by team_abv, season order by date) then 'OT'
            else null
        end as last_game_result,
        -- Running streak count (simplified - would need more complex logic for accurate streaks)
        1 as streak_count
    from standings_base
),

standings_facts as (
    select
        -- Keys
        cast(replace(cast(date(sb.date) as string), '-', '') as int) as date_key,
        sb.season as season_key,
        dt.team_key,
        
        -- Core standings metrics
        sb.games_played,
        sb.wins,
        sb.losses,
        sb.ot_losses,
        sb.points,
        sb.wins + sb.ot_losses as row_wins,  -- Regulation + OT wins
        sb.point_pct as points_percentage,
        
        -- Goals
        sb.goals_for,
        sb.goals_against,
        sb.goal_diff as goal_differential,
        
        -- Home/Away splits (would need to calculate from game logs)
        null::int as home_wins,
        null::int as home_losses,
        null::int as home_ot_losses,
        null::int as away_wins,
        null::int as away_losses,
        null::int as away_ot_losses,
        
        -- Last 10 games
        coalesce(l10.last_10_wins, 0) as last_10_wins,
        coalesce(l10.last_10_losses, 0) as last_10_losses,
        coalesce(l10.last_10_ot_losses, 0) as last_10_ot_losses,
        
        -- Streak information
        s.last_game_result as streak_type,
        s.streak_count,
        
        -- Rankings
        sb.div_sequence as division_rank,
        sb.conf_sequence as conference_rank,
        sb.league_sequence as league_rank,
        sb.wc_sequence as wildcard_rank,
        
        -- Playoff positioning
        case
            when sb.div_sequence <= 3 then true  -- Top 3 in division
            when sb.wc_sequence <= 2 then true    -- Wildcard spot
            else false
        end as playoff_spot_flag,
        
        -- Distance from playoff line
        case
            when sb.div_sequence <= 3 or sb.wc_sequence <= 2 then 0
            else (
                select min(s2.points) 
                from standings_base s2 
                where s2.date = sb.date 
                    and s2.season = sb.season
                    and s2.conference = sb.conference
                    and (s2.div_sequence = 3 or s2.wc_sequence = 2)
            ) - sb.points
        end as points_from_playoff,
        
        -- Games remaining
        82 - sb.games_played as games_remaining,
        
        -- Maximum possible points
        sb.points + (82 - sb.games_played) * 2 as max_possible_points,
        
        -- Magic/Tragic numbers (simplified)
        null::int as magic_number,
        null::int as elimination_number,
        
        -- Metadata
        sb.division,
        sb.conference,
        sb.date,
        sb.team_abv,
        sb.team_name
        
    from standings_base sb
    left join {{ ref('dim_teams') }} dt
        on sb.team_abv = dt.team_abv
    left join last_10_games l10
        on sb.date = l10.date
        and sb.team_abv = l10.team_abv
        and sb.season = l10.season
    left join streaks s
        on sb.date = s.date
        and sb.team_abv = s.team_abv
        and sb.season = s.season
)

select
    date_key,
    season_key,
    team_key,
    games_played,
    wins,
    losses,
    ot_losses,
    points,
    row_wins,
    points_percentage,
    goals_for,
    goals_against,
    goal_differential,
    home_wins,
    home_losses,
    home_ot_losses,
    away_wins,
    away_losses,
    away_ot_losses,
    last_10_wins,
    last_10_losses,
    last_10_ot_losses,
    streak_type,
    streak_count,
    division_rank,
    conference_rank,
    league_rank,
    wildcard_rank,
    playoff_spot_flag,
    points_from_playoff,
    games_remaining,
    max_possible_points,
    magic_number,
    elimination_number,
    -- Calculated pace metrics
    case
        when games_played > 0
        then round(points * 82.0 / games_played, 1)
        else 0
    end as points_pace,
    case
        when games_played > 0
        then round(wins * 82.0 / games_played, 1)
        else 0
    end as wins_pace,
    case
        when games_played > 0
        then round(goals_for * 82.0 / games_played, 1)
        else 0
    end as goals_for_pace,
    case
        when games_played > 0
        then round(goals_against * 82.0 / games_played, 1)
        else 0
    end as goals_against_pace
from standings_facts
order by 
    date_key, 
    team_key