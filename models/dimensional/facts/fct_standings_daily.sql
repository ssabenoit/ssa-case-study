{{ config(materialized='table') }}

-- models/dimensional/facts/fct_standings_daily.sql
-- Daily standings snapshot fact table with complete home/road/shootout records

with

standings_base as (
    select *
    from {{ ref('int__standings_by_day') }}
),

-- Get home/road/shootout stats from raw daily standings
standings_enhanced as (
    select 
        sb.*,
        -- Parse home/road/shootout data from source if available
        ds.homewins::int as home_wins_raw,
        ds.homelosses::int as home_losses_raw,
        ds.homeotlosses::int as home_ot_losses_raw,
        ds.roadwins::int as road_wins_raw,
        ds.roadlosses::int as road_losses_raw,
        ds.roadotlosses::int as road_ot_losses_raw,
        ds.shootoutwins::int as shootout_wins_raw,
        ds.shootoutlosses::int as shootout_losses_raw,
        -- Get streak info directly from source
        ds.streakcode as streak_type_raw,
        ds.streakcount::int as streak_count_raw
    from standings_base sb
    left join {{ ref('stg_nhl__daily_standings') }} ds
        on sb.date = ds.date
        and sb.team_abv = ds.teamabbrev:default::string
        and sb.season = cast(ds.seasonid as int)
),

-- Calculate last 10 games stats
standings_with_lag as (
    select
        *,
        lag(wins) over (partition by team_abv, season order by date) as prev_wins,
        lag(losses) over (partition by team_abv, season order by date) as prev_losses,
        lag(ot_losses) over (partition by team_abv, season order by date) as prev_ot_losses
    from standings_enhanced
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
    from standings_enhanced s1
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
        
        -- Home/Away splits
        coalesce(sb.home_wins_raw, 0) as home_wins,
        coalesce(sb.home_losses_raw, 0) as home_losses,
        coalesce(sb.home_ot_losses_raw, 0) as home_ot_losses,
        coalesce(sb.road_wins_raw, 0) as away_wins,
        coalesce(sb.road_losses_raw, 0) as away_losses,
        coalesce(sb.road_ot_losses_raw, 0) as away_ot_losses,
        
        -- Shootout records
        coalesce(sb.shootout_wins_raw, 0) as shootout_wins,
        coalesce(sb.shootout_losses_raw, 0) as shootout_losses,
        
        -- Last 10 games
        coalesce(l10.last_10_wins, 0) as last_10_wins,
        coalesce(l10.last_10_losses, 0) as last_10_losses,
        coalesce(l10.last_10_ot_losses, 0) as last_10_ot_losses,
        
        -- Streak information (use source data directly)
        sb.streak_type_raw as streak_type,
        sb.streak_count_raw as streak_count,
        
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
        
    from standings_enhanced sb
    left join {{ ref('dim_teams') }} dt
        on sb.team_abv = dt.team_abv
    left join last_10_games l10
        on sb.date = l10.date
        and sb.team_abv = l10.team_abv
        and sb.season = l10.season
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
    shootout_wins,
    shootout_losses,
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
    end as goals_against_pace,
    -- Build record strings for easier reporting
    concat(wins, '-', losses, '-', ot_losses) as record,
    concat(home_wins, '-', home_losses, '-', home_ot_losses) as home_record,
    concat(away_wins, '-', away_losses, '-', away_ot_losses) as away_record,
    concat(last_10_wins, '-', last_10_losses, '-', last_10_ot_losses) as last_10_record,
    concat(shootout_wins, '-', shootout_losses) as shootout_record,
    case 
        when goal_differential > 0 then concat('+', goal_differential)
        else cast(goal_differential as string)
    end as goal_diff_string,
    case
        when streak_type is not null and streak_count is not null
        then concat(streak_type, streak_count)
        else null
    end as streak_string
from standings_facts
order by 
    date_key, 
    team_key