{{ config(materialized='table') }}

-- models/dimensional/facts/fct_standings_daily.sql
-- Daily standings snapshot fact table with complete home/road/shootout
-- records, true regulation/ROW wins, and the league's own last-10 splits.
-- Grain: one row per (date, team).

with

standings_base as (
    select *
    from {{ ref('int__standings_by_day') }}
),

standings_facts as (
    select
        -- Keys
        {{ dbt_utils.generate_surrogate_key(['sb.date', 'sb.team_abv']) }} as standings_daily_key,
        cast(replace(cast(date(sb.date) as string), '-', '') as int) as date_key,
        sb.season as season_key,
        dt.team_key,

        -- Core standings metrics
        sb.games_played,
        sb.wins,
        sb.losses,
        sb.ot_losses,
        sb.points,
        sb.regulation_wins,
        -- ROW = regulation + overtime wins (shootout wins excluded), straight
        -- from the league feed. (Previously mis-derived as wins + OT losses.)
        sb.regulation_plus_ot_wins as row_wins,
        sb.point_pct as points_percentage,

        -- Goals
        sb.goals_for,
        sb.goals_against,
        sb.goal_diff as goal_differential,

        -- Home/Away splits
        coalesce(sb.home_wins, 0) as home_wins,
        coalesce(sb.home_losses, 0) as home_losses,
        coalesce(sb.home_ot_losses, 0) as home_ot_losses,
        coalesce(sb.road_wins, 0) as away_wins,
        coalesce(sb.road_losses, 0) as away_losses,
        coalesce(sb.road_ot_losses, 0) as away_ot_losses,

        -- Shootout records
        coalesce(sb.shootout_wins, 0) as shootout_wins,
        coalesce(sb.shootout_losses, 0) as shootout_losses,

        -- Last 10 games: the league's own rolling split (true last 10 games,
        -- not a calendar-window approximation)
        coalesce(sb.l10_wins, 0) as last_10_wins,
        coalesce(sb.l10_losses, 0) as last_10_losses,
        coalesce(sb.l10_ot_losses, 0) as last_10_ot_losses,
        coalesce(sb.l10_points, 0) as last_10_points,
        coalesce(sb.l10_goals_for, 0) as last_10_goals_for,
        coalesce(sb.l10_goals_against, 0) as last_10_goals_against,
        coalesce(sb.l10_goal_diff, 0) as last_10_goal_differential,

        -- Streak information
        sb.streak_code as streak_type,
        sb.streak_count,

        -- Rankings
        sb.div_sequence as division_rank,
        sb.conf_sequence as conference_rank,
        sb.league_sequence as league_rank,
        sb.wc_sequence as wildcard_rank,

        -- Playoff positioning
        case
            when sb.div_sequence <= 3 then true  -- Top 3 in division
            when sb.wc_sequence <= 2 then true   -- Wildcard spot
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
        {{ var('regular_season_games') }} - sb.games_played as games_remaining,

        -- Maximum possible points
        sb.points + ({{ var('regular_season_games') }} - sb.games_played) * 2 as max_possible_points,

        -- Magic/Tragic numbers (not derived; kept for interface stability)
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
)

select
    standings_daily_key,
    date_key,
    season_key,
    team_key,
    games_played,
    wins,
    losses,
    ot_losses,
    points,
    regulation_wins,
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
    last_10_points,
    last_10_goals_for,
    last_10_goals_against,
    last_10_goal_differential,
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
        then round(points * {{ var('regular_season_games') }} * 1.0 / games_played, 1)
        else 0
    end as points_pace,
    case
        when games_played > 0
        then round(wins * {{ var('regular_season_games') }} * 1.0 / games_played, 1)
        else 0
    end as wins_pace,
    case
        when games_played > 0
        then round(goals_for * {{ var('regular_season_games') }} * 1.0 / games_played, 1)
        else 0
    end as goals_for_pace,
    case
        when games_played > 0
        then round(goals_against * {{ var('regular_season_games') }} * 1.0 / games_played, 1)
        else 0
    end as goals_against_pace,
    -- Record strings for easier reporting
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
