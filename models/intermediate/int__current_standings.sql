{{ config(materialized='table') }}

-- models/intermediate/int__current_standings.sql
-- Formats the current NHL standings with all relevant statistics

with

raw_standings as (
    select *
    from {{ ref("stg_nhl__current_standings") }}
)

select
    parse_json("teamName"):default::string as team,
    parse_json("teamAbbrev"):default::string as team_abv,
    "divisionName" as division,
    "divisionAbbrev" as division_abv,
    "conferenceName" as conference,
    "conferenceAbbrev" as conference_abv,
    "leagueSequence"::int as league_standing,
    "conferenceSequence"::int as conference_standing,
    "divisionSequence"::int as division_standing,
    "wildcardSequence"::int as wildcard_standing,
    "waiversSequence"::int as waiver,
    "points"::int as points,
    "wins"::int as wins,
    "losses"::int as losses,
    "otLosses"::int as ot_losses,
    "gamesPlayed"::int as games_played,
    concat(wins, '-', losses, '-', ot_losses) as record,
    "winPctg" as win_pct,
    "pointPctg" as point_pct,
    "regulationWins"::int as reg_wins,
    "regulationPlusOtWins"::int as reg_ot_wins,
    "regulationPlusOtWinPctg" as reg_ot_win_pct,
    "regulationWinPctg" as reg_win_pct,
    "shootoutWins"::int as shootout_w,
    "shootoutLosses"::int as shootout_l,
    concat(shootout_w, '-', shootout_l) as shootout_record,
    "goalFor"::int as goals_for,
    "goalAgainst"::int as goals_against,
    "goalDifferential"::int as goal_diff,
    case
        when goal_diff > 0 then concat('+', goal_diff)
        else cast(goal_diff as string)
    end as goal_diff_string,
    "goalsForPctg" as goals_per_game,
    "goalDifferentialPctg" as goal_diff_pct,
    "homeWins"::int as home_wins,
    "homeLosses"::int as home_losses,
    "homeOtLosses"::int as home_ot_l,
    "homePoints"::int as home_points,
    concat(home_wins, '-', home_losses, '-', home_ot_l) as home_record,
    "homeGamesPlayed"::int as home_gp,
    "homeGoalsFor"::int as home_goals,
    "homeGoalsAgainst"::int as home_goals_against,
    "homeGoalDifferential"::int as home_goal_diff,
    case
        when home_goal_diff > 0 then concat('+', home_goal_diff)
        else cast(home_goal_diff as string)
    end as home_goal_diff_string,
    "homeRegulationWins"::int as home_reg_wins,
    "homeRegulationPlusOtWins"::int as home_reg_ot_wins,
    "leagueHomeSequence"::int as league_home_standing,
    "divisionHomeSequence"::int as div_home_standing,
    "conferenceHomeSequence"::int as conf_home_standing,
    "roadWins"::int as road_wins,
    "roadLosses"::int as road_losses,
    "roadOtLosses"::int as road_ot_l,
    "roadPoints"::int as road_points,
    concat(road_wins, '-', road_losses, '-', road_ot_l) as road_record,
    "roadGamesPlayed"::int as road_gp,
    "roadGoalsFor"::int as road_goals,
    "roadGoalsAgainst"::int as road_goals_against,
    "roadGoalDifferential"::int as road_goal_diff,
    case
        when road_goal_diff > 0 then concat('+', road_goal_diff)
        else cast(road_goal_diff as string)
    end as road_goal_diff_string,
    "roadRegulationWins"::int as road_reg_wins,
    "roadRegulationPlusOtWins"::int as road_reg_ot_wins,
    "leagueRoadSequence"::int as league_road_standing,
    "divisionRoadSequence"::int as div_road_standing,
    "conferenceRoadSequence"::int as conf_road_standing,
    "streakCode" as streak,
    "streakCount"::int as streak_count,
    concat(streak, streak_count) as streak_string,
    "l10GamesPlayed"::int as l10_gp,
    "l10Wins"::int as l10_wins,
    "l10Losses"::int as l10_losses,
    "l10OtLosses"::int as l10_ot_l,
    concat(l10_wins, '-', l10_losses, '-', l10_ot_l) as l10_record,
    "l10Points"::int as l10_points,
    "l10GoalsFor"::int as l10_goals,
    "l10GoalsAgainst"::int as l10_goals_against,
    "l10GoalDifferential"::int as l10_goal_diff,
    "l10RegulationWins"::int as l10_reg_wins,
    "l10RegulationPlusOtWins"::int as l10_reg_ot_wins,
    "divisionL10Sequence"::int as div_l10_standing,
    "conferenceL10Sequence"::int as conf_l10_standing
from raw_standings
