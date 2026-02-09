{{ config(materialized='table') }}

-- models/intermediate/int__current_standings.sql
-- Formats the current NHL standings with all relevant statistics

with

raw_standings as (
    select *
    from {{ ref("stg_nhl__current_standings") }}
)

select
    TEAMNAME_DEFAULT::string as team,
    TEAMABBREV_DEFAULT::string as team_abv,
    DIVISIONNAME as division,
    DIVISIONABBREV as division_abv,
    CONFERENCENAME as conference,
    CONFERENCEABBREV as conference_abv,
    LEAGUESEQUENCE::int as league_standing,
    CONFERENCESEQUENCE::int as conference_standing,
    DIVISIONSEQUENCE::int as division_standing,
    WILDCARDSEQUENCE::int as wildcard_standing,
    WAIVERSSEQUENCE::int as waiver,
    POINTS::int as points,
    WINS::int as wins,
    LOSSES::int as losses,
    OTLOSSES::int as ot_losses,
    GAMESPLAYED::int as games_played,
    concat(wins, '-', losses, '-', ot_losses) as record,
    WINPCTG as win_pct,
    POINTPCTG as point_pct,
    REGULATIONWINS::int as reg_wins,
    REGULATIONPLUSOTWINS::int as reg_ot_wins,
    REGULATIONPLUSOTWINPCTG as reg_ot_win_pct,
    REGULATIONWINPCTG as reg_win_pct,
    SHOOTOUTWINS::int as shootout_w,
    SHOOTOUTLOSSES::int as shootout_l,
    concat(shootout_w, '-', shootout_l) as shootout_record,
    GOALFOR::int as goals_for,
    GOALAGAINST::int as goals_against,
    GOALDIFFERENTIAL::int as goal_diff,
    case
        when goal_diff > 0 then concat('+', goal_diff)
        else cast(goal_diff as string)
    end as goal_diff_string,
    GOALSFORPCTG as goals_per_game,
    GOALDIFFERENTIALPCTG as goal_diff_pct,
    HOMEWINS::int as home_wins,
    HOMELOSSES::int as home_losses,
    HOMEOTLOSSES::int as home_ot_l,
    HOMEPOINTS::int as home_points,
    concat(home_wins, '-', home_losses, '-', home_ot_l) as home_record,
    HOMEGAMESPLAYED::int as home_gp,
    HOMEGOALSFOR::int as home_goals,
    HOMEGOALSAGAINST::int as home_goals_against,
    HOMEGOALDIFFERENTIAL::int as home_goal_diff,
    case
        when home_goal_diff > 0 then concat('+', home_goal_diff)
        else cast(home_goal_diff as string)
    end as home_goal_diff_string,
    HOMEREGULATIONWINS::int as home_reg_wins,
    HOMEREGULATIONPLUSOTWINS::int as home_reg_ot_wins,
    LEAGUEHOMESEQUENCE::int as league_home_standing,
    DIVISIONHOMESEQUENCE::int as div_home_standing,
    CONFERENCEHOMESEQUENCE::int as conf_home_standing,
    ROADWINS::int as road_wins,
    ROADLOSSES::int as road_losses,
    ROADOTLOSSES::int as road_ot_l,
    ROADPOINTS::int as road_points,
    concat(road_wins, '-', road_losses, '-', road_ot_l) as road_record,
    ROADGAMESPLAYED::int as road_gp,
    ROADGOALSFOR::int as road_goals,
    ROADGOALSAGAINST::int as road_goals_against,
    ROADGOALDIFFERENTIAL::int as road_goal_diff,
    case
        when road_goal_diff > 0 then concat('+', road_goal_diff)
        else cast(road_goal_diff as string)
    end as road_goal_diff_string,
    ROADREGULATIONWINS::int as road_reg_wins,
    ROADREGULATIONPLUSOTWINS::int as road_reg_ot_wins,
    LEAGUEROADSEQUENCE::int as league_road_standing,
    DIVISIONROADSEQUENCE::int as div_road_standing,
    CONFERENCEROADSEQUENCE::int as conf_road_standing,
    STREAKCODE as streak,
    STREAKCOUNT::int as streak_count,
    concat(streak, streak_count) as streak_string,
    L10GAMESPLAYED::int as l10_gp,
    L10WINS::int as l10_wins,
    L10LOSSES::int as l10_losses,
    L10OTLOSSES::int as l10_ot_l,
    concat(l10_wins, '-', l10_losses, '-', l10_ot_l) as l10_record,
    L10POINTS::int as l10_points,
    L10GOALSFOR::int as l10_goals,
    L10GOALSAGAINST::int as l10_goals_against,
    L10GOALDIFFERENTIAL::int as l10_goal_diff,
    L10REGULATIONWINS::int as l10_reg_wins,
    L10REGULATIONPLUSOTWINS::int as l10_reg_ot_wins,
    DIVISIONL10SEQUENCE::int as div_l10_standing,
    CONFERENCEL10SEQUENCE::int as conf_l10_standing
from raw_standings
