{{ config(materialized='table') }}

-- models/intermediate/int__current_standings.sql
-- Formats the current NHL standings with all relevant statistics

with

raw_standings as (
    select *
    from {{ ref("stg_nhl__current_standings") }}
)

select
    teamname:default::string as team,
    teamabbrev:default::string as team_abv,
    divisionname as division,
    divisionabbrev as division_abv,
    conferencename as conference,
    conferenceabbrev as conference_abv,
    leaguesequence::int as league_standing,
    conferencesequence::int as conference_standing,
    divisionsequence::int as division_standing,
    wildcardsequence::int as wildcard_standing,
    waiverssequence::int as waiver,
    points::int as points,
    wins::int as wins,
    losses::int as losses,
    otlosses::int as ot_losses,
    gamesplayed::int as games_played,
    concat(wins, '-', losses, '-', otlosses) as record,
    winpctg as win_pct,
    pointpctg as point_pct,
    regulationwins::int as reg_wins,
    regulationplusotwins::int as reg_ot_wins,
    regulationplusotwinpctg as reg_ot_win_pct,
    regulationwinpctg as reg_win_pct,
    shootoutwins::int as shootout_w,
    shootoutlosses::int as shootout_l,
    concat(shootoutwins, '-', shootoutlosses) as shootout_record,
    goalfor::int as goals_for,
    goalagainst::int as goals_against,
    goaldifferential::int as goal_diff,
    case
        when goal_diff > 0 then concat('+', goal_diff)
        else cast(goal_diff as string)
    end as goal_diff_string,
    goalsforpctg as goals_per_game,
    goaldifferentialpctg as goal_diff_pct,
    homewins::int as home_wins,
    homelosses::int as home_losses,
    homeotlosses::int as home_ot_l,
    homepoints::int as home_points,
    concat(homewins, '-', homelosses, '-', homeotlosses) as home_record,
    homegamesplayed::int as home_gp,
    homegoalsfor::int as home_goals,
    homegoalsagainst::int as home_goals_against,
    homegoaldifferential::int as home_goal_diff,
    case
        when home_goal_diff > 0 then concat('+', home_goal_diff)
        else cast(home_goal_diff as string)
    end as home_goal_diff_string,
    homeregulationwins::int as home_reg_wins,
    homeregulationplusotwins::int as home_reg_ot_wins,
    leaguehomesequence::int as league_home_standing,
    divisionhomesequence::int as div_home_standing,
    conferencehomesequence::int as conf_home_standing,
    roadwins::int as road_wins,
    roadlosses::int as road_losses,
    roadotlosses::int as road_ot_l,
    roadpoints::int as road_points,
    concat(roadwins, '-', roadlosses, '-', roadotlosses) as road_record,
    roadgamesplayed::int as road_gp,
    roadgoalsfor::int as road_goals,
    roadgoalsagainst::int as road_goals_against,
    roadgoaldifferential::int as road_goal_diff,
    case
        when road_goal_diff > 0 then concat('+', road_goal_diff)
        else cast(road_goal_diff as string)
    end as road_goal_diff_string,
    roadregulationwins::int as road_reg_wins,
    roadregulationplusotwins::int as road_reg_ot_wins,
    leagueroadsequence::int as league_road_standing,
    divisionroadsequence::int as div_road_standing,
    conferenceroadsequence::int as conf_road_standing,
    streakcode as streak,
    streakcount::int as streak_count,
    concat(streakcode, streakcount) as streak_string,
    l10gamesplayed::int as l10_gp,
    l10wins::int as l10_wins,
    l10losses::int as l10_losses,
    l10otlosses::int as l10_ot_l,
    concat(l10wins, '-', l10losses, '-', l10otlosses) as l10_record,
    l10points::int as l10_points,
    l10goalsfor::int as l10_goals,
    l10goalsagainst::int as l10_goals_against,
    l10goaldifferential::int as l10_goal_diff,
    l10regulationwins::int as l10_reg_wins,
    l10regulationplusotwins::int as l10_reg_ot_wins,
    divisionl10sequence::int as div_l10_standing,
    conferencel10sequence::int as conf_l10_standing
from raw_standings
