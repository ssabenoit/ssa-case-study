-- models/intermediate/int__current_standings.sql
-- formatting the current standings
{{ config(materialized='table')}}

with raw_standings as (
    select *
    from {{ ref("stg_nhl__current_standings") }}
)

select
    -- team info
    teamname:default::STRING as team,
    teamabbrev:default::STRING as team_abv,
    divisionname as division,
    divisionabbrev as division_abv,
    conferencename as conference,
    conferenceabbrev as conference_abv,
    leaguesequence::INT as league_standing,
    conferencesequence::INT as conference_standing,
    divisionsequence::INT as division_standing,
    wildcardsequence::INT as wildcard_standing,
    -- clinchindicator as clinch_indicator,
    waiverssequence::INT as waiver,

    -- basic total stats
    points::INT as points,
    wins::INT as wins,
    losses::INT as losses,
    otlosses::INT as ot_losses,
    gamesplayed::INT as games_played,
    concat(wins, '-', losses, '-', otlosses) as record,
    winpctg as win_pct,
    pointpctg as point_pct,

    regulationwins::INT as reg_wins,
    regulationplusotwins::INT as reg_ot_wins,
    regulationplusotwinpctg as reg_ot_win_pct,
    regulationwinpctg as reg_win_pct,
    shootoutwins::INT as shootout_w,
    shootoutlosses::INT as shootout_l,
    concat(shootoutwins, '-', shootoutlosses) as shootout_record,

    goalfor::INT as goals_for,
    goalagainst::INT as goals_against,
    goaldifferential::INT as goal_diff,
    case
        when goal_diff > 0 then concat('+', goal_diff)
        else CAST(goal_diff as STRING)
    end as goal_diff_string,
    goalsforpctg as goals_per_game,
    goaldifferentialpctg as goal_diff_pct,
    
    -- home and away stats
    homewins::INT as home_wins,
    homelosses::INT as home_losses,
    homeotlosses::INT as home_ot_l,
    homepoints::INT as home_points,
    concat(homewins, '-', homelosses, '-', homeotlosses) as home_record,
    homegamesplayed::INT as home_gp,
    homegoalsfor::INT as home_goals,
    homegoalsagainst::INT as home_goals_against,
    homegoaldifferential::INT as home_goal_diff,
    case
        when home_goal_diff > 0 then concat('+', home_goal_diff)
        else CAST(home_goal_diff as STRING)
    end as home_goal_diff_string,
    homeregulationwins::INT as home_reg_wins,
    homeregulationplusotwins::INT as home_reg_ot_wins,
    leaguehomesequence::INT as league_home_standing,
    divisionhomesequence::INT as div_home_standing,
    conferencehomesequence::INT as conf_home_standing,

    roadwins::INT as road_wins,
    roadlosses::INT as road_losses,
    roadotlosses::INT as road_ot_l,
    roadpoints::INT as road_points,
    concat(roadwins, '-', roadlosses, '-', roadotlosses) as road_record,
    roadgamesplayed::INT as road_gp,
    roadgoalsfor::INT as road_goals,
    roadgoalsagainst::INT as road_goals_against,
    roadgoaldifferential::INT as road_goal_diff,
    case
        when road_goal_diff > 0 then concat('+', road_goal_diff)
        else CAST(road_goal_diff as STRING)
    end as road_goal_diff_string,
    roadregulationwins::INT as road_reg_wins,
    roadregulationplusotwins::INT as road_reg_ot_wins,
    leagueroadsequence::INT as league_road_standing,
    divisionroadsequence::INT as div_road_standing,
    conferenceroadsequence::INT as conf_road_standing,

    -- recent (L10) game stats
    streakcode as streak,
    streakcount::INT as streak_count,
    concat(streakcode, streakcount) as streak_string,
    l10gamesplayed::INT as l10_gp,
    l10wins::INT as l10_wins,
    l10losses::INT as l10_losses,
    l10otlosses::INT as l10_ot_l,
    concat(l10wins, '-', l10losses, '-', l10otlosses) as l10_record,
    l10points::INT as l10_points,
    l10goalsfor::INT as l10_goals,
    l10goalsagainst::INT as l10_goals_against,
    l10goaldifferential::INT as l10_goal_diff,
    l10regulationwins::INT as l10_reg_wins,
    l10regulationplusotwins::INT as l10_reg_ot_wins,
    divisionl10sequence::INT as div_l10_standing,
    conferencel10sequence::INT as conf_l10_standing

from raw_standings