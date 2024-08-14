-- models/marts/team_season_stats_regular.sql
-- aggregates per game data to get regular season stats data for each team

with team_game_stats as (
    select *
    from {{ ref("team_per_game_stats_all") }}
),

teams_info as (
    select *
    from {{ ref("int__teams_basic_info") }}
),

team_stats as (
    select
        season,
        team_abv,
        team_id,
        count(*) as games_played,
        sum(goals) as goals,
        avg(goals) as goals_per_game,
        sum(goals_against) as goals_against,
        avg(goals_against) as goals_against_average,
        sum(shots) as shots,
        avg(shots) as shots_per_game,
        sum(p1_goals) as p1_goals,
        sum(p2_goals) as p2_goals,
        sum(p3_goals) as p3_goals,
        sum(ot_goals) as ot_goals,
        sum(p1_shots) as p1_shots,
        avg(p1_shots) as p1_shots_per_game,
        sum(p2_shots) as p2_shots,
        avg(p2_shots) as p2_shots_per_game,
        sum(p3_shots) as p3_shots,
        avg(p3_shots) as p3_shots_per_game,
        sum(ot_shots) as ot_shots,
        sum(pp_goals) as pp_goals,
        sum(pp_attempts) as pp_attempts,
        sum(pp_goals)/sum(pp_attempts) as pp_pct,
        sum(pim) as pim,
        avg(pim) as pim_per_game,
        sum(hits) as hits,
        avg(hits) as hits_per_game,
        sum(blocked_shots) as blocks,
        avg(blocked_shots) as blocks_per_game,
        sum(giveaways) as giveaways,
        avg(giveaways) as giveaways_per_game,
        sum(takeaways) as takeaways,
        avg(takeaways) as takeaways_per_game
    from team_game_stats
    where game_type = 'regular'
    group by season, team_abv, team_id
)

select s.*, i.logo_url
from team_stats s left outer join teams_info i
on s.team_abv = i.team_abv
