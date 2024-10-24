-- models/marts/team_season_stats_playoff.sql
-- aggregates per game data to get playoff season stats data for each team

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
        -- team_id,
        count(*) as games_played,
        sum(goals) as goals,
        round(avg(goals), 2) as goals_per_game,
        round(avg(goals_against), 2) as goals_against_average,
        sum(goals_against) as goals_against,
        sum(shots) as shots,
        round(avg(shots), 2) as shots_per_game,
        -- sum(p1_goals) as p1_goals,
        -- sum(p2_goals) as p2_goals,
        -- sum(p3_goals) as p3_goals,
        -- sum(ot_goals) as ot_goals,
        -- sum(p1_shots) as p1_shots,
        -- round(avg(p1_shots), 2) as p1_shots_per_game,
        -- sum(p2_shots) as p2_shots,
        -- round(avg(p2_shots), 2) as p2_shots_per_game,
        -- sum(p3_shots) as p3_shots,
        -- round(avg(p3_shots), 2) as p3_shots_per_game,
        -- sum(ot_shots) as ot_shots,
        sum(pp_goals) as pp_goals,
        sum(pp_attempts) as pp_attempts,
        sum(pp_goals)/sum(pp_attempts) as pp_pct,
        sum(pim) as pim,
        sum(pk_attempts) as pk_attempts,
        (sum(pk_attempts) - sum(pk_goals_against)) / sum(pk_attempts) as pk_pct,
        round(avg(pim), 2) as pim_per_game,
        sum(hits) as hits,
        round(avg(hits), 2) as hits_per_game,
        sum(blocks) as blocks,
        round(avg(blocks), 2) as blocks_per_game,
        sum(giveaways) as giveaways,
        round(avg(giveaways), 2) as giveaways_per_game,
        sum(takeaways) as takeaways,
        round(avg(takeaways), 2) as takeaways_per_game
    from team_game_stats
    where game_type = 'playoff'
    group by season, team_abv --, team_id
)

select s.*, i.logo_url
from team_stats s left outer join teams_info i
on s.team_abv = i.team_abv