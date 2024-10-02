-- models/marts/current_statwise_rankings.sql
-- updated team rankings of every team in every stat category

with season_stats as (
    select *
    from {{ ref('team_season_stats_regular') }}
)

select
    ROW_NUMBER() over(order by goals_per_game desc) as goals_per_game_rank,
    ROW_NUMBER() over(order by shots_per_game desc) as shots_per_game_rank,
    ROW_NUMBER() over(order by p1_shots_per_game desc) as p1_shots_per_game_rank,
    ROW_NUMBER() over(order by p2_shots_per_game desc) as p2_shots_per_game_rank,
    ROW_NUMBER() over(order by p3_shots_per_game desc) as p3_shots_per_game_rank,
    ROW_NUMBER() over(order by p1_goals desc) as p1_goals_rank,
    ROW_NUMBER() over(order by p2_goals desc) as p2_goals_rank,
    ROW_NUMBER() over(order by p3_goals desc) as p3_goals_rank,
    ROW_NUMBER() over(order by ot_goals desc) as ot_goals_rank,
    ROW_NUMBER() over(order by ot_shots desc) as ot_shots_rank,
    ROW_NUMBER() over(order by pp_goals desc) as pp_goals_rank,
    ROW_NUMBER() over(order by pp_attempts desc) as pp_attempts_rank,
    ROW_NUMBER() over(order by pp_pct desc) as pp_pct_rank,
    ROW_NUMBER() over(order by pim_per_game desc) as pim_per_game_rank,
    ROW_NUMBER() over(order by hits_per_game desc) as hits_per_game_rank,
    ROW_NUMBER() over(order by blocks_per_game desc) as blocks_per_game_rank,
    ROW_NUMBER() over(order by takeaways_per_game desc) as takeaways_per_game_rank,
    ROW_NUMBER() over(order by giveaways_per_game desc) as giveaways_per_game_rank,
    *
from season_stats