-- models/marts/current_statwise_rankings.sql
-- updated team rankings of every team in every stat category

{% set columns = ['goals_per_game_rank', 'shots_per_game_rank', 'pp_goals_rank', 'pim_per_game_rank', 
'hits_per_game_rank', 'goals_against_average_rank', 'takeaways_per_game_rank', 'giveaways_per_game_rank', 
'blocks_per_game_rank'] %}

/*
, 'p1_shots_per_game_rank''p2_shots_per_game_rank', 'p3_shots_per_game_rank', 
'p1_goals_rank', 'p2_goals_rank', 'p3_goals_rank', 'ot_goals_rank', 'ot_shots_rank', 'pp_attempts_rank'
, 'takeaways_per_game_rank', 'giveaways_per_game_rank', 'blocks_per_game_rank'
*/

with season_stats as (
    select *
    from {{ ref('team_season_stats_regular') }}
),

ranked_stats as (
    select
        -- team_abv,
        -- season,
        ROW_NUMBER() over(partition by season order by goals_per_game desc) as goals_per_game_rank,
        ROW_NUMBER() over(partition by season order by shots_per_game desc) as shots_per_game_rank,
        ROW_NUMBER() over(partition by season order by goals_against_average asc) as goals_against_average_rank,
        -- ROW_NUMBER() over(order by p1_shots_per_game desc) as p1_shots_per_game_rank,
        -- ROW_NUMBER() over(order by p2_shots_per_game desc) as p2_shots_per_game_rank,
        -- ROW_NUMBER() over(order by p3_shots_per_game desc) as p3_shots_per_game_rank,
        -- ROW_NUMBER() over(order by p1_goals desc) as p1_goals_rank,
        -- ROW_NUMBER() over(order by p2_goals desc) as p2_goals_rank,
        -- ROW_NUMBER() over(order by p3_goals desc) as p3_goals_rank,
        -- ROW_NUMBER() over(order by ot_goals desc) as ot_goals_rank,
        -- ROW_NUMBER() over(order by ot_shots desc) as ot_shots_rank,
        ROW_NUMBER() over(partition by season order by pp_goals desc) as pp_goals_rank,
        -- ROW_NUMBER() over(order by pp_attempts desc) as pp_attempts_rank,
        -- ROW_NUMBER() over(order by pp_pct desc) as pp_pct_rank,
        ROW_NUMBER() over(partition by season order by pim_per_game desc) as pim_per_game_rank,
        ROW_NUMBER() over(partition by season order by hits_per_game desc) as hits_per_game_rank,
        ROW_NUMBER() over(order by blocks_per_game desc) as blocks_per_game_rank,
        ROW_NUMBER() over(order by takeaways_per_game desc) as takeaways_per_game_rank,
        ROW_NUMBER() over(order by giveaways_per_game desc) as giveaways_per_game_rank,
        *
    from season_stats
)

-- formatting ranks to be ordinal (with number suffix)
SELECT
  {% for column_name in columns %}
    CASE 
      WHEN ({{ column_name }} % 100) IN (11, 12, 13) THEN CONCAT({{ column_name }}, 'th')
      WHEN ({{ column_name }} % 10) = 1 THEN CONCAT({{ column_name }}, 'st')
      WHEN ({{ column_name }} % 10) = 2 THEN CONCAT({{ column_name }}, 'nd')
      WHEN ({{ column_name }} % 10) = 3 THEN CONCAT({{ column_name }}, 'rd')
      ELSE CONCAT({{ column_name }}, 'th')
    END AS {{ column_name }}_ordinal,
  {% endfor %}
  *
FROM ranked_stats