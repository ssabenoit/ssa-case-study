-- models/marts/current_statwise_rankings.sql
-- Team rankings in every stat category with ordinal formatting

{% set columns = [
    'goals_per_game_rank', 
    'shots_per_game_rank', 
    'pp_goals_rank', 
    'pim_per_game_rank', 
    'hits_per_game_rank', 
    'goals_against_average_rank', 
    'takeaways_per_game_rank', 
    'giveaways_per_game_rank', 
    'blocks_per_game_rank', 
    'pp_pct_rank', 
    'pk_pct_rank'
] %}

with

season_stats as (
    select *
    from {{ ref('team_season_stats_regular') }}
),

ranked_stats as (
    select
        row_number() over(partition by season order by goals_per_game desc) as goals_per_game_rank,
        row_number() over(partition by season order by shots_per_game desc) as shots_per_game_rank,
        row_number() over(partition by season order by goals_against_average asc) as goals_against_average_rank,
        row_number() over(partition by season order by pp_goals desc) as pp_goals_rank,
        row_number() over(partition by season order by pp_pct desc) as pp_pct_rank,
        row_number() over(partition by season order by pk_pct desc) as pk_pct_rank,
        row_number() over(partition by season order by pim_per_game asc) as pim_per_game_rank,
        row_number() over(partition by season order by hits_per_game desc) as hits_per_game_rank,
        row_number() over(partition by season order by blocks_per_game desc) as blocks_per_game_rank,
        row_number() over(partition by season order by takeaways_per_game desc) as takeaways_per_game_rank,
        row_number() over(partition by season order by giveaways_per_game asc) as giveaways_per_game_rank,
        *
    from season_stats
)

select
  {% for column_name in columns %}
    case 
      when ({{ column_name }} % 100) in (11, 12, 13) then concat({{ column_name }}, 'th')
      when ({{ column_name }} % 10) = 1 then concat({{ column_name }}, 'st')
      when ({{ column_name }} % 10) = 2 then concat({{ column_name }}, 'nd')
      when ({{ column_name }} % 10) = 3 then concat({{ column_name }}, 'rd')
      else concat({{ column_name }}, 'th')
    end as {{ column_name }}_ordinal,
  {% endfor %}
  *
from ranked_stats
