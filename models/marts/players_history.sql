-- models/marts/players_history.sql
-- Compiles history of all teams that a player has played for

with

skaters as (
    select *
    from {{ ref('int__skaters_per_game_stats') }}
)

select distinct 
    player_id, 
    team_abv
from skaters
