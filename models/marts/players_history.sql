-- models/marts/players_history.sql
-- compile history of all teams that a player has played for

with skaters as (
    select *
    from {{ ref('skaters_per_game_stats_all') }}
)

select distinct player_id, team_abv
from skaters  