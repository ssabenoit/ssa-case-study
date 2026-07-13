-- models/marts/players_history.sql
-- Every team a player has appeared in a league game for.
-- Sourced from the player-team bridge so goalies are included (the previous
-- version only looked at skater game logs).

select distinct
    player_id,
    team_abv
from {{ ref('dim_player_team_bridge') }}
