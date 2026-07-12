{{ config(materialized='table') }}

-- models/marts/all_players.sql
-- Master table containing all rostered players for every team.
-- Thin presentation rename over int__all_players (the union of skaters and
-- goalies already lives there — no need to rebuild it).

select
    team_abv,
    player_id,
    first_name,
    last_name,
    position,
    number,
    height,
    weight,
    shoots,
    birth_date,
    birth_city,
    birth_state,
    birth_country,
    headshot_url
from {{ ref('int__all_players') }}
