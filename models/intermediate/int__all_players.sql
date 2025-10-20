{{ config(materialized='table') }}

-- models/intermediate/int__all_players.sql
-- Master table containing all players (forwards, defensemen, and goalies) for every team

with

skaters as (
    select *
    from {{ ref('int__all_skaters') }}
),

goalies as (
    select *
    from {{ ref('int__all_goalies') }}
)

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
    birthdate as birth_date,
    birth_city,
    birth_state,
    birth_country,
    headshot_url
from skaters

union all

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
    birthdate as birth_date,
    birth_city,
    birth_state,
    birth_country,
    headshot_url
from goalies