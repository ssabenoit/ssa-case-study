{{ config(materialized='table') }}

-- models/intermediate/int__game_faceoffs.sql
-- Faceoff wins per team per league game, counted from play-by-play events
-- (the event owner of a faceoff is the winning team).
-- Grain: one row per (game_id, team_id).

with

faceoffs as (
    select
        game_id,
        play_team_id as team_id
    from {{ ref('stg_nhl__play_by_play') }}
    where description = 'faceoff'
        and play_team_id is not null
        and game_id in (select game_id from {{ ref('int__league_games') }})
)

select
    game_id,
    team_id,
    count(*) as faceoffs_won,
    sum(count(*)) over (partition by game_id) as faceoffs_in_game
from faceoffs
group by game_id, team_id
