{{ config(materialized='incremental', unique_key='game_id', incremental_strategy='delete+insert') }}

-- models/intermediate/int__player_game_shifts.sql
-- Per player-game shift summary from the shift-chart feed: real shift counts
-- and ice time measured from actual shifts (independent of the boxscore TOI).
-- Grain: one row per (game_id, player_id).

with

shifts as (
    select *
    from {{ ref('stg_nhl__shift_charts') }}
    {% if is_incremental() %}
    where _loaded_at > (select coalesce(max(loaded_at), '1900-01-01') from {{ this }})
    {% endif %}
)

select
    game_id,
    player_id,
    team_id,
    team_abv,
    count(*) as shifts,
    sum(duration_seconds) as toi_seconds,
    min(period) as first_period,
    max(period) as last_period,
    max(case when period >= 4 then true else false end) as played_overtime,
    max(_loaded_at) as loaded_at
from shifts
where game_id in (select game_id from {{ ref('int__league_games') }})
group by game_id, player_id, team_id, team_abv
