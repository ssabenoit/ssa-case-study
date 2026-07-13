{{ config(materialized='table') }}

-- models/intermediate/int__game_penalties.sql
-- Team penalty totals per league game, from play-by-play events.
-- Grain: one row per (game_id, team_id) for teams that took >= 1 penalty.
--
-- times_shorthanded approximates the official NHL "times shorthanded" /
-- opponent PP-opportunity count: minors (2), double minors (4), and majors
-- (5) each count once; offsetting penalties (same game clock time, same
-- duration, opposite teams) cancel; misconducts (10) never create a power
-- play but do count toward PIM.

with

penalties as (
    select
        game_id,
        play_team_id as team_id,
        period,
        time_in_period,
        penalty_duration as duration
    from {{ ref('stg_nhl__play_by_play') }}
    where description = 'penalty'
        and penalty_duration is not null
        and penalty_duration > 0
        and game_id in (select game_id from {{ ref('int__league_games') }})
),

-- penalties that can create a power play, grouped so offsetting ones can cancel
pp_creating as (
    select
        game_id,
        team_id,
        period,
        time_in_period,
        duration,
        count(*) as n_penalties
    from penalties
    where duration in (2, 4, 5)
    group by game_id, team_id, period, time_in_period, duration
),

offset_netted as (
    select
        own.game_id,
        own.team_id,
        own.n_penalties - least(own.n_penalties, coalesce(opp.n_penalties, 0)) as net_pp_creating
    from pp_creating own
    left join pp_creating opp
        on opp.game_id = own.game_id
        and opp.period = own.period
        and opp.time_in_period = own.time_in_period
        and opp.duration = own.duration
        and opp.team_id <> own.team_id
),

pim_totals as (
    select
        game_id,
        team_id,
        sum(duration) as pim,
        count(*) as penalties_taken
    from penalties
    group by game_id, team_id
),

shorthanded_totals as (
    select
        game_id,
        team_id,
        sum(net_pp_creating) as times_shorthanded
    from offset_netted
    group by game_id, team_id
)

select
    p.game_id,
    p.team_id,
    p.pim,
    p.penalties_taken,
    coalesce(s.times_shorthanded, 0) as times_shorthanded
from pim_totals p
left join shorthanded_totals s
    on s.game_id = p.game_id
    and s.team_id = p.team_id
