{{ config(materialized='table') }}

-- models/intermediate/int__team_per_game_stats.sql
-- Per-game statistics for each team in every league game (regular + playoff).
-- Grain: one row per (game_id, team_id) — two rows per game.
--
-- The game_summaries source does not carry team counting stats (its
-- summary.teamGameStats is not loaded), so this model assembles them from
-- first-party sources instead:
--   hits/blocks/giveaways/takeaways/pp_goals  -> summed player boxscore lines
--   pim / times shorthanded (pp+pk attempts)  -> play-by-play penalty events
--   faceoff wins & percentage                 -> play-by-play faceoff events
-- Notes: giveaways/takeaways exclude goalie events (not present in the parsed
-- goalie boxscore); times_shorthanded is a close approximation of the
-- official PP-opportunity count (see int__game_penalties).

with

summaries as (
    select *
    from {{ ref('stg_nhl__game_summaries') }}
),

league_games as (
    select *
    from {{ ref('int__league_games') }}
),

skaters as (
    select *
    from {{ ref('int__skaters_per_game_stats') }}
),

goalies as (
    select *
    from {{ ref('int__goalies_per_game_stats') }}
),

penalties as (
    select *
    from {{ ref('int__game_penalties') }}
),

faceoffs as (
    select *
    from {{ ref('int__game_faceoffs') }}
),

base as (
    select *
    from summaries
    where game_id in (select game_id from league_games)
),

skater_sums as (
    select
        game_id,
        team_abv,
        sum(hits) as hits,
        sum(blocks) as blocks,
        sum(giveaways) as giveaways,
        sum(takeaways) as takeaways,
        sum(pp_goals) as pp_goals
    from skaters
    group by game_id, team_abv
),

goalie_sums as (
    select
        game_id,
        team_abv,
        sum(goals_against) as goals_against,
        sum(shots_saved) as saves,
        sum(shots_against) as shots_against,
        sum(shots_saved)::float / nullif(sum(shots_against), 0) as save_pct,
        sum(pim) as goalie_pim,
        sum(pp_shots_against)::int as pp_shots_against,
        sum(pp_shots_saved)::int as pp_saves
    from goalies
    group by game_id, team_abv
)

select
    b.season,
    b.game_id,
    b.team_abv,
    b.team_id,
    b.type,
    b.game_type,
    b.shots,
    b.goals,
    b.goals_against,
    b.sog,
    fo.faceoffs_won::float / nullif(fo.faceoffs_in_game, 0) as faceoff_pct,
    coalesce(own_sk.pp_goals, 0) || '/' || coalesce(opp_pen.times_shorthanded, 0) as power_play,
    coalesce(own_sk.pp_goals, 0) as pp_goals,
    coalesce(opp_pen.times_shorthanded, 0) as pp_attempts,
    coalesce(own_pen.times_shorthanded, 0) - coalesce(opp_sk.pp_goals, 0)
        || '/' || coalesce(own_pen.times_shorthanded, 0) as penalty_kill,
    coalesce(opp_sk.pp_goals, 0) as pk_goals_against,
    coalesce(own_pen.times_shorthanded, 0) as pk_attempts,
    coalesce(own_sk.pp_goals, 0)::float / nullif(opp_pen.times_shorthanded, 0) as pp_pct,
    coalesce(own_pen.pim, 0) as pim,
    own_sk.hits,
    own_sk.blocks,
    own_sk.giveaways,
    own_sk.takeaways,
    -- goaltending rollup for the team's own net
    g.goals_against as goals_against_g,
    g.saves,
    g.shots_against,
    g.save_pct,
    g.pp_shots_against,
    g.pp_saves,
    -- game metadata + faceoff counts (new columns, appended last)
    b.game_date,
    b.start_time_utc,
    b.venue,
    b.last_period_type,
    fo.faceoffs_won,
    fo.faceoffs_in_game as faceoffs_taken,
    (coalesce(own_pen.times_shorthanded, 0) - coalesce(opp_sk.pp_goals, 0))::float
        / nullif(own_pen.times_shorthanded, 0) as pk_pct,
    coalesce(own_pen.penalties_taken, 0) as penalties_taken
from base b
left join skater_sums own_sk
    on own_sk.game_id = b.game_id
    and own_sk.team_abv = b.team_abv
left join skater_sums opp_sk
    on opp_sk.game_id = b.game_id
    and opp_sk.team_abv <> b.team_abv
left join penalties own_pen
    on own_pen.game_id = b.game_id
    and own_pen.team_id = b.team_id
left join penalties opp_pen
    on opp_pen.game_id = b.game_id
    and opp_pen.team_id <> b.team_id
left join faceoffs fo
    on fo.game_id = b.game_id
    and fo.team_id = b.team_id
left join goalie_sums g
    on g.game_id = b.game_id
    and g.team_abv = b.team_abv
