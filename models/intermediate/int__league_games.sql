{{ config(materialized='table') }}

-- models/intermediate/int__league_games.sql
-- Canonical universe of NHL league games: regular-season and playoff games
-- played between two NHL franchises. All-Star, 4 Nations, and other
-- special-event games are excluded here. This is the single decontamination
-- choke point every downstream stats model filters through.

with

games as (
    select *
    from {{ ref('stg_nhl__games') }}
),

-- Teams that appear in the standings are, by definition, NHL franchises for
-- that season. Deriving the list per season (rather than from current_teams)
-- keeps relocations honest, e.g. ARI in 2023-24 vs UTA in 2025-26.
league_teams as (
    select distinct
        SEASONID::int as season,
        TEAMABBREV_DEFAULT::string as team_abv
    from {{ ref('stg_nhl__daily_standings') }}
)

select
    g.ID::int as game_id,
    g.SEASON::int as season,
    case
        when g.GAMETYPE = 2 then 'regular'
        when g.GAMETYPE = 3 then 'playoff'
    end as game_type,
    g.GAMEDATE::date as game_date,
    g.STARTTIMEUTC::string as start_time_utc,
    g.GAMESTATE::string as game_state,
    g.GAMEOUTCOME_LASTPERIODTYPE::string as last_period_type,
    g.HOMETEAM_ID::int as home_team_id,
    g.HOMETEAM_ABBREV::string as home_team_abv,
    g.AWAYTEAM_ID::int as away_team_id,
    g.AWAYTEAM_ABBREV::string as away_team_abv
from games g
inner join league_teams home_franchise
    on home_franchise.season = g.SEASON
    and home_franchise.team_abv = g.HOMETEAM_ABBREV
inner join league_teams away_franchise
    on away_franchise.season = g.SEASON
    and away_franchise.team_abv = g.AWAYTEAM_ABBREV
where g.GAMETYPE in (2, 3)
