{{ config(materialized='table') }}

-- models/intermediate/int__all_games.sql
-- Maintains basic information table for all games in the dataset

with

summaries as (
    select
        "id"::int as id,
        "season"::int as season,
        parse_json("awayTeam"):id::int as away_id,
        parse_json("awayTeam"):abbrev::string as away_abv,
        parse_json("awayTeam"):score::int as away_score,
        parse_json("homeTeam"):id::int as home_id,
        parse_json("homeTeam"):abbrev::string as home_abv,
        parse_json("homeTeam"):score::int as home_score
    from {{ source('nhl_staging_data', 'game_summaries') }}
),

games as (
    select
        "id"::int as id,
        "gameDate" as date,
        parse_json("venue"):default::string as venue,
        "neutralSite"::boolean as neutral_site
    from {{ ref("stg_nhl__games") }}
),

outcomes as (
    select
        "id"::int as id,
        parse_json("gameOutcome"):lastPeriodType::string as game_outcome
    from {{ source('nhl_staging_data', 'play_by_play') }}
)

select
    s.*,
    g.date,
    g.venue,
    g.neutral_site,
    o.game_outcome
from summaries s
left join games g
    on g.id = s.id
left join outcomes o
    on g.id = o.id
