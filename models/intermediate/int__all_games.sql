{{ config(materialized='table') }}

-- models/intermediate/int__all_games.sql
-- Maintains basic information table for all games in the dataset

with

summaries as (
    select
        ID::int as id,
        SEASON::int as season,
        AWAYTEAM_ID::int as away_id,
        AWAYTEAM_ABBREV::string as away_abv,
        AWAYTEAM_SCORE::int as away_score,
        HOMETEAM_ID::int as home_id,
        HOMETEAM_ABBREV::string as home_abv,
        HOMETEAM_SCORE::int as home_score
    from {{ source('nhl_staging_data', 'game_summaries') }}
),

games as (
    select
        ID::int as id,
        GAMEDATE::date as date,
        VENUE_DEFAULT::string as venue,
        NEUTRALSITE::boolean as neutral_site,
        GAMEOUTCOME_LASTPERIODTYPE::string as game_outcome
    from {{ ref("stg_nhl__games") }}
)

select
    s.*,
    g.date,
    g.venue,
    g.neutral_site,
    g.game_outcome
from summaries s
left join games g
    on g.id = s.id
