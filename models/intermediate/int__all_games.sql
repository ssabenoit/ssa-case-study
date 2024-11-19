-- models/intermediate/int__all_games.sql
-- maintains basic information table for all games in the data set
{{ config(materialized='table')}}


with summaries as (
    select
        id::int as id,
        season::int as season,
        awayteam:id::int as away_id,
        awayteam:abbrev::string as away_abv,
        awayteam:score::int as away_score,
        hometeam:id::int as home_id,
        hometeam:abbrev::string as home_abv,
        hometeam:score::int as home_score
    from {{ source('nhl_staging_data', 'game_summaries') }}
),

games as (
    select
        id::int as id,
        date,
        venue:default::string as venue,
        neutralsite::boolean as neutral_site,
    from {{ ref("stg_nhl__games") }}  
),

outcomes as (
    select
        id::int as id,
        gameoutcome:lastPeriodType::string as game_outcome
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
