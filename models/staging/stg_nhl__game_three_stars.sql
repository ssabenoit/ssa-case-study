-- models/staging/stg_nhl__game_three_stars.sql
-- Three Stars of each game, flattened from the game summary JSON.
-- Grain: one row per (game_id, star_number); games without a published
-- three-stars list contribute no rows.

with

summaries as (
    select *
    from {{ source('nhl_staging_data', 'game_summaries') }}
    qualify row_number() over (partition by ID order by _loaded_at desc) = 1
)

select
    s.ID::int as game_id,
    s.SEASON::string as season,
    s.GAMEDATE::date as game_date,
    star.value:star::int as star_number,
    star.value:playerId::int as player_id,
    star.value:name:default::string as player_name,
    star.value:teamAbbrev::string as team_abv,
    star.value:position::string as position,
    star.value:goals::int as goals,
    star.value:assists::int as assists,
    star.value:points::int as points,
    star.value:goalsAgainstAverage::float as gaa,
    star.value:savePctg::float as save_pct
from summaries s,
    lateral flatten(input => parse_json(s.SUMMARY_THREESTARS)) star
