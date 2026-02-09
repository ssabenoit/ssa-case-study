-- models/staging/stg_nhl__game_summaries.sql
-- Pulls team information and high-level stat summary for each game
-- Note: teamGameStats (PP/PK/hits/blocks/etc.) not available in flattened format.
-- Those fields will populate once summary_teamGameStats column is loaded.

with

summaries as (
    select *
    from {{ source('nhl_staging_data', 'game_summaries') }}
),

away_team_stats as (
    select
        SEASON::string as season,
        ID::int as game_id,
        AWAYTEAM_ABBREV::string as team_abv,
        AWAYTEAM_ID::int as team_id,
        'away' as type,
        case
            when GAMETYPE = 1 then 'preseason'
            when GAMETYPE = 2 then 'regular'
            when GAMETYPE = 3 then 'playoff'
            else 'other'
        end as game_type,
        AWAYTEAM_SOG::int as shots,
        AWAYTEAM_SCORE::int as goals,
        HOMETEAM_SCORE::int as goals_against,
        AWAYTEAM_SOG::int as sog,
        null::float as faceoff_pct,
        null::string as power_play,
        null::int as pp_goals,
        null::int as pp_attempts,
        null::string as penalty_kill,
        null::int as pk_goals_against,
        null::int as pk_attempts,
        null::float as pp_pct,
        null::int as pim,
        null::int as hits,
        null::int as blocks,
        null::int as giveaways,
        null::int as takeaways
    from summaries
),

home_team_stats as (
    select
        SEASON::string as season,
        ID::int as game_id,
        HOMETEAM_ABBREV::string as team_abv,
        HOMETEAM_ID::int as team_id,
        'home' as type,
        case
            when GAMETYPE = 1 then 'preseason'
            when GAMETYPE = 2 then 'regular'
            when GAMETYPE = 3 then 'playoff'
            else 'other'
        end as game_type,
        HOMETEAM_SOG::int as shots,
        HOMETEAM_SCORE::int as goals,
        AWAYTEAM_SCORE::int as goals_against,
        HOMETEAM_SOG::int as sog,
        null::float as faceoff_pct,
        null::string as power_play,
        null::int as pp_goals,
        null::int as pp_attempts,
        null::string as penalty_kill,
        null::int as pk_goals_against,
        null::int as pk_attempts,
        null::float as pp_pct,
        null::int as pim,
        null::int as hits,
        null::int as blocks,
        null::int as giveaways,
        null::int as takeaways
    from summaries
)

select *
from (
    select *
    from home_team_stats
    union all
    select *
    from away_team_stats
)
qualify row_number() over (partition by game_id, team_id order by game_id) = 1
