-- models/staging/stg_nhl__game_summaries.sql
-- Pulls team information and high-level stat summary (mainly PP/PK) for each game

with

summaries as (
    select *
    from {{ source('nhl_staging_data', 'game_summaries') }}
),

away_team_stats as (
    select
        season::string as season,
        id::int as game_id,
        awayteam:abbrev::string as team_abv,
        awayteam:id::int as team_id,
        'away' as type,
        case 
            when gametype = 1 then 'preseason'
            when gametype = 2 then 'regular'
            when gametype = 3 then 'playoff'
            else 'other'
        end as game_type,
        awayteam:sog::int as shots,
        awayteam:score::int as goals,
        hometeam:score::int as goals_against,
        summary:teamGameStats[0]:awayValue::int as sog,
        summary:teamGameStats[1]:awayValue::float as faceoff_pct,
        summary:teamGameStats[2]:awayValue::string as power_play,
        cast(split_part(power_play, '/', 0) as int) as pp_goals,
        cast(split_part(power_play, '/', -1) as int) as pp_attempts,
        summary:teamGameStats[2]:homeValue::string as penalty_kill,
        cast(split_part(penalty_kill, '/', 0) as int) as pk_goals_against,
        cast(split_part(penalty_kill, '/', -1) as int) as pk_attempts,
        summary:teamGameStats[3]:awayValue::float as pp_pct,
        summary:teamGameStats[4]:awayValue::int as pim,
        summary:teamGameStats[5]:awayValue::int as hits,
        summary:teamGameStats[6]:awayValue::int as blocked_shots,
        summary:teamGameStats[7]:awayValue::int as giveaways,
        summary:teamGameStats[8]:awayValue::int as takeaways
    from summaries
),

home_team_stats as (
    select
        season::string as season,
        id::int as game_id,
        hometeam:abbrev::string as team_abv,
        hometeam:id::int as team_id,
        'home' as type,
        case 
            when gametype = 1 then 'preseason'
            when gametype = 2 then 'regular'
            when gametype = 3 then 'playoff'
            else 'other'
        end as game_type,
        hometeam:sog::int as shots,
        hometeam:score::int as goals,
        awayteam:score::int as goals_against,
        summary:teamGameStats[0]:homeValue::int as sog,
        summary:teamGameStats[1]:homeValue::float as faceoff_pct,
        summary:teamGameStats[2]:homeValue::string as power_play,
        cast(split_part(power_play, '/', 0) as int) as pp_goals,
        cast(split_part(power_play, '/', -1) as int) as pp_attempts,
        summary:teamGameStats[2]:awayValue::string as penalty_kill,
        cast(split_part(penalty_kill, '/', 0) as int) as pk_goals_against,
        cast(split_part(penalty_kill, '/', -1) as int) as pk_attempts,
        summary:teamGameStats[3]:homeValue::float as pp_pct,
        summary:teamGameStats[4]:homeValue::int as pim,
        summary:teamGameStats[5]:homeValue::int as hits,
        summary:teamGameStats[6]:homeValue::int as blocks,
        summary:teamGameStats[7]:homeValue::int as giveaways,
        summary:teamGameStats[8]:homeValue::int as takeaways
    from summaries
)

select * 
from home_team_stats
union all
select * 
from away_team_stats
