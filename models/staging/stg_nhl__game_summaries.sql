-- models/staging/stg_nhl__game_summaries.sql
-- pulls each team information and their high level stat summary (mainly PP/PK) for each game

with summaries as (
    select *
    from {{ source('nhl_staging_data', 'game_summaries') }}
),

away_team_stats as (
    select
        season::STRING as season,
        id::INT as game_id,
        awayteam:abbrev::STRING as team_abv,
        awayteam:id::INT as team_id,
        'away' as type,
        case 
            when gametype = 2 then 'regular'
            when gametype = 3 then 'playoff'
            else 'other'
        end as game_type,
        awayteam:sog::INT as shots,
        awayteam:score::INT as goals,
        hometeam:score::INT as goals_against,
        -- summary:linescore:byPeriod[0]:away::INT as p1_goals,
        -- summary:linescore:byPeriod[1]:away::INT as p2_goals,
        -- summary:linescore:byPeriod[2]:away::INT as p3_goals,
        -- goals - p1_goals - p2_goals - p3_goals as ot_goals,
        -- summary:shotsByPeriod[0]:away::INT as p1_shots,
        -- summary:shotsByPeriod[1]:away::INT as p2_shots,
        -- summary:shotsByPeriod[2]:away::INT as p3_shots,
        -- shots - p1_shots - p2_shots - p3_shots as ot_shots,
        summary:teamGameStats[0]:awayValue::INT as sog,
        summary:teamGameStats[1]:awayValue::FLOAT as faceoff_pct,

        summary:teamGameStats[2]:awayValue::STRING as power_play,
        CAST(split_part(power_play, '/', 0) as INT) as pp_goals,
        CAST(split_part(power_play, '/', -1) as INT) as pp_attempts,

        summary:teamGameStats[2]:homeValue::STRING as penalty_kill,
        CAST(split_part(penalty_kill, '/', 0) as INT) as pk_goals_against,
        CAST(split_part(penalty_kill, '/', -1) as INT) as pk_attempts,

        summary:teamGameStats[3]:awayValue::FLOAT as pp_pct,
        summary:teamGameStats[4]:awayValue::INT as pim,
        summary:teamGameStats[5]:awayValue::INT as hits,
        summary:teamGameStats[6]:awayValue::INT as blocked_shots,
        summary:teamGameStats[7]:awayValue::INT as giveaways,
        summary:teamGameStats[8]:awayValue::INT as takeaways,

    from summaries
),

home_team_stats as (
    select
        season::STRING as season,
        id::INT as game_id,
        hometeam:abbrev::STRING as team_abv,
        hometeam:id::INT as team_id,
        'home' as type,
        case 
            when gametype = 2 then 'regular'
            when gametype = 3 then 'playoff'
            else 'other'
        end as game_type,
        hometeam:sog::INT as shots,
        hometeam:score::INT as goals,
        awayteam:score::INT as goals_against,
        -- summary:linescore:byPeriod[0]:home::INT as p1_goals,
        -- summary:linescore:byPeriod[1]:home::INT as p2_goals,
        -- summary:linescore:byPeriod[2]:home::INT as p3_goals,
        -- goals - p1_goals - p2_goals - p3_goals as ot_goals,
        -- summary:shotsByPeriod[0]:home::INT as p1_shots,
        -- summary:shotsByPeriod[1]:home::INT as p2_shots,
        -- summary:shotsByPeriod[2]:home::INT as p3_shots,
        -- shots - p1_shots - p2_shots - p3_shots as ot_shots,

        summary:teamGameStats[0]:homeValue::INT as sog,
        summary:teamGameStats[1]:homeValue::FLOAT as faceoff_pct,
        summary:teamGameStats[2]:homeValue::STRING as power_play,
        CAST(split_part(power_play, '/', 0) as INT) as pp_goals,
        CAST(split_part(power_play, '/', -1) as INT) as pp_attempts,

        summary:teamGameStats[2]:awayValue::STRING as penalty_kill,
        CAST(split_part(penalty_kill, '/', 0) as INT) as pk_goals_against,
        CAST(split_part(penalty_kill, '/', -1) as INT) as pk_attempts,

        summary:teamGameStats[3]:homeValue::FLOAT as pp_pct,
        summary:teamGameStats[4]:homeValue::INT as pim,
        summary:teamGameStats[5]:homeValue::INT as hits,
        summary:teamGameStats[6]:homeValue::INT as blocks,
        summary:teamGameStats[7]:homeValue::INT as giveaways,
        summary:teamGameStats[8]:homeValue::INT as takeaways,
    from summaries
)

select * 
from home_team_stats
union all
select * 
from away_team_stats