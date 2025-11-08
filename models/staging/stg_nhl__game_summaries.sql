-- models/staging/stg_nhl__game_summaries.sql
-- Pulls team information and high-level stat summary (mainly PP/PK) for each game

with

summaries as (
    select
        *,
        parse_json("awayTeam") as awayTeam_json,
        parse_json("homeTeam") as homeTeam_json,
        parse_json("summary") as summary_json
    from {{ source('nhl_staging_data', 'game_summaries') }}
),

away_team_stats as (
    select
        "season"::string as season,
        "id"::int as game_id,
        awayTeam_json:abbrev::string as team_abv,
        awayTeam_json:id::int as team_id,
        'away' as type,
        case
            when "gameType" = 1 then 'preseason'
            when "gameType" = 2 then 'regular'
            when "gameType" = 3 then 'playoff'
            else 'other'
        end as game_type,
        awayTeam_json:sog::int as shots,
        awayTeam_json:score::int as goals,
        homeTeam_json:score::int as goals_against,
        summary_json:teamGameStats[0]:awayValue::int as sog,
        summary_json:teamGameStats[1]:awayValue::float as faceoff_pct,
        summary_json:teamGameStats[2]:awayValue::string as power_play,
        cast(split_part(power_play, '/', 0) as int) as pp_goals,
        cast(split_part(power_play, '/', -1) as int) as pp_attempts,
        summary_json:teamGameStats[2]:homeValue::string as penalty_kill,
        cast(split_part(penalty_kill, '/', 0) as int) as pk_goals_against,
        cast(split_part(penalty_kill, '/', -1) as int) as pk_attempts,
        summary_json:teamGameStats[3]:awayValue::float as pp_pct,
        summary_json:teamGameStats[4]:awayValue::int as pim,
        summary_json:teamGameStats[5]:awayValue::int as hits,
        summary_json:teamGameStats[6]:awayValue::int as blocked_shots,
        summary_json:teamGameStats[7]:awayValue::int as giveaways,
        summary_json:teamGameStats[8]:awayValue::int as takeaways
    from summaries
),

home_team_stats as (
    select
        "season"::string as season,
        "id"::int as game_id,
        homeTeam_json:abbrev::string as team_abv,
        homeTeam_json:id::int as team_id,
        'home' as type,
        case
            when "gameType" = 1 then 'preseason'
            when "gameType" = 2 then 'regular'
            when "gameType" = 3 then 'playoff'
            else 'other'
        end as game_type,
        homeTeam_json:sog::int as shots,
        homeTeam_json:score::int as goals,
        awayTeam_json:score::int as goals_against,
        summary_json:teamGameStats[0]:homeValue::int as sog,
        summary_json:teamGameStats[1]:homeValue::float as faceoff_pct,
        summary_json:teamGameStats[2]:homeValue::string as power_play,
        cast(split_part(power_play, '/', 0) as int) as pp_goals,
        cast(split_part(power_play, '/', -1) as int) as pp_attempts,
        summary_json:teamGameStats[2]:awayValue::string as penalty_kill,
        cast(split_part(penalty_kill, '/', 0) as int) as pk_goals_against,
        cast(split_part(penalty_kill, '/', -1) as int) as pk_attempts,
        summary_json:teamGameStats[3]:homeValue::float as pp_pct,
        summary_json:teamGameStats[4]:homeValue::int as pim,
        summary_json:teamGameStats[5]:homeValue::int as hits,
        summary_json:teamGameStats[6]:homeValue::int as blocks,
        summary_json:teamGameStats[7]:homeValue::int as giveaways,
        summary_json:teamGameStats[8]:homeValue::int as takeaways
    from summaries
)

select * 
from home_team_stats
union all
select * 
from away_team_stats
