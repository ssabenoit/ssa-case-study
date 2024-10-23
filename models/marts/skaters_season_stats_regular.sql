-- models/marts/skaters_season_stats_regular.sql
-- compiles stats for each player for each regular season (from the per game stats table)

with skaters_info as (
    select *
    from {{ ref("int__all_skaters") }}
),

skater_stats as (
    select *
    from {{ ref("skaters_per_game_stats_all") }}
),

skater_season_stats as (
    select
        season,
        player_id,
        name,
        count(*) as games_played,
        sum(goals) as goals,
        sum(assists) as assists,
        sum(hits) as hits,
        sum(shots) as shots,
        avg(faceoff_pct) as faceoff_pct,
        sum(pim) as pim,
        sum(plus_minus) as plus_minus,
        sum(points) as points,
        sum(pp_goals) as pp_goals,
        sum(giveaways) as giveaways,
        sum(takeaways) as takeaways,
        sum(shifts) as shifts,
        SUM(EXTRACT(hour FROM toi) * 3600 +  EXTRACT(minute FROM toi) * 60 + EXTRACT(second FROM toi) ) as total_toi,
        /*
        TIMEADD(
            'second', 
            SUM(
                EXTRACT(hour FROM toi) * 3600 + 
                EXTRACT(minute FROM toi) * 60 + 
                EXTRACT(second FROM toi)
            ),
            TO_TIME('00:00:00')
        ) AS total_toi, -- doesn't allow more than 24 hours (could remove it)
        */
        TIMEADD(
            'second', 
            AVG(
                EXTRACT(hour FROM toi) * 3600 + 
                EXTRACT(minute FROM toi) * 60 + 
                EXTRACT(second FROM toi)
            ),
            TO_TIME('00:00:00')
        ) AS avg_toi,
        -- avg(total_toi) as average_toi
        -- sum(toi) as toi
    from skater_stats
    where game_type = 'regular'
    group by season, player_id, name
)

select 
    ss.*,
    si.first_name,
    si.last_name,
    si.team_abv,
    si.position
from skater_season_stats ss
left join skaters_info si on ss.player_id = si.player_id