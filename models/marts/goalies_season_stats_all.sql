-- models/marts/goalies_season_stats_regular.sql
-- compiles stats for each goalie for each regular season (from the per game stats table)

with goalies_info as (
    select *
    from {{ ref("int__all_goalies") }}
),

goalies_stats as (
    select *
    from {{ ref("goalies_per_game_stats_all") }}
),

goalies_season_stats as (
    select
        season,
        player_id,
        name,
        position,
        count(*) as games_played,
        avg(goals_against) as gaa,
        count(case when starter then 1 end) as starts,
        sum(shots_saved) as saves,
        sum(shots_against) as shots_faced,
        sum(even_goals_against) as even_goals_against,
        sum(pp_goals_against) as pp_ga,
        

        -- sum(goals) as goals,
        -- sum(assists) as assists,
        -- sum(hits) as hits,
        -- sum(shots) as shots,
        -- avg(faceoff_pct) as faceoff_pct,
        -- sum(pim) as pim,
        -- sum(plus_minus) as plus_minus,
        -- sum(points) as points,
        -- sum(pp_goals) as pp_goals,
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
        -- TIMEADD(
        --     'second', 
        --     AVG(
        --         EXTRACT(hour FROM toi) * 3600 + 
        --         EXTRACT(minute FROM toi) * 60 + 
        --         EXTRACT(second FROM toi)
        --     ),
        --     TO_TIME('00:00:00')
        -- ) AS avg_toi,
        -- avg(total_toi) as average_toi
        -- sum(toi) as toi
    from goalies_stats
    where game_type = 'regular'
    group by season, player_id, name
)

-- select 
--     ss.*,
--     si.first_name,
--     si.last_name,
--     si.team_abv,
--     si.position
-- from goalies_season_stats ss
-- left join goalies_info si on ss.player_id = si.player_id

select *
from goalies_stats