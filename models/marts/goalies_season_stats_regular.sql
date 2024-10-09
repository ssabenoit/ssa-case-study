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
        -- position,
        count(*) as gp,
        round(avg(goals_against), 2) as gaa,
        count(case when starter then 1 end) as starts,
        sum(shots_saved) as saves,
        sum(shots_against) as shots_faced,
        sum(even_goals_against) as even_goals_against,
        sum(even_shots_saved) as even_shots_saved,
        sum(even_shots_against) as even_shots_against,
        sum(pp_goals_against) as pp_ga,
        sum(pp_shots_saved) as pp_saves,
        sum(pp_shots_against) as pp_shots_against,
        sum(sh_goals_against) as sh_goals_against,
        coalesce(sum(sh_shots_saved), 1) as sh_saves,
        coalesce(sum(sh_shots_against), 1) as sh_shots_against,
        sum(pim) as pim
    from goalies_stats
    where game_type = 'regular' and starter = True or result is not null
    group by season, player_id, name
)

select 
    *,
    saves/shots_faced as save_pct,
    case
        when even_shots_saved != 0 and even_shots_against != 0 then even_shots_saved/even_shots_against
        else 0
    end as even_save_pct,
    case
        when pp_saves != 0 and pp_shots_against != 0 then pp_saves/pp_shots_against
        else 0
    end as pp_save_pct,
    case
        when sh_saves != 0 and sh_shots_against != 0 then sh_saves/sh_shots_against
        else 0
    end as sh_save_pct
    -- sh_saves/sh_shots_against as sh_save_pct
from goalies_season_stats