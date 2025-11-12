{{ config(materialized='table') }}

-- models/dimensional/facts/fct_games.sql
-- Game-level fact table with one row per game

with

games_base as (
    select
        g.id as game_id,
        g.season,
        g.date,
        case 
            when sg.game_type = 2 then 'Regular'
            when sg.game_type = 3 then 'Playoff'
            when sg.game_type = 1 then 'Preseason'
            else 'Other'
        end as game_type,
        g.home_abv,
        g.away_abv,
        g.home_score,
        g.away_score,
        g.game_outcome,
        gi.venue,
        gi.is_neutral,
        gi.start_time_utc,
        gi.venue_tz,
        gi.venue_utc_offset
    from {{ ref('int__all_games') }} g 
    left join {{ ref('int__basic_games_info') }} gi 
        on g.id = gi.game_id
    left join {{ ref('stg_nhl__season_schedules') }} sg 
        on g.id = sg.id
)

,

game_stats as (
    select
        game_id,
        sum(case when type = 'home' then shots end) as home_shots,
        sum(case when type = 'away' then shots end) as away_shots,
        sum(case when type = 'home' then hits end) as home_hits,
        sum(case when type = 'away' then hits end) as away_hits,
        sum(case when type = 'home' then pim end) as home_pim,
        sum(case when type = 'away' then pim end) as away_pim,
        sum(case when type = 'home' then pp_goals end) as home_pp_goals,
        sum(case when type = 'away' then pp_goals end) as away_pp_goals,
        sum(case when type = 'home' then pp_attempts end) as home_pp_attempts,
        sum(case when type = 'away' then pp_attempts end) as away_pp_attempts,
        sum(case when type = 'home' then faceoff_pct end) as home_faceoff_pct,
        sum(case when type = 'away' then faceoff_pct end) as away_faceoff_pct
    from {{ ref('stg_nhl__game_summaries') }}
    group by game_id
),

game_attendance as (
    -- Placeholder for attendance data when available
    select
        game_id,
        null::int as attendance
    from games_base
),

game_facts as (
    select
        gb.game_id,
        cast(replace(cast(gb.date as string), '-', '') as int) as date_key,
        gb.season as season_key,
        gb.game_type,
        ht.team_key as home_team_key,
        at.team_key as away_team_key,
        gb.venue as venue_name,
        ga.attendance,
        -- Score at end of regulation
        case 
            when gb.game_outcome = 'REG' then gb.home_score
            when gb.game_outcome in ('OT', 'SO') then 
                case 
                    when gb.home_score > gb.away_score then gb.home_score - 1
                    else gb.home_score
                end
            else gb.home_score
        end as regulation_home_score,
        case 
            when gb.game_outcome = 'REG' then gb.away_score
            when gb.game_outcome in ('OT', 'SO') then 
                case 
                    when gb.away_score > gb.home_score then gb.away_score - 1
                    else gb.away_score
                end
            else gb.away_score
        end as regulation_away_score,
        -- Final scores
        gb.home_score as final_home_score,
        gb.away_score as final_away_score,
        -- Determine winner/loser
        case 
            when gb.home_score > gb.away_score then ht.team_key
            when gb.away_score > gb.home_score then at.team_key
            else null
        end as winning_team_key,
        case 
            when gb.home_score < gb.away_score then ht.team_key
            when gb.away_score < gb.home_score then at.team_key
            else null
        end as losing_team_key,
        -- Game duration and overtime info
        case
            when gb.game_outcome = 'REG' then 60
            when gb.game_outcome = 'OT' then 65
            when gb.game_outcome = 'SO' then 65
            else null
        end as game_duration_minutes,
        case
            when gb.game_outcome = 'OT' then 1
            when gb.game_outcome = 'SO' then 1
            else 0
        end as overtime_periods,
        case
            when gb.game_outcome = 'SO' then true
            else false
        end as is_shootout,
        gb.is_neutral as is_neutral_site,
        -- Additional game stats
        gs.home_shots,
        gs.away_shots,
        gs.home_hits,
        gs.away_hits,
        gs.home_pim,
        gs.away_pim,
        gs.home_pp_goals,
        gs.away_pp_goals,
        gs.home_pp_attempts,
        gs.away_pp_attempts,
        gs.home_faceoff_pct,
        gs.away_faceoff_pct,
        -- Goal differential
        gb.home_score - gb.away_score as goal_differential,
        abs(gb.home_score - gb.away_score) as goal_differential_abs,
        -- Game flags
        case 
            when abs(gb.home_score - gb.away_score) >= 5 then true
            else false
        end as is_blowout,
        case
            when gb.home_score + gb.away_score >= 10 then true
            else false
        end as is_high_scoring,
        case
            when gb.home_score + gb.away_score <= 3 then true
            else false
        end as is_low_scoring,
        gb.start_time_utc,
        gb.venue_tz,
        gb.venue_utc_offset
    from games_base gb
    left join {{ ref('dim_teams') }} ht
        on gb.home_abv = ht.team_abv
    left join {{ ref('dim_teams') }} at
        on gb.away_abv = at.team_abv
    left join game_stats gs
        on gb.game_id = gs.game_id
    left join game_attendance ga
        on gb.game_id = ga.game_id
)

select
    row_number() over (order by game_id) as game_key,
    *
from game_facts
order by game_key