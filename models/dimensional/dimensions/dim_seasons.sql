{{ config(materialized='table') }}

-- models/dimensional/dimensions/dim_seasons.sql  
-- Season dimension table with NHL season attributes

with

season_boundaries as (
    select
        season,
        min(date) as season_start_date,
        max(date) as season_end_date
    from {{ ref('standings_by_day') }}
    group by season
),

season_games as (
    select
        g.season,
        count(distinct game_id) as total_games,
        count(distinct case when game_type = 'regular' then game_id end) as regular_season_games,
        count(distinct case when game_type = 'playoff' then game_id end) as playoff_games,
        min(case when game_type = 'regular' then date end) as regular_season_start,
        max(case when game_type = 'regular' then date end) as regular_season_end,
        min(case when game_type = 'playoff' then date end) as playoff_start,
        max(case when game_type = 'playoff' then date end) as playoff_end
    from {{ ref('int__all_games') }} g
    inner join {{ ref('int__team_per_game_stats') }} t
        on g.id = t.game_id
    group by g.season
),

season_teams as (
    select
        season,
        count(distinct team_abv) as total_teams
    from {{ ref('standings_by_day') }}
    group by season
),

seasons as (
    select
        sb.season as season_key,
        sb.season as season_id,
        -- Format as "2023-24" style
        substr(cast(sb.season as string), 1, 4) || '-' || 
        substr(cast(sb.season as string), 7, 2) as season_display_name,
        substr(cast(sb.season as string), 1, 4)::int as start_year,
        substr(cast(sb.season as string), 5, 4)::int as end_year,
        sb.season_start_date as start_date,
        sb.season_end_date as end_date,
        sg.regular_season_start,
        sg.regular_season_end,
        sg.playoff_start,
        sg.playoff_end,
        sg.regular_season_games,
        sg.playoff_games,
        sg.total_games,
        st.total_teams,
        datediff(day, sb.season_start_date, sb.season_end_date) + 1 as season_length_days,
        datediff(day, sg.regular_season_start, sg.regular_season_end) + 1 as regular_season_days,
        datediff(day, sg.playoff_start, sg.playoff_end) + 1 as playoff_length_days,
        -- Season type flags
        case 
            when sb.season = 20202021 then true 
            else false 
        end as is_covid_season,
        case
            when sg.regular_season_games < 1230 then true  -- Less than 82 games * 15 matchups
            else false
        end as is_shortened_season,
        -- Additional metadata
        case sb.season
            when 20152016 then 71500000
            when 20162017 then 73000000  
            when 20172018 then 75000000
            when 20182019 then 79500000
            when 20192020 then 81500000
            when 20202021 then 81500000  -- Flat cap due to COVID
            when 20212022 then 81500000  -- Flat cap continued
            when 20222023 then 82500000
            when 20232024 then 83500000
            when 20242025 then 88000000
            else null
        end as salary_cap,
        case sb.season
            when 20152016 then 'Jonathan Toews'
            when 20162017 then 'Sidney Crosby'
            when 20172018 then 'Alexander Ovechkin'
            when 20182019 then 'Alex Pietrangelo'
            when 20192020 then 'Victor Hedman'
            when 20202021 then 'Victor Hedman'
            when 20212022 then 'Gabriel Landeskog'
            when 20222023 then 'Jonathan Marchessault'
            when 20232024 then 'Aleksander Barkov'
            else null
        end as stanley_cup_captain
    from season_boundaries sb
    left join season_games sg
        on sb.season = sg.season
    left join season_teams st
        on sb.season = st.season
)

select *
from seasons
order by season_key