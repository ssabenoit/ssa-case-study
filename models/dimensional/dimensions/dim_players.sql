{{ config(materialized='table') }}

-- models/dimensional/dimensions/dim_players.sql
-- Players dimension table with player attributes

with

all_players as (
    select distinct
        player_id,
        first_name,
        last_name,
        position,
        number,
        height,
        weight,
        shoots,
        birth_date,
        birth_city,
        birth_state,
        birth_country,
        headshot_url,
        team_abv
    from {{ ref('all_players') }}
),

-- Get the most recent team for each player as their current team
current_team as (
    select 
        player_id,
        team_abv,
        row_number() over (partition by player_id order by player_id) as rn
    from all_players
    qualify rn = 1
),

player_stats_summary as (
    -- Get career summary stats to help identify player type
    select
        player_id,
        count(distinct season) as seasons_played,
        sum(games_played) as career_games,
        sum(goals) as career_goals,
        sum(assists) as career_assists,
        sum(points) as career_points,
        avg(plus_minus) as avg_plus_minus
    from {{ ref('skaters_season_stats_regular') }}
    group by player_id
),

goalie_stats_summary as (
    select
        player_id,
        count(distinct season) as seasons_played,
        sum(gp) as career_games,
        avg(save_pct) as career_save_pct,
        avg(gaa) as career_gaa
    from {{ ref('goalies_season_stats_regular') }}
    group by player_id
),

players_with_attributes as (
    select
        p.player_id,
        p.first_name,
        p.last_name,
        p.first_name || ' ' || p.last_name as full_name,
        p.birth_date,
        -- Calculate age at different points
        datediff(year, p.birth_date::date, current_date()) as current_age,
        case
            when p.birth_date is not null
            then extract(year from p.birth_date::date) + 18  -- Typical draft age
            else null
        end as approximate_draft_year,
        p.birth_city,
        p.birth_state,
        p.birth_country,
        -- Determine nationality based on birth country
        case
            when p.birth_country = 'USA' then 'American'
            when p.birth_country = 'CAN' then 'Canadian'
            when p.birth_country = 'SWE' then 'Swedish'
            when p.birth_country = 'FIN' then 'Finnish'
            when p.birth_country = 'RUS' then 'Russian'
            when p.birth_country = 'CZE' then 'Czech'
            when p.birth_country = 'SVK' then 'Slovak'
            when p.birth_country = 'CHE' then 'Swiss'
            when p.birth_country = 'DEU' then 'German'
            when p.birth_country = 'DNK' then 'Danish'
            when p.birth_country = 'NOR' then 'Norwegian'
            when p.birth_country = 'LVA' then 'Latvian'
            when p.birth_country = 'AUT' then 'Austrian'
            when p.birth_country = 'FRA' then 'French'
            when p.birth_country = 'SVN' then 'Slovenian'
            else p.birth_country
        end as nationality,
        p.height as height_inches,
        p.weight as weight_pounds,
        p.shoots as shoots_catches,
        p.position as primary_position_code,
        -- Expand position names
        case p.position
            when 'C' then 'Center'
            when 'L' then 'Left Wing'
            when 'R' then 'Right Wing'
            when 'D' then 'Defenseman'
            when 'G' then 'Goalie'
            else p.position
        end as primary_position_name,
        -- Position categories
        case 
            when p.position in ('C', 'L', 'R') then 'Forward'
            when p.position = 'D' then 'Defenseman'
            when p.position = 'G' then 'Goalie'
            else 'Unknown'
        end as position_category,
        ct.team_abv as current_team_abv,
        p.number as jersey_number,
        p.headshot_url,
        -- Player status and experience
        case
            when pss.career_games > 0 or gss.career_games > 0 then true
            else false
        end as has_nhl_experience,
        coalesce(pss.seasons_played, gss.seasons_played, 0) as seasons_played,
        coalesce(pss.career_games, gss.career_games, 0) as career_games,
        -- Determine if currently active (played in recent season)
        case
            when exists (
                select 1 
                from {{ ref('int__skaters_per_game_stats') }} s
                where s.player_id = p.player_id
                and s.season >= (select max(season) from {{ ref('int__skaters_per_game_stats') }})
            ) or exists (
                select 1
                from {{ ref('int__goalies_per_game_stats') }} g  
                where g.player_id = p.player_id
                and g.season >= (select max(season) from {{ ref('int__goalies_per_game_stats') }})
            ) then true
            else false
        end as is_active
    from all_players p
    left join current_team ct
        on p.player_id = ct.player_id
    left join player_stats_summary pss
        on p.player_id = pss.player_id
    left join goalie_stats_summary gss
        on p.player_id = gss.player_id
)

select
    row_number() over (order by player_id) as player_key,
    player_id,
    first_name,
    last_name,
    full_name,
    birth_date,
    current_age,
    approximate_draft_year as draft_year_estimate,
    birth_city,
    birth_state,
    birth_country,
    nationality,
    height_inches,
    weight_pounds,
    shoots_catches,
    primary_position_code,
    primary_position_name,
    position_category,
    current_team_abv,
    jersey_number,
    is_active,
    has_nhl_experience,
    seasons_played,
    career_games,
    headshot_url,
    -- Categorize player by experience level
    case
        when career_games = 0 then 'Prospect'
        when career_games between 1 and 25 then 'Rookie'
        when career_games between 26 and 100 then 'Sophomore'
        when career_games between 101 and 300 then 'Experienced'
        when career_games between 301 and 500 then 'Veteran'
        when career_games > 500 then 'Elite Veteran'
        else 'Unknown'
    end as experience_level
from players_with_attributes
order by player_key