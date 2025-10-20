{{ config(materialized='table') }}

-- models/dimensional/facts/fct_upcoming_games.sql
-- Fact table for upcoming games with proper dimensional keys

with

upcoming_games_source as (
    select *
    from {{ ref('stg_nhl__season_schedules') }}
    where
        game_state = 'FUT'
        and game_type = 2  -- Regular season games
),

teams as (
    select *
    from {{ ref('dim_teams') }}
),

upcoming_games_fact as (
    select
        -- Keys
        g.id as game_id,
        g.id as game_key,  -- Using game_id as the key since these are future games
        cast(replace(cast(g.game_date as string), '-', '') as int) as date_key,
        g.season as season_key,
        ht.team_key as home_team_key,
        at.team_key as away_team_key,
        
        -- Game attributes
        g.game_date,
        g.game_type,
        g.game_state,
        
        -- Team identifiers (for convenience)
        g.home_id,
        g.home_abv,
        ht.team_name as home_team_name,
        ht.logo_url as home_logo,
        
        g.away_id,
        g.away_abv,
        at.team_name as away_team_name,
        at.logo_url as away_logo,
        
        -- Additional metadata
        case
            when extract(dayofweek from g.game_date::date) in (1, 7) then true
            else false
        end as is_weekend,
        
        case
            when extract(hour from g.game_date::timestamp) < 17 then 'Afternoon'
            when extract(hour from g.game_date::timestamp) < 20 then 'Early Evening'
            else 'Late Evening'
        end as game_time_category,
        
        -- Days until game (useful for filtering)
        datediff('day', current_date(), g.game_date::date) as days_until_game,
        
        -- Divisional/Conference matchup flags
        case
            when ht.division = at.division then true
            else false
        end as is_divisional_game,
        
        case
            when ht.conference = at.conference then true
            else false
        end as is_conference_game,
        
        -- Timestamp
        current_timestamp() as last_updated
        
    from upcoming_games_source g
    left join teams ht
        on g.home_abv = ht.team_abv
    left join teams at
        on g.away_abv = at.team_abv
)

select *
from upcoming_games_fact
order by 
    game_date,
    game_id