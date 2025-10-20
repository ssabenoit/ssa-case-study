{{ config(materialized='table') }}

-- models/dimensional/facts/fct_player_game_stats.sql
-- Player (skater) game-level statistics fact table

with

skater_stats as (
    select
        s.*,
        g.date,
        g.home_abv,
        g.away_abv,
        case
            when s.type = 'home' then g.away_abv
            else g.home_abv
        end as opponent_abv
    from {{ ref('int__skaters_per_game_stats') }} s
    inner join {{ ref('int__all_games') }} g
        on s.game_id = g.id
),

player_game_facts as (
    select
        -- Keys
        fg.game_key,
        dp.player_key,
        dt.team_key as team_key,
        cast(replace(cast(ss.date as string), '-', '') as int) as date_key,
        ss.season as season_key,
        opp.team_key as opponent_team_key,
        
        -- Game context
        case 
            when ss.type = 'home' then true
            else false
        end as is_home_game,
        
        -- Player info for this game
        coalesce(ap.position, dp.primary_position_code) as position_played,
        ap.number as jersey_number,
        
        -- Offensive stats
        ss.goals,
        ss.assists,
        -- Calculate primary vs secondary assists (would need play-by-play data for accuracy)
        case 
            when ss.assists >= 2 then 1
            when ss.assists = 1 then 1
            else 0
        end as primary_assists,
        case
            when ss.assists >= 2 then ss.assists - 1
            else 0
        end as secondary_assists,
        ss.points,
        ss.shots,
        case
            when ss.shots > 0 then cast(ss.goals as float) / ss.shots
            else 0
        end as shooting_pct,
        ss.pp_goals,
        -- Estimate PP assists (would need detailed data)
        case
            when ss.points > ss.goals and ss.pp_goals > 0 then 1
            else 0
        end as pp_assists,
        
        -- Defensive stats
        ss.plus_minus,
        ss.hits,
        ss.blocks,
        ss.takeaways,
        ss.giveaways,
        
        -- Faceoff stats
        case
            when ss.faceoff_pct is not null and ss.faceoff_pct > 0 
            then round(ss.shifts * ss.faceoff_pct / 100) -- Rough estimate
            else 0
        end as faceoff_wins,
        case
            when ss.faceoff_pct is not null and ss.faceoff_pct > 0 
            then round(ss.shifts * (1 - ss.faceoff_pct / 100)) -- Rough estimate
            else 0
        end as faceoff_losses,
        ss.faceoff_pct,
        
        -- Penalty stats
        ss.pim as penalty_minutes,
        case
            when ss.pim = 2 then 1
            else 0
        end as minor_penalties,
        case
            when ss.pim = 5 then 1
            else 0
        end as major_penalties,
        case
            when ss.pim = 10 then 1
            else 0
        end as misconduct_penalties,
        
        -- Time on ice
        extract(hour from ss.toi) * 3600 + 
        extract(minute from ss.toi) * 60 + 
        extract(second from ss.toi) as time_on_ice_seconds,
        
        -- Estimate TOI by strength (would need detailed shift data)
        -- Using rough estimates based on typical distributions
        (extract(hour from ss.toi) * 3600 + 
         extract(minute from ss.toi) * 60 + 
         extract(second from ss.toi)) * 0.8 as even_strength_toi,
        (extract(hour from ss.toi) * 3600 + 
         extract(minute from ss.toi) * 60 + 
         extract(second from ss.toi)) * 0.15 as powerplay_toi,
        (extract(hour from ss.toi) * 3600 + 
         extract(minute from ss.toi) * 60 + 
         extract(second from ss.toi)) * 0.05 as shorthanded_toi,
        
        -- Special goals (would need play-by-play for accuracy)
        0 as sh_goals,
        0 as sh_assists,
        0 as game_winning_goals,
        0 as overtime_goals,
        
        -- Additional metadata
        ss.shifts,
        ss.game_type,
        ss.player_id,
        ss.game_id,
        ss.team_abv,
        ss.name as player_name
        
    from skater_stats ss
    left join {{ ref('fct_games') }} fg
        on ss.game_id = fg.game_id
    left join {{ ref('dim_players') }} dp
        on ss.player_id = dp.player_id
    left join {{ ref('dim_teams') }} dt
        on ss.team_abv = dt.team_abv
    left join {{ ref('dim_teams') }} opp
        on ss.opponent_abv = opp.team_abv
    left join {{ ref('int__all_players') }} ap
        on ss.player_id = ap.player_id
        and ss.team_abv = ap.team_abv
    where ss.game_type in ('regular', 'playoff')
)

select
    game_key,
    player_key,
    team_key,
    date_key,
    season_key,
    opponent_team_key,
    is_home_game,
    position_played,
    jersey_number,
    goals,
    assists,
    primary_assists,
    secondary_assists,
    points,
    plus_minus,
    shots,
    shooting_pct,
    hits,
    blocks,
    giveaways,
    takeaways,
    faceoff_wins,
    faceoff_losses,
    faceoff_pct,
    penalty_minutes,
    minor_penalties,
    major_penalties,
    misconduct_penalties,
    time_on_ice_seconds,
    even_strength_toi,
    powerplay_toi,
    shorthanded_toi,
    pp_goals,
    pp_assists,
    sh_goals,
    sh_assists,
    game_winning_goals,
    overtime_goals,
    shifts,
    -- Calculated metrics
    case
        when goals >= 3 then true
        else false
    end as hat_trick,
    case
        when points >= 4 then true
        else false  
    end as four_point_game,
    case
        when goals >= 4 then true
        else false
    end as four_goal_game,
    goals + assists as point_contributions
from player_game_facts
order by 
    game_key, 
    player_key