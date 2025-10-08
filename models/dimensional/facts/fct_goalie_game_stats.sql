{{ config(materialized='table') }}

-- models/dimensional/facts/fct_goalie_game_stats.sql
-- Goalie game-level statistics fact table

with

goalie_stats as (
    select
        gs.*,
        g.date,
        g.home_abv,
        g.away_abv,
        case
            when gs.type = 'home' then g.away_abv
            else g.home_abv
        end as opponent_abv
    from {{ ref('int__goalies_per_game_stats') }} gs
    inner join {{ ref('int__all_games') }} g
        on gs.game_id = g.id
),

goalie_game_facts as (
    select
        -- Keys
        fg.game_key,
        dp.player_key,
        dt.team_key,
        cast(replace(cast(gs.date as string), '-', '') as int) as date_key,
        gs.season as season_key,
        opp.team_key as opponent_team_key,
        
        -- Game context
        case 
            when gs.type = 'home' then true
            else false
        end as is_home_game,
        
        -- Goalie status
        gs.starter as is_starting_goalie,
        gs.result as decision,
        
        -- Basic save stats
        gs.shots_against as shots_faced,
        gs.shots_saved as saves,
        gs.goals_against,
        gs.save_pct as save_percentage,
        
        -- Calculate GAA for the game
        case
            when extract(hour from gs.toi) * 60 + extract(minute from gs.toi) > 0
            then (gs.goals_against * 60.0) / 
                 (extract(hour from gs.toi) * 60 + extract(minute from gs.toi))
            else 0
        end as goals_against_average,
        
        -- Time on ice
        extract(hour from gs.toi) * 3600 + 
        extract(minute from gs.toi) * 60 + 
        extract(second from gs.toi) as time_on_ice_seconds,
        
        -- Even strength stats
        gs.even_shots_saved as even_strength_saves,
        gs.even_shots_against as even_strength_shots,
        gs.even_goals_against as even_strength_goals_against,
        case
            when gs.even_shots_against > 0
            then cast(gs.even_shots_saved as float) / gs.even_shots_against
            else null
        end as even_strength_save_pct,
        
        -- Power play stats
        gs.pp_shots_saved as powerplay_saves,
        gs.pp_shots_against as powerplay_shots,
        gs.pp_goals_against as powerplay_goals_against,
        case
            when gs.pp_shots_against > 0
            then cast(gs.pp_shots_saved as float) / gs.pp_shots_against
            else null
        end as powerplay_save_pct,
        
        -- Shorthanded stats
        gs.sh_shots_saved as shorthanded_saves,
        gs.sh_shots_against as shorthanded_shots,
        gs.sh_goals_against as shorthanded_goals_against,
        case
            when gs.sh_shots_against > 0
            then cast(gs.sh_shots_saved as float) / gs.sh_shots_against
            else null
        end as shorthanded_save_pct,
        
        -- Penalty shot stats (would need detailed data)
        0 as penalty_shot_saves,
        0 as penalty_shot_attempts,
        
        -- Shutout flag
        case
            when gs.goals_against = 0 
                and gs.starter = true
                and gs.result = 'W'
            then true
            else false
        end as shutout_flag,
        
        -- Period breakdown (would need detailed data)
        null::int as goals_allowed_1st,
        null::int as goals_allowed_2nd,
        null::int as goals_allowed_3rd,
        null::int as goals_allowed_ot,
        
        -- Additional stats
        gs.pim as penalty_minutes,
        
        -- Quality metrics
        case
            when gs.save_pct >= 0.950 then 'Elite'
            when gs.save_pct >= 0.920 then 'Excellent'
            when gs.save_pct >= 0.900 then 'Good'
            when gs.save_pct >= 0.880 then 'Average'
            else 'Below Average'
        end as performance_category,
        
        -- Relief appearance flag
        case
            when gs.starter = false and gs.result is not null
            then true
            else false
        end as is_relief_appearance,
        
        -- Metadata
        gs.game_type,
        gs.player_id,
        gs.game_id,
        gs.team_abv,
        gs.name as player_name
        
    from goalie_stats gs
    left join {{ ref('fct_games') }} fg
        on gs.game_id = fg.game_id
    left join {{ ref('dim_players') }} dp
        on gs.player_id = dp.player_id
    left join {{ ref('dim_teams') }} dt
        on gs.team_abv = dt.team_abv
    left join {{ ref('dim_teams') }} opp
        on gs.opponent_abv = opp.team_abv
    where gs.game_type in ('regular', 'playoff')
)

select
    game_key,
    player_key,
    team_key,
    date_key,
    season_key,
    opponent_team_key,
    is_home_game,
    is_starting_goalie,
    decision,
    shots_faced,
    saves,
    goals_against,
    save_percentage,
    goals_against_average,
    time_on_ice_seconds,
    even_strength_saves,
    even_strength_shots,
    even_strength_goals_against,
    even_strength_save_pct,
    powerplay_saves,
    powerplay_shots,
    powerplay_goals_against,
    powerplay_save_pct,
    shorthanded_saves,
    shorthanded_shots,
    shorthanded_goals_against,
    shorthanded_save_pct,
    penalty_shot_saves,
    penalty_shot_attempts,
    shutout_flag,
    goals_allowed_1st,
    goals_allowed_2nd,
    goals_allowed_3rd,
    goals_allowed_ot,
    penalty_minutes,
    performance_category,
    is_relief_appearance,
    -- Calculated quality starts metric
    case
        when is_starting_goalie 
            and save_percentage >= 0.917
            and goals_against <= 2
        then true
        else false
    end as quality_start,
    -- Really bad starts
    case
        when is_starting_goalie
            and save_percentage < 0.850
        then true
        else false
    end as really_bad_start,
    -- Saves above/below average (using 0.910 as league average)
    saves - (shots_faced * 0.910) as saves_above_average
from goalie_game_facts
order by 
    game_key, 
    player_key