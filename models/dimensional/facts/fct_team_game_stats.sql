{{ config(materialized='table') }}

-- models/dimensional/facts/fct_team_game_stats.sql
-- Team game-level statistics fact table (2 rows per game, one for each team)

with

team_game_stats as (
    select
        tgs.*,
        g.date,
        g.home_abv,
        g.away_abv,
        g.home_score,
        g.away_score,
        g.game_outcome,
        case
            when tgs.type = 'home' then g.away_abv
            else g.home_abv
        end as opponent_abv
    from {{ ref('int__team_per_game_stats') }} tgs
    inner join {{ ref('int__all_games') }} g
        on tgs.game_id = g.id
),

team_game_facts as (
    select
        -- Keys
        fg.game_key,
        dt.team_key,
        opp.team_key as opponent_team_key,
        cast(replace(cast(tgs.date as string), '-', '') as int) as date_key,
        tgs.season as season_key,
        
        -- Game context
        case 
            when tgs.type = 'home' then true
            else false
        end as is_home,
        
        -- Game result
        case
            when tgs.type = 'home' and tgs.home_score > tgs.away_score then 'W'
            when tgs.type = 'away' and tgs.away_score > tgs.home_score then 'W'
            when tgs.game_outcome = 'REG' then 'L'
            when tgs.game_outcome in ('OT', 'SO') then 'OTL'
            else 'L'
        end as game_result,
        
        -- Points earned
        case
            when (tgs.type = 'home' and tgs.home_score > tgs.away_score) or
                 (tgs.type = 'away' and tgs.away_score > tgs.home_score) then 2
            when tgs.game_outcome in ('OT', 'SO') and
                 ((tgs.type = 'home' and tgs.home_score < tgs.away_score) or
                  (tgs.type = 'away' and tgs.away_score < tgs.home_score)) then 1
            else 0
        end as points_earned,
        
        -- Goals
        tgs.goals as goals_for,
        tgs.goals_against,
        tgs.goals - tgs.goals_against as goal_differential,
        
        -- Shots
        tgs.shots as shots_for,
        tgs.shots_against,
        case
            when tgs.shots > 0 then cast(tgs.goals as float) / tgs.shots
            else 0
        end as shooting_pct,
        tgs.save_pct,
        
        -- Special teams
        tgs.pp_goals as powerplay_goals,
        tgs.pp_attempts as powerplay_opportunities,
        case
            when tgs.pp_attempts > 0 then cast(tgs.pp_goals as float) / tgs.pp_attempts
            else 0
        end as powerplay_pct,
        tgs.pk_goals_against as penalty_kill_goals_against,
        tgs.pk_attempts as penalty_kill_situations,
        case
            when tgs.pk_attempts > 0 
            then cast(tgs.pk_attempts - tgs.pk_goals_against as float) / tgs.pk_attempts
            else 0
        end as penalty_kill_pct,
        
        -- Faceoffs
        round(tgs.faceoff_pct * tgs.shots / 100) as faceoff_wins,  -- Rough estimate
        round((100 - tgs.faceoff_pct) * tgs.shots / 100) as faceoff_losses,  -- Rough estimate
        tgs.faceoff_pct as faceoff_win_pct,
        
        -- Physical play
        tgs.hits,
        tgs.blocks,
        tgs.giveaways,
        tgs.takeaways,
        tgs.pim as penalty_minutes,
        
        -- Score state flags (would need period data for accuracy)
        case
            when tgs.goals > 0 then true
            else false
        end as score_first_flag,
        null::boolean as lead_after_1st,
        null::boolean as lead_after_2nd,
        
        -- Comeback win flag
        case
            when (tgs.type = 'home' and tgs.home_score > tgs.away_score and tgs.goals_against > tgs.goals/2) or
                 (tgs.type = 'away' and tgs.away_score > tgs.home_score and tgs.goals_against > tgs.goals/2)
            then true
            else false
        end as comeback_win_flag,
        
        -- Additional metrics
        tgs.sog,
        tgs.blocks as blocked_shots,
        
        -- Metadata
        tgs.game_type,
        tgs.date,
        tgs.game_outcome,
        tgs.game_id,
        tgs.team_abv
        
    from team_game_stats tgs
    left join {{ ref('fct_games') }} fg
        on tgs.game_id = fg.game_id
    left join {{ ref('dim_teams') }} dt
        on tgs.team_abv = dt.team_abv
    left join {{ ref('dim_teams') }} opp
        on tgs.opponent_abv = opp.team_abv
    where tgs.game_type in ('regular', 'playoff')
)

select
    game_key,
    team_key,
    opponent_team_key,
    date_key,
    season_key,
    is_home,
    game_result,
    points_earned,
    goals_for,
    goals_against,
    goal_differential,
    shots_for,
    shots_against,
    shooting_pct,
    save_pct,
    powerplay_goals,
    powerplay_opportunities,
    powerplay_pct,
    penalty_kill_goals_against,
    penalty_kill_situations,
    penalty_kill_pct,
    faceoff_wins,
    faceoff_losses,
    faceoff_win_pct,
    hits,
    blocks,
    giveaways,
    takeaways,
    penalty_minutes,
    score_first_flag,
    lead_after_1st,
    lead_after_2nd,
    comeback_win_flag,
    -- Metadata columns
    game_type,
    date as game_date,
    game_outcome,
    -- Additional derived columns
    case
        when game_outcome = 'OT' then true
        else false
    end as is_overtime,
    case
        when game_outcome = 'SO' then true
        else false
    end as is_shootout,
    penalty_kill_goals_against as shorthanded_goals_against,
    penalty_kill_situations as times_shorthanded,
    -- Calculated metrics
    shots_for - shots_against as shot_differential,
    case
        when goals_for >= 5 then true
        else false
    end as scored_5_plus,
    case
        when goals_against = 0 then true
        else false
    end as shutout_for,
    case
        when goals_for = 0 then true
        else false
    end as shutout_against,
    case
        when powerplay_opportunities > 0 and powerplay_goals = 0 then true
        else false
    end as pp_drought,
    case
        when penalty_kill_situations > 0 and penalty_kill_goals_against = 0 then true
        else false
    end as perfect_pk,
    -- Possession proxy (shot share)
    case
        when (shots_for + shots_against) > 0
        then cast(shots_for as float) / (shots_for + shots_against)
        else 0.5
    end as shot_share_pct
from team_game_facts
order by 
    game_key, 
    team_key