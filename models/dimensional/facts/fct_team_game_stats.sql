{{ config(materialized='table') }}

-- models/dimensional/facts/fct_team_game_stats.sql
-- Team game-level statistics fact table (2 rows per game, one per team).
-- Grain: one row per (game, team). Faceoff counts are real play-by-play
-- counts; score-state flags are derived from actual goal sequence.

with

team_game_stats as (
    select *
    from {{ ref('int__team_per_game_stats') }}
    where game_type in ('regular', 'playoff')
),

-- Goal timing per (game, team) for score-state flags
period_goals as (
    select
        game_key as game_id,
        event_team_key as team_id,
        count_if(period_number = 1) as p1_goals,
        count_if(period_number <= 2) as p2_cumulative_goals
    from {{ ref('fct_plays') }}
    where event_type_name = 'goal'
        and period_category != 'Shootout'
    group by game_id, team_id
),

first_goals as (
    select
        game_key as game_id,
        min_by(event_team_key, game_elapsed_seconds) as first_goal_team_id
    from {{ ref('fct_plays') }}
    where event_type_name = 'goal'
        and period_category != 'Shootout'
    group by game_id
),

team_game_facts as (
    select
        -- Keys
        {{ dbt_utils.generate_surrogate_key(['tgs.game_id', 'tgs.team_id']) }} as team_game_key,
        tgs.game_id as game_key,
        dt.team_key,
        opp_dt.team_key as opponent_team_key,
        cast(replace(cast(tgs.game_date as string), '-', '') as int) as date_key,
        tgs.season as season_key,

        -- Game context
        (tgs.type = 'home') as is_home,

        -- Game result (self-contained: own goals vs opponent goals + how the game ended)
        case
            when tgs.goals > tgs.goals_against then 'W'
            when coalesce(tgs.last_period_type, 'REG') in ('OT', 'SO') then 'OTL'
            else 'L'
        end as game_result,

        case
            when tgs.goals > tgs.goals_against then 2
            when coalesce(tgs.last_period_type, 'REG') in ('OT', 'SO') then 1
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

        -- Faceoffs: real counts from play-by-play events
        tgs.faceoffs_won as faceoff_wins,
        tgs.faceoffs_taken - tgs.faceoffs_won as faceoff_losses,
        tgs.faceoff_pct as faceoff_win_pct,

        -- Physical play
        tgs.hits,
        tgs.blocks,
        tgs.giveaways,
        tgs.takeaways,
        tgs.pim as penalty_minutes,

        -- Score state (derived from actual goal sequence)
        (fgo.first_goal_team_id = tgs.team_id) as score_first_flag,
        coalesce(own_pg.p1_goals, 0) > coalesce(opp_pg.p1_goals, 0) as lead_after_1st,
        coalesce(own_pg.p2_cumulative_goals, 0) > coalesce(opp_pg.p2_cumulative_goals, 0) as lead_after_2nd,

        -- Comeback win: won despite trailing at the first or second intermission
        case
            when tgs.goals > tgs.goals_against
                and (
                    coalesce(own_pg.p1_goals, 0) < coalesce(opp_pg.p1_goals, 0)
                    or coalesce(own_pg.p2_cumulative_goals, 0) < coalesce(opp_pg.p2_cumulative_goals, 0)
                )
            then true
            else false
        end as comeback_win_flag,

        -- Additional metrics
        tgs.sog,
        tgs.blocks as blocked_shots,

        -- Metadata
        tgs.game_type,
        tgs.game_date,
        tgs.last_period_type as game_outcome,
        tgs.game_id,
        tgs.team_abv

    from team_game_stats tgs
    left join {{ ref('int__league_games') }} lg
        on lg.game_id = tgs.game_id
    left join {{ ref('dim_teams') }} dt
        on tgs.team_abv = dt.team_abv
    left join {{ ref('dim_teams') }} opp_dt
        on opp_dt.team_abv = case
            when tgs.type = 'home' then lg.away_team_abv
            else lg.home_team_abv
        end
    left join period_goals own_pg
        on own_pg.game_id = tgs.game_id and own_pg.team_id = tgs.team_id
    left join period_goals opp_pg
        on opp_pg.game_id = tgs.game_id and opp_pg.team_id != tgs.team_id
    left join first_goals fgo
        on fgo.game_id = tgs.game_id
)

select
    team_game_key,
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
    game_type,
    game_date,
    game_outcome,
    game_id,
    team_abv,
    (game_outcome = 'OT') as is_overtime,
    (game_outcome = 'SO') as is_shootout,
    penalty_kill_goals_against as shorthanded_goals_against,
    penalty_kill_situations as times_shorthanded,
    shots_for - shots_against as shot_differential,
    (goals_for >= 5) as scored_5_plus,
    (goals_against = 0) as shutout_for,
    (goals_for = 0) as shutout_against,
    (powerplay_opportunities > 0 and powerplay_goals = 0) as pp_drought,
    (penalty_kill_situations > 0 and penalty_kill_goals_against = 0) as perfect_pk,
    case
        when (shots_for + shots_against) > 0
        then cast(shots_for as float) / (shots_for + shots_against)
        else 0.5
    end as shot_share_pct
from team_game_facts
order by
    game_key,
    team_key
