{{ config(materialized='table') }}

-- models/dimensional/facts/fct_goalie_game_stats.sql
-- Goalie game-level statistics fact table.
-- Grain: one row per (game, goalie). player_key/team_key are NHL natural ids;
-- the goals-by-period breakdown comes from play-by-play (not estimated).

with

league_games as (
    select *
    from {{ ref('int__league_games') }}
),

goalie_stats as (
    select
        gs.*,
        lg.game_date as date,
        lg.home_team_abv as home_abv,
        lg.away_team_abv as away_abv,
        case
            when gs.type = 'home' then lg.away_team_abv
            else lg.home_team_abv
        end as opponent_abv,
        -- seconds are stored as a TIME; convert once
        extract(hour from gs.toi) * 3600
            + extract(minute from gs.toi) * 60
            + extract(second from gs.toi) as toi_seconds,
        count_if(extract(hour from gs.toi) * 3600
            + extract(minute from gs.toi) * 60
            + extract(second from gs.toi) > 0)
            over (partition by gs.game_id, gs.team_abv) as goalies_used_by_team
    from {{ ref('int__goalies_per_game_stats') }} gs
    inner join league_games lg
        on gs.game_id = lg.game_id
    where gs.game_type in ('regular', 'playoff')
),

-- Real goals-allowed-by-period from the plays fact
goals_by_period as (
    select
        game_key as game_id,
        goalie_player_key as player_id,
        count_if(period_number = 1) as goals_allowed_1st,
        count_if(period_number = 2) as goals_allowed_2nd,
        count_if(period_number = 3) as goals_allowed_3rd,
        count_if(period_category = 'Overtime') as goals_allowed_ot
    from {{ ref('fct_plays') }}
    where event_type_name = 'goal'
        and period_category != 'Shootout'
        and goalie_player_key is not null
    group by game_id, player_id
),

goalie_game_facts as (
    select
        -- Keys
        {{ dbt_utils.generate_surrogate_key(['gs.game_id', 'gs.player_id']) }} as goalie_game_key,
        gs.game_id as game_key,
        gs.player_id as player_key,
        dt.team_key,
        cast(replace(cast(gs.date as string), '-', '') as int) as date_key,
        gs.season as season_key,
        opp.team_key as opponent_team_key,

        -- Game context
        (gs.type = 'home') as is_home_game,

        -- Goalie status
        gs.starter as is_starting_goalie,
        gs.result as decision,

        -- Basic save stats
        gs.shots_against as shots_faced,
        gs.shots_saved as saves,
        gs.goals_against,
        gs.save_pct as save_percentage,

        -- GAA for the game: goals against per 60 minutes of ice time
        case
            when gs.toi_seconds > 0
            then (gs.goals_against * 3600.0) / gs.toi_seconds
            else 0
        end as goals_against_average,

        gs.toi_seconds as time_on_ice_seconds,

        -- Even strength stats
        gs.even_shots_saved as even_strength_saves,
        gs.even_shots_against as even_strength_shots,
        gs.even_goals_against as even_strength_goals_against,
        case
            when gs.even_shots_against > 0
            then cast(gs.even_shots_saved as float) / gs.even_shots_against
            else null
        end as even_strength_save_pct,

        -- Power play (opponent PP) stats
        gs.pp_shots_saved as powerplay_saves,
        gs.pp_shots_against as powerplay_shots,
        gs.pp_goals_against as powerplay_goals_against,
        case
            when gs.pp_shots_against > 0
            then cast(gs.pp_shots_saved as float) / gs.pp_shots_against
            else null
        end as powerplay_save_pct,

        -- Shorthanded (own team on PP) stats
        gs.sh_shots_saved as shorthanded_saves,
        gs.sh_shots_against as shorthanded_shots,
        gs.sh_goals_against as shorthanded_goals_against,
        case
            when gs.sh_shots_against > 0
            then cast(gs.sh_shots_saved as float) / gs.sh_shots_against
            else null
        end as shorthanded_save_pct,

        -- Shutout: allowed no goals and was the only goalie the team used
        (gs.goals_against = 0 and gs.goalies_used_by_team = 1) as shutout_flag,

        -- Real period breakdown (play-by-play derived)
        coalesce(gbp.goals_allowed_1st, 0) as goals_allowed_1st,
        coalesce(gbp.goals_allowed_2nd, 0) as goals_allowed_2nd,
        coalesce(gbp.goals_allowed_3rd, 0) as goals_allowed_3rd,
        coalesce(gbp.goals_allowed_ot, 0) as goals_allowed_ot,

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

        (gs.starter = false and gs.toi_seconds > 0) as is_relief_appearance,

        -- Metadata
        gs.game_type,
        gs.player_id,
        gs.game_id,
        gs.team_abv,
        gs.name as player_name

    from goalie_stats gs
    left join {{ ref('dim_teams') }} dt
        on gs.team_abv = dt.team_abv
    left join {{ ref('dim_teams') }} opp
        on gs.opponent_abv = opp.team_abv
    left join goals_by_period gbp
        on gbp.game_id = gs.game_id
        and gbp.player_id = gs.player_id
)

select
    goalie_game_key,
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
    shutout_flag,
    goals_allowed_1st,
    goals_allowed_2nd,
    goals_allowed_3rd,
    goals_allowed_ot,
    penalty_minutes,
    performance_category,
    is_relief_appearance,
    game_type,
    player_id,
    game_id,
    team_abv,
    player_name,
    -- Quality starts metric
    (is_starting_goalie and save_percentage >= 0.917 and goals_against <= 2) as quality_start,
    (is_starting_goalie and save_percentage < 0.850) as really_bad_start,
    -- Saves above/below average (0.910 league average; parameterized in Phase 3)
    saves - (shots_faced * 0.910) as saves_above_average
from goalie_game_facts
order by
    game_key,
    player_key
