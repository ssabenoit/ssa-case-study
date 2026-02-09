{{ config(materialized='table') }}

-- models/dimensional/facts/fct_plays.sql
-- Play-by-play event-level fact table

with

plays_base as (
    select
        p.*,
        g.date,
        g.season,
        g.away_id,
        g.away_abv,
        g.home_id,
        g.home_abv
    from {{ ref('stg_nhl__play_by_play') }} p
    inner join {{ ref('int__all_games') }} g
        on p.game_id = g.id
)
,

play_facts as (
    select
        -- Keys
        row_number() over (order by pb.game_id, pb.event_id) as play_key,
        fg.game_key,
        cast(replace(cast(pb.date as string), '-', '') as int) as date_key,
        pb.season as season_key,

        -- Event identification
        pb.event_id as event_idx,
        pb.sort_order,

        -- Period information
        pb.period as period_number,
        pb.period_type,
        pb.time_in_period,
        pb.elapsed_seconds as time_elapsed_seconds,
        pb.time_remaining,
        -- Calculate total elapsed time in game
        case
            when pb.period = 1 then pb.elapsed_seconds
            when pb.period = 2 then 1200 + pb.elapsed_seconds
            when pb.period = 3 then 2400 + pb.elapsed_seconds
            when pb.period = 4 then 3600 + pb.elapsed_seconds  -- OT
            when pb.period = 5 then 3900 + pb.elapsed_seconds  -- SO
            else pb.elapsed_seconds
        end as game_elapsed_seconds,

        -- Event details
        pb.type_code as event_type_id,
        pb.description as event_type_name,
        dt.team_key as event_team_key,

        -- Players involved
        null::int as primary_player_key,
        null::int as secondary_player_key,

        -- Location data
        pb.x_pos as x_coordinate,
        pb.y_pos as y_coordinate,
        pb.zone_code,

        -- Shot-specific details
        case
            when pb.description in ('shot-on-goal', 'goal', 'missed-shot', 'blocked-shot')
            then pb.shot_type
            else null
        end as shot_type,

        -- Calculate shot distance using Pythagorean theorem
        case
            when pb.description in ('shot-on-goal', 'goal', 'missed-shot', 'blocked-shot')
                and pb.x_pos is not null and pb.y_pos is not null
            then sqrt(power(abs(89 - abs(pb.x_pos)), 2) + power(pb.y_pos, 2))
            else null
        end as shot_distance,

        -- Calculate shot angle
        case
            when pb.description in ('shot-on-goal', 'goal', 'missed-shot', 'blocked-shot')
                and pb.x_pos is not null and pb.y_pos is not null
            then degrees(atan2(pb.y_pos, abs(89 - abs(pb.x_pos))))
            else null
        end as shot_angle,

        -- Event outcome flags
        case
            when pb.description = 'goal' then true
            else false
        end as is_goal,

        case
            when pb.description like 'penalty%' then true
            else false
        end as is_penalty,

        -- Penalty details
        case
            when pb.description like 'penalty%'
            then pb.penalty_reason
            else null
        end as penalty_type,

        null::string as penalty_severity,

        case
            when pb.description like 'penalty%'
            then pb.penalty_duration
            else null
        end as penalty_minutes,

        -- Game situation
        pb.situation_code as strength_code,

        -- Parse strength from situation code
        case
            when pb.situation_code = '1' then 'Even'
            when pb.situation_code in ('2', '3') then 'PP'
            when pb.situation_code in ('4', '5') then 'SH'
            else 'Other'
        end as strength_state,

        -- Special situations
        false as is_empty_net,
        case
            when pb.situation_code in ('2', '3') then true
            else false
        end as is_powerplay,
        case
            when pb.situation_code in ('4', '5') then true
            else false
        end as is_shorthanded,

        -- Metadata
        pb.home_side,
        pb.game_id,
        case
            when pb.play_team_id = pb.home_id then pb.home_abv
            when pb.play_team_id = pb.away_id then pb.away_abv
            else null
        end as play_team_abv

    from plays_base pb
    left join {{ ref('fct_games') }} fg
        on pb.game_id = fg.game_id
    left join {{ ref('dim_teams') }} dt
        on dt.team_abv = case
            when pb.play_team_id = pb.home_id then pb.home_abv
            when pb.play_team_id = pb.away_id then pb.away_abv
            else null
        end
)

select
    play_key,
    game_key,
    date_key,
    season_key,
    event_idx,
    period_number,
    period_type,
    time_elapsed_seconds,
    game_elapsed_seconds,
    time_in_period,
    time_remaining,
    event_type_id,
    event_type_name,
    event_team_key,
    primary_player_key,
    secondary_player_key,
    x_coordinate,
    y_coordinate,
    zone_code,
    shot_type,
    shot_distance,
    shot_angle,
    is_goal,
    is_penalty,
    penalty_type,
    penalty_severity,
    penalty_minutes,
    strength_code,
    strength_state,
    is_empty_net,
    is_powerplay,
    is_shorthanded,
    -- Derived fields
    case
        when zone_code = 'O' then 'Offensive'
        when zone_code = 'D' then 'Defensive'
        when zone_code = 'N' then 'Neutral'
        else 'Unknown'
    end as zone_name,
    case
        when shot_distance <= 10 then 'Close'
        when shot_distance <= 25 then 'Medium'
        when shot_distance > 25 then 'Long'
        else null
    end as shot_distance_category,
    case
        when period_number <= 3 then 'Regulation'
        when period_number = 4 then 'Overtime'
        when period_number = 5 then 'Shootout'
        else 'Unknown'
    end as period_category
from play_facts
order by
    game_key,
    event_idx
