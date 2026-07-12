{{ config(materialized='table') }}

-- models/dimensional/facts/fct_plays.sql
-- Play-by-play event-level fact table for league games.
-- Grain: one row per (game, event). player_key values are NHL player ids
-- (same natural key as dim_players).

with

plays_base as (
    select
        p.*,
        lg.game_date as date,
        lg.season,
        lg.away_team_id as away_id,
        lg.away_team_abv as away_abv,
        lg.home_team_id as home_id,
        lg.home_team_abv as home_abv
    from {{ ref('stg_nhl__play_by_play') }} p
    inner join {{ ref('int__league_games') }} lg
        on p.game_id = lg.game_id
),

play_facts as (
    select
        -- Keys
        {{ dbt_utils.generate_surrogate_key(['pb.game_id', 'pb.event_id']) }} as play_key,
        pb.game_id as game_key,
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

        -- Players involved: the main actor and their counterpart per event type
        case pb.description
            when 'goal' then pb.scoring_player_id
            when 'shot-on-goal' then pb.shooting_player_id
            when 'missed-shot' then pb.shooting_player_id
            when 'blocked-shot' then pb.blocking_player_id
            when 'faceoff' then pb.faceoff_winner_player_id
            when 'hit' then pb.hitting_player_id
            when 'penalty' then coalesce(pb.committed_by_player_id, pb.served_by_player_id)
            when 'giveaway' then pb.event_player_id
            when 'takeaway' then pb.event_player_id
            when 'failed-shot-attempt' then pb.shooting_player_id
        end as primary_player_key,
        case pb.description
            when 'goal' then pb.assist1_player_id
            when 'shot-on-goal' then pb.goalie_in_net_id
            when 'blocked-shot' then pb.shooting_player_id
            when 'faceoff' then pb.faceoff_loser_player_id
            when 'hit' then pb.hittee_player_id
            when 'penalty' then pb.drawn_by_player_id
        end as secondary_player_key,
        case
            when pb.description = 'goal' then pb.assist2_player_id
        end as tertiary_player_key,
        case
            when pb.description in ('goal', 'shot-on-goal', 'missed-shot') then pb.goalie_in_net_id
        end as goalie_player_key,

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

        case
            when pb.description in ('shot-on-goal', 'goal', 'missed-shot', 'blocked-shot')
                and pb.x_pos is not null and pb.y_pos is not null
            then sqrt(power(abs(89 - abs(pb.x_pos)), 2) + power(pb.y_pos, 2))
            else null
        end as shot_distance,

        case
            when pb.description in ('shot-on-goal', 'goal', 'missed-shot', 'blocked-shot')
                and pb.x_pos is not null and pb.y_pos is not null
            then degrees(atan2(pb.y_pos, abs(89 - abs(pb.x_pos))))
            else null
        end as shot_angle,

        -- Event outcome flags
        (pb.description = 'goal') as is_goal,
        (pb.description like 'penalty%') as is_penalty,

        -- Penalty details
        case
            when pb.description like 'penalty%' then pb.penalty_desc_key
            else null
        end as penalty_type,

        case
            when pb.description not like 'penalty%' then null
            when pb.penalty_type_code = 'MIN' or pb.penalty_duration = 2 then 'Minor'
            when pb.penalty_duration = 4 then 'Double Minor'
            when pb.penalty_type_code = 'MAJ' or pb.penalty_duration = 5 then 'Major'
            when pb.penalty_type_code = 'BEN' then 'Bench Minor'
            when pb.penalty_type_code in ('MIS', 'GMI') or pb.penalty_duration = 10 then 'Misconduct'
            when pb.penalty_type_code = 'MAT' then 'Match'
            when pb.penalty_type_code = 'PS' then 'Penalty Shot'
            else 'Other'
        end as penalty_severity,

        case
            when pb.description like 'penalty%' then pb.penalty_duration
            else null
        end as penalty_minutes,

        -- Game situation. situationCode is a 4-character code read as
        -- [away goalie in net][away skaters][home skaters][home goalie in net],
        -- e.g. 1551 = 5-on-5 both goalies in, 1451 = home PP (away 4 skaters).
        pb.situation_code as strength_code,
        case
            when len(pb.situation_code) != 4 or pb.play_team_id is null then 'Other'
            when own_skaters.n > opp_skaters.n then 'PP'
            when own_skaters.n < opp_skaters.n then 'SH'
            else 'Even'
        end as strength_state,

        -- Empty net relative to the event team: the OPPOSING goalie is out
        case
            when len(pb.situation_code) != 4 or pb.play_team_id is null then false
            when pb.play_team_id = pb.home_id then substr(pb.situation_code, 1, 1) = '0'
            when pb.play_team_id = pb.away_id then substr(pb.situation_code, 4, 1) = '0'
            else false
        end as is_empty_net,

        case
            when len(pb.situation_code) != 4 or pb.play_team_id is null then false
            else own_skaters.n > opp_skaters.n
        end as is_powerplay,
        case
            when len(pb.situation_code) != 4 or pb.play_team_id is null then false
            else own_skaters.n < opp_skaters.n
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
    left join {{ ref('dim_teams') }} dt
        on dt.team_id = pb.play_team_id
    -- skater counts from the event team's perspective
    cross join lateral (
        select case
            when len(pb.situation_code) != 4 then null
            when pb.play_team_id = pb.home_id then substr(pb.situation_code, 3, 1)::int
            else substr(pb.situation_code, 2, 1)::int
        end as n
    ) own_skaters
    cross join lateral (
        select case
            when len(pb.situation_code) != 4 then null
            when pb.play_team_id = pb.home_id then substr(pb.situation_code, 2, 1)::int
            else substr(pb.situation_code, 3, 1)::int
        end as n
    ) opp_skaters
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
    tertiary_player_key,
    goalie_player_key,
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
