{{ config(materialized='table') }}

-- models/dimensional/dimensions/dim_player_team_bridge.sql
-- Bridge table to handle player-team relationships over time (trades, signings, etc.)

with

-- Get all player-team-season combinations from game stats
skater_team_seasons as (
    select distinct
        player_id,
        team_abv,
        season,
        min(game_id) as first_game_id,
        max(game_id) as last_game_id,
        count(distinct game_id) as games_played
    from {{ ref('int__skaters_per_game_stats') }}
    group by player_id, team_abv, season
),

goalie_team_seasons as (
    select distinct
        player_id,
        team_abv,
        season,
        min(game_id) as first_game_id,
        max(game_id) as last_game_id,
        count(distinct game_id) as games_played
    from {{ ref('int__goalies_per_game_stats') }}
    group by player_id, team_abv, season
),

-- Combine skaters and goalies
all_player_team_seasons as (
    select * from skater_team_seasons
    union all
    select * from goalie_team_seasons
),

-- Get game dates for start and end dates
player_team_dates as (
    select
        pts.player_id,
        pts.team_abv,
        pts.season,
        pts.games_played,
        fg.date as start_date,
        lg.date as end_date
    from all_player_team_seasons pts
    left join {{ ref('int__all_games') }} fg
        on pts.first_game_id = fg.id
    left join {{ ref('int__all_games') }} lg
        on pts.last_game_id = lg.id
),

-- Get player information for each stint
player_info_per_stint as (
    select
        ptd.*,
        p.position,
        p.number as jersey_number
    from player_team_dates ptd
    left join {{ ref('all_players') }} p
        on ptd.player_id = p.player_id
        and ptd.team_abv = p.team_abv
),

-- Identify mid-season trades (player switches teams within a season)
player_trades as (
    select
        player_id,
        season,
        count(distinct team_abv) as teams_in_season,
        case 
            when count(distinct team_abv) > 1 then true
            else false
        end as was_traded
    from player_info_per_stint
    group by player_id, season
),

-- Add acquisition type logic
player_team_bridge_base as (
    select
        pis.*,
        pt.was_traded,
        pt.teams_in_season,
        -- Determine acquisition type
        case
            when pt.was_traded and row_number() over (
                partition by pis.player_id, pis.season 
                order by pis.start_date
            ) > 1 then 'Trade'
            when lag(pis.team_abv) over (
                partition by pis.player_id 
                order by pis.season, pis.start_date
            ) != pis.team_abv then 'Free Agent'
            when lag(pis.season) over (
                partition by pis.player_id 
                order by pis.season
            ) is null then 'Draft/Entry'
            else 'Retained'
        end as acquisition_type,
        -- Check if this is their current team
        case
            when pis.season = (select max(season) from all_player_team_seasons)
                and pis.end_date = (
                    select max(end_date) 
                    from player_team_dates pd 
                    where pd.player_id = pis.player_id
                )
            then true
            else false
        end as is_current_team
    from player_info_per_stint pis
    left join player_trades pt
        on pis.player_id = pt.player_id
        and pis.season = pt.season
),

final_bridge as (
    select
        row_number() over (order by ptb.player_id, ptb.season, ptb.start_date) as bridge_key,
        dp.player_key,
        dt.team_key,
        ds.season_key,
        ptb.player_id,
        ptb.team_abv,
        ptb.season,
        ptb.start_date,
        ptb.end_date,
        ptb.jersey_number,
        ptb.position as position_code,
        case ptb.position
            when 'C' then 'Center'
            when 'L' then 'Left Wing'
            when 'R' then 'Right Wing'
            when 'D' then 'Defenseman'
            when 'G' then 'Goalie'
            else ptb.position
        end as position_name,
        ptb.acquisition_type,
        ptb.is_current_team,
        ptb.games_played as games_played_for_team,
        ptb.was_traded as traded_mid_season,
        ptb.teams_in_season,
        datediff(day, ptb.start_date, ptb.end_date) + 1 as stint_length_days
    from player_team_bridge_base ptb
    left join {{ ref('dim_players') }} dp
        on ptb.player_id = dp.player_id
    left join {{ ref('dim_teams') }} dt
        on ptb.team_abv = dt.team_abv
    left join {{ ref('dim_seasons') }} ds
        on ptb.season = ds.season_key
)

select
    bridge_key,
    player_key,
    team_key,
    season_key,
    player_id,
    team_abv,
    season,
    start_date,
    end_date,
    jersey_number,
    position_code,
    position_name,
    acquisition_type,
    is_current_team,
    games_played_for_team,
    traded_mid_season,
    teams_in_season,
    stint_length_days,
    -- Add useful derived fields
    case
        when games_played_for_team >= 41 then 'Full Season'
        when games_played_for_team >= 20 then 'Significant'
        when games_played_for_team >= 10 then 'Regular'
        when games_played_for_team >= 1 then 'Limited'
        else 'None'
    end as participation_level
from final_bridge
order by 
    player_key, 
    season_key, 
    start_date