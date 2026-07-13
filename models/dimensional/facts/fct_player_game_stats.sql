{{ config(materialized='table') }}

-- models/dimensional/facts/fct_player_game_stats.sql
-- Player (skater) game-level statistics fact table.
-- Grain: one row per (game, player). Boxscore counting stats are enriched
-- with play-by-play-derived detail (real primary/secondary assists, PP/SH
-- assists, SH/OT/empty-net goals, game-winning goals) — none of these are
-- estimated anymore. player_key/team_key are NHL natural ids.

with

skater_stats as (
    select
        s.*,
        lg.game_date as date,
        lg.home_team_abv as home_abv,
        lg.away_team_abv as away_abv,
        lg.last_period_type,
        case
            when s.type = 'home' then lg.away_team_abv
            else lg.home_team_abv
        end as opponent_abv
    from {{ ref('int__skaters_per_game_stats') }} s
    inner join {{ ref('int__league_games') }} lg
        on s.game_id = lg.game_id
    where s.game_type in ('regular', 'playoff')
),

-- Real (non-shootout) goals with attribution, from the plays fact
goal_events as (
    select
        game_key as game_id,
        event_team_key as team_id,
        primary_player_key as scorer_id,
        secondary_player_key as assist1_id,
        tertiary_player_key as assist2_id,
        strength_state,
        is_empty_net,
        period_category,
        game_elapsed_seconds
    from {{ ref('fct_plays') }}
    where event_type_name = 'goal'
        and period_category != 'Shootout'
),

scorer_detail as (
    select
        game_id,
        scorer_id as player_id,
        count_if(strength_state = 'SH') as sh_goals,
        count_if(period_category = 'Overtime') as overtime_goals,
        count_if(is_empty_net) as empty_net_goals
    from goal_events
    group by game_id, scorer_id
),

assist_detail as (
    select
        game_id,
        player_id,
        sum(is_primary) as primary_assists,
        sum(is_secondary) as secondary_assists,
        count_if(strength_state = 'PP') as pp_assists,
        count_if(strength_state = 'SH') as sh_assists
    from (
        select game_id, assist1_id as player_id, 1 as is_primary, 0 as is_secondary, strength_state
        from goal_events
        where assist1_id is not null
        union all
        select game_id, assist2_id as player_id, 0 as is_primary, 1 as is_secondary, strength_state
        from goal_events
        where assist2_id is not null
    )
    group by game_id, player_id
),

-- Game-winning goal: the winning team's (loser_final + 1)th goal.
-- Shootout wins credit no skater GWG, so SO games are excluded.
game_winners as (
    select game_id, team_id, goals_against as losing_team_final_score
    from {{ ref('int__team_per_game_stats') }}
    where goals > goals_against
        and coalesce(last_period_type, 'REG') != 'SO'
),

goal_sequence as (
    select
        game_id,
        team_id,
        scorer_id,
        row_number() over (
            partition by game_id, team_id
            order by game_elapsed_seconds
        ) as team_goal_number
    from goal_events
),

gwg as (
    select
        gs.game_id,
        gs.scorer_id as player_id,
        1 as game_winning_goals
    from goal_sequence gs
    inner join game_winners w
        on w.game_id = gs.game_id
        and w.team_id = gs.team_id
    where gs.team_goal_number = w.losing_team_final_score + 1
),

-- Real faceoff win/loss counts per player-game from play-by-play events
player_faceoffs as (
    select
        game_key as game_id,
        player_id,
        sum(won) as faceoffs_won,
        sum(lost) as faceoffs_lost
    from (
        select game_key, primary_player_key as player_id, 1 as won, 0 as lost
        from {{ ref('fct_plays') }}
        where event_type_name = 'faceoff' and primary_player_key is not null
        union all
        select game_key, secondary_player_key as player_id, 0 as won, 1 as lost
        from {{ ref('fct_plays') }}
        where event_type_name = 'faceoff' and secondary_player_key is not null
    )
    group by game_key, player_id
),

player_game_facts as (
    select
        -- Keys
        {{ dbt_utils.generate_surrogate_key(['ss.game_id', 'ss.player_id']) }} as player_game_key,
        ss.game_id as game_key,
        ss.player_id as player_key,
        dt.team_key as team_key,
        cast(replace(cast(ss.date as string), '-', '') as int) as date_key,
        ss.season as season_key,
        opp.team_key as opponent_team_key,

        -- Game context
        (ss.type = 'home') as is_home_game,

        -- Player info for this game
        coalesce(ss.position, dp.primary_position_code) as position_played,
        dp.jersey_number,

        -- Offensive stats
        ss.goals,
        ss.assists,
        coalesce(ad.primary_assists, 0) as primary_assists,
        coalesce(ad.secondary_assists, 0) as secondary_assists,
        ss.points,
        ss.shots,
        case
            when ss.shots > 0 then cast(ss.goals as float) / ss.shots
            else 0
        end as shooting_pct,
        ss.pp_goals,
        coalesce(ad.pp_assists, 0) as pp_assists,

        -- Defensive stats
        ss.plus_minus,
        ss.hits,
        ss.blocks,
        ss.takeaways,
        ss.giveaways,

        ss.faceoff_pct,
        coalesce(pf.faceoffs_won, 0) as faceoffs_won,
        coalesce(pf.faceoffs_lost, 0) as faceoffs_lost,

        -- Penalty stats
        ss.pim as penalty_minutes,

        -- Time on ice (toi is already in seconds)
        ss.toi as time_on_ice_seconds,

        -- Play-by-play-derived special goals
        coalesce(sd.sh_goals, 0) as sh_goals,
        coalesce(ad.sh_assists, 0) as sh_assists,
        coalesce(sd.empty_net_goals, 0) as empty_net_goals,
        coalesce(g.game_winning_goals, 0) as game_winning_goals,
        coalesce(sd.overtime_goals, 0) as overtime_goals,

        -- Additional metadata
        ss.shifts,
        ss.game_type,
        ss.player_id,
        ss.game_id,
        ss.team_abv,
        ss.name as player_name

    from skater_stats ss
    left join {{ ref('dim_players') }} dp
        on ss.player_id = dp.player_id
    left join {{ ref('dim_teams') }} dt
        on ss.team_abv = dt.team_abv
    left join {{ ref('dim_teams') }} opp
        on ss.opponent_abv = opp.team_abv
    left join scorer_detail sd
        on sd.game_id = ss.game_id and sd.player_id = ss.player_id
    left join assist_detail ad
        on ad.game_id = ss.game_id and ad.player_id = ss.player_id
    left join gwg g
        on g.game_id = ss.game_id and g.player_id = ss.player_id
    left join player_faceoffs pf
        on pf.game_id = ss.game_id and pf.player_id = ss.player_id
)

select
    player_game_key,
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
    faceoff_pct,
    faceoffs_won,
    faceoffs_lost,
    penalty_minutes,
    time_on_ice_seconds,
    pp_goals,
    pp_assists,
    sh_goals,
    sh_assists,
    empty_net_goals,
    game_winning_goals,
    overtime_goals,
    shifts,
    game_type,
    player_id,
    game_id,
    team_abv,
    player_name,
    -- Calculated metrics
    (goals >= 3) as hat_trick,
    (points >= 4) as four_point_game,
    (goals >= 4) as four_goal_game,
    goals + assists as point_contributions
from player_game_facts
order by
    game_key,
    player_key
