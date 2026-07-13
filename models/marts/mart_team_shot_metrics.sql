-- models/marts/mart_team_shot_metrics.sql
-- Possession-proxy shot metrics per team-season from play-by-play events:
-- Corsi (all shot attempts), Fenwick (unblocked attempts), and PDO
-- (shooting% + save%, x1000). All-situations and even-strength variants.
-- Note: on a blocked-shot event the NHL credits the BLOCKING team as the
-- event owner, so the attempt is attributed to the opponent here.

with

league_games as (
    select *
    from {{ ref('int__league_games') }}
),

shot_events as (
    select
        p.game_key as game_id,
        p.season_key as season,
        p.event_type_name,
        p.strength_state,
        -- the attempting team: event owner, except blocked shots (owner = blocker)
        case
            when p.event_type_name = 'blocked-shot'
                then case
                    when p.event_team_key = lg.home_team_id then lg.away_team_id
                    else lg.home_team_id
                end
            else p.event_team_key
        end as attempt_team_id,
        lg.home_team_id,
        lg.away_team_id
    from {{ ref('fct_plays') }} p
    inner join league_games lg
        on lg.game_id = p.game_key
    where p.event_type_name in ('shot-on-goal', 'goal', 'missed-shot', 'blocked-shot')
        and p.period_category != 'Shootout'
),

-- attempts for/against per team-game (each event counts for one team and
-- against the other)
attempts_by_team as (
    select
        season,
        game_id,
        team_id,
        count_if(is_for) as attempts_for,
        count_if(not is_for) as attempts_against,
        count_if(is_for and event_type_name != 'blocked-shot') as fenwick_for,
        count_if(not is_for and event_type_name != 'blocked-shot') as fenwick_against,
        count_if(is_for and strength_state = 'Even') as ev_attempts_for,
        count_if(not is_for and strength_state = 'Even') as ev_attempts_against
    from (
        select se.season, se.game_id, t.team_id, se.event_type_name, se.strength_state,
               (se.attempt_team_id = t.team_id) as is_for
        from shot_events se
        inner join (
            select game_id, home_team_id as team_id from league_games
            union all
            select game_id, away_team_id as team_id from league_games
        ) t
            on t.game_id = se.game_id
    )
    group by season, game_id, team_id
),

season_rollup as (
    select
        a.season,
        dt.team_abv,
        dt.team_name,
        count(distinct a.game_id) as games_played,
        sum(a.attempts_for) as shot_attempts_for,
        sum(a.attempts_against) as shot_attempts_against,
        sum(a.fenwick_for) as fenwick_for,
        sum(a.fenwick_against) as fenwick_against,
        sum(a.ev_attempts_for) as ev_shot_attempts_for,
        sum(a.ev_attempts_against) as ev_shot_attempts_against
    from attempts_by_team a
    inner join {{ ref('dim_teams') }} dt
        on dt.team_id = a.team_id
    group by a.season, dt.team_abv, dt.team_name
),

team_pcts as (
    select
        season::string as season,
        team_abv,
        {{ safe_divide('sum(goals)::float', 'sum(shots)') }} as shooting_pct,
        {{ safe_divide('sum(saves)::float', 'sum(shots_against)') }} as save_pct
    from {{ ref('int__team_per_game_stats') }}
    group by season, team_abv
)

select
    r.season,
    {{ season_display('r.season') }} as season_display,
    r.team_abv,
    r.team_name,
    r.games_played,
    r.shot_attempts_for,
    r.shot_attempts_against,
    round({{ safe_divide('r.shot_attempts_for::float', 'r.shot_attempts_for + r.shot_attempts_against') }}, 4) as corsi_pct,
    r.fenwick_for,
    r.fenwick_against,
    round({{ safe_divide('r.fenwick_for::float', 'r.fenwick_for + r.fenwick_against') }}, 4) as fenwick_pct,
    r.ev_shot_attempts_for,
    r.ev_shot_attempts_against,
    round({{ safe_divide('r.ev_shot_attempts_for::float', 'r.ev_shot_attempts_for + r.ev_shot_attempts_against') }}, 4) as ev_corsi_pct,
    round(tp.shooting_pct, 4) as shooting_pct,
    round(tp.save_pct, 4) as save_pct,
    round((coalesce(tp.shooting_pct, 0) + coalesce(tp.save_pct, 0)) * 1000, 1) as pdo
from season_rollup r
left join team_pcts tp
    on tp.season = r.season::string
    and tp.team_abv = r.team_abv
