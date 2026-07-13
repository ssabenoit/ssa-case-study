-- models/marts/mart_team_form.sql
-- Latest standings snapshot per team per season: record, home/road splits,
-- true last-10, and streak — as of the last date with standings data.
-- Offseason-safe by construction (no current_date lookup), which fixes the
-- "Selected Team Record" dashboard card that went blank between seasons.

with

standings as (
    select *
    from {{ ref('int__standings_by_day') }}
),

latest as (
    select *
    from standings
    qualify row_number() over (
        partition by season, team_abv
        order by date desc
    ) = 1
)

select
    season,
    {{ season_display('season') }} as season_display,
    date as as_of_date,
    team_abv,
    team_name,
    division,
    conference,
    games_played,
    points,
    point_pct,
    wins,
    losses,
    ot_losses,
    regulation_wins,
    regulation_plus_ot_wins as row_wins,
    goals_for,
    goals_against,
    goal_diff,
    div_sequence as division_rank,
    conf_sequence as conference_rank,
    league_sequence as league_rank,
    wc_sequence as wildcard_rank,
    record,
    concat(home_wins, '-', home_losses, '-', home_ot_losses) as home_record,
    concat(road_wins, '-', road_losses, '-', road_ot_losses) as road_record,
    concat(l10_wins, '-', l10_losses, '-', l10_ot_losses) as last_10_record,
    l10_points as last_10_points,
    l10_goals_for as last_10_goals_for,
    l10_goals_against as last_10_goals_against,
    concat(shootout_wins, '-', shootout_losses) as shootout_record,
    case
        when streak_code is not null and streak_count is not null
        then concat(streak_code, streak_count)
    end as streak,
    streak_code,
    streak_count
from latest
