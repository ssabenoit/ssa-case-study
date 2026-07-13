{{ config(materialized='table') }}

-- models/intermediate/int__standings_by_day.sql
-- Compiles standings for every day in each season with continuous records
-- for each team: the raw feed only has rows for dates a snapshot was taken,
-- so we scaffold every (season, date, team) and forward-fill.

{% set fill_zero = [
    'games_played', 'points', 'wins', 'losses', 'ot_losses',
    'regulation_wins', 'regulation_plus_ot_wins',
    'goals_for', 'goals_against', 'goal_diff', 'point_pct',
    'home_wins', 'home_losses', 'home_ot_losses',
    'road_wins', 'road_losses', 'road_ot_losses',
    'shootout_wins', 'shootout_losses', 'streak_count',
    'l10_wins', 'l10_losses', 'l10_ot_losses', 'l10_points',
    'l10_goals_for', 'l10_goals_against', 'l10_goal_diff',
] %}
{% set fill_null = ['div_sequence', 'wc_sequence', 'conf_sequence', 'league_sequence', 'streak_code'] %}

with

source_standings as (
    select *
    from {{ ref('stg_nhl__daily_standings') }}
),

date_cte as (
    select
        dateadd(day, seq4(), '2020-01-01'::date) as date
    from table(generator(rowcount => 3650))  -- 10 years of dates
),

parsed_standings as (
    select
        DATE as date,
        cast(SEASONID as int) as season,
        TEAMNAME_DEFAULT::string as team_name,
        TEAMABBREV_DEFAULT::string as team_abv,
        cast(GAMESPLAYED as int) as games_played,
        cast(POINTS as int) as points,
        cast(WINS as int) as wins,
        cast(LOSSES as int) as losses,
        cast(OTLOSSES as int) as ot_losses,
        cast(REGULATIONWINS as int) as regulation_wins,
        cast(REGULATIONPLUSOTWINS as int) as regulation_plus_ot_wins,
        concat(cast(WINS as int), '-', cast(LOSSES as int), '-', cast(OTLOSSES as int)) as record,
        cast(GOALFOR as int) as goals_for,
        cast(GOALAGAINST as int) as goals_against,
        cast(GOALFOR as int) - cast(GOALAGAINST as int) as goal_diff,
        cast(POINTS as float) / nullif(cast(GAMESPLAYED as int) * 2, 0) as point_pct,
        DIVISIONNAME as division,
        CONFERENCENAME as conference,
        cast(DIVISIONSEQUENCE as int) as div_sequence,
        cast(WILDCARDSEQUENCE as int) as wc_sequence,
        cast(CONFERENCESEQUENCE as int) as conf_sequence,
        cast(LEAGUESEQUENCE as int) as league_sequence,
        -- Home/Road/Shootout splits
        cast(HOMEWINS as int) as home_wins,
        cast(HOMELOSSES as int) as home_losses,
        cast(HOMEOTLOSSES as int) as home_ot_losses,
        cast(ROADWINS as int) as road_wins,
        cast(ROADLOSSES as int) as road_losses,
        cast(ROADOTLOSSES as int) as road_ot_losses,
        cast(SHOOTOUTWINS as int) as shootout_wins,
        cast(SHOOTOUTLOSSES as int) as shootout_losses,
        -- Rolling form straight from the source (true last-10-games splits)
        cast(L10WINS as int) as l10_wins,
        cast(L10LOSSES as int) as l10_losses,
        cast(L10OTLOSSES as int) as l10_ot_losses,
        cast(L10POINTS as int) as l10_points,
        cast(L10GOALSFOR as int) as l10_goals_for,
        cast(L10GOALSAGAINST as int) as l10_goals_against,
        cast(L10GOALDIFFERENTIAL as int) as l10_goal_diff,
        -- Streak information
        STREAKCODE as streak_code,
        cast(STREAKCOUNT as int) as streak_count
    from source_standings
),

season_date_range as (
    select
        season,
        min(date) as season_start,
        max(date) as season_end
    from parsed_standings
    group by season
),

teams_per_season as (
    select distinct
        season,
        team_abv,
        team_name,
        division,
        conference
    from parsed_standings
),

date_spine as (
    select
        sdr.season,
        d.date
    from season_date_range sdr
    cross join date_cte d
    where d.date between sdr.season_start and sdr.season_end
),

team_date_scaffold as (
    select
        d.date,
        d.season,
        t.team_abv,
        t.team_name,
        t.division,
        t.conference
    from date_spine d
    inner join teams_per_season t
        on d.season = t.season
),

standings_with_gaps as (
    select
        s.date,
        s.season,
        s.team_abv,
        s.team_name,
        s.division,
        s.conference,
        p.record,
        {% for col in fill_zero + fill_null %}
        p.{{ col }}{{ ',' if not loop.last }}
        {% endfor %}
    from team_date_scaffold s
    left join parsed_standings p
        on s.date = p.date
        and s.team_abv = p.team_abv
        and s.season = p.season
),

-- Forward-fill each metric so every team has a value for every date
filled_standings as (
    select
        date,
        season,
        team_abv,
        team_name,
        division,
        conference,
        coalesce(
            record,
            last_value(record ignore nulls) over (
                partition by season, team_abv order by date
                rows between unbounded preceding and current row
            ),
            '0-0-0'
        ) as record,
        {% for col in fill_zero %}
        coalesce(
            {{ col }},
            last_value({{ col }} ignore nulls) over (
                partition by season, team_abv order by date
                rows between unbounded preceding and current row
            ),
            0
        ) as {{ col }},
        {% endfor %}
        {% for col in fill_null %}
        coalesce(
            {{ col }},
            last_value({{ col }} ignore nulls) over (
                partition by season, team_abv order by date
                rows between unbounded preceding and current row
            )
        ) as {{ col }}{{ ',' if not loop.last }}
        {% endfor %}
    from standings_with_gaps
)

select
    date,
    season,
    team_name,
    team_abv,
    games_played,
    points,
    wins,
    losses,
    ot_losses,
    regulation_wins,
    regulation_plus_ot_wins,
    record,
    goals_for,
    goals_against,
    goal_diff,
    point_pct,
    division,
    conference,
    div_sequence,
    wc_sequence,
    conf_sequence,
    league_sequence,
    home_wins,
    home_losses,
    home_ot_losses,
    road_wins,
    road_losses,
    road_ot_losses,
    shootout_wins,
    shootout_losses,
    l10_wins,
    l10_losses,
    l10_ot_losses,
    l10_points,
    l10_goals_for,
    l10_goals_against,
    l10_goal_diff,
    streak_code,
    streak_count
from filled_standings
order by
    season,
    date,
    team_abv
