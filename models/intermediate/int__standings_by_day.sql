{{ config(materialized='table') }}

-- models/intermediate/int__standings_by_day.sql
-- Compiles standings for every day in each season with continuous records for each team

with

-- Get the actual standings data from source
source_standings as (
    select *
    from {{ ref('stg_nhl__daily_standings') }}
),

date_cte as (
    select
        dateadd(day, seq4(), '2020-01-01'::date) as date
    from table(generator(rowcount => 3650))  -- 10 years of dates
),

-- Parse the source standings into our format
parsed_standings as (
    select
        date,
        cast(seasonid as int) as season,
        teamname:default::string as team_name,
        teamabbrev:default::string as team_abv,
        cast(gamesplayed as int) as games_played,
        cast(points as int) as points,
        cast(wins as int) as wins,
        cast(losses as int) as losses,
        cast(otlosses as int) as ot_losses,
        concat(cast(wins as int), '-', cast(losses as int), '-', cast(otlosses as int)) as record,
        cast(goalfor as int) as goals_for,
        cast(goalagainst as int) as goals_against,
        cast(goalfor as int) - cast(goalagainst as int) as goal_diff,
        cast(points as float) / nullif(cast(gamesplayed as int) * 2, 0) as point_pct,
        divisionname as division,
        conferencename as conference,
        cast(divisionsequence as int) as div_sequence,
        cast(wildcardsequence as int) as wc_sequence,
        cast(conferencesequence as int) as conf_sequence,
        cast(leaguesequence as int) as league_sequence
    from source_standings
),

-- Get the date range for each season (use actual NHL season dates)
season_date_range as (
    select
        season,
        -- NHL seasons typically start in October and end in June
        -- Use a more reliable method to determine season boundaries
        min(date) as season_start,
        max(date) as season_end
    from parsed_standings
    group by season
),

-- Get all unique teams per season (ensure we have all 32 NHL teams)
teams_per_season as (
    select distinct
        season,
        team_abv,
        team_name,
        division,
        conference
    from parsed_standings
),

-- Generate a date spine for each season
date_spine as (
    select season
    , d.date
    from season_date_range sdr
    cross join date_cte d
    where d.date between sdr.season_start and sdr.season_end
)
,

-- Cross join to get every team for every date in each season
team_date_scaffold as (
    select
        d.date,
        d.season,
        t.team_abv,
        t.team_name,
        t.division,
        t.conference
    from date_spine d
    cross join teams_per_season t
    where d.season = t.season
),

-- Join with actual standings data
standings_with_gaps as (
    select
        s.date,
        s.season,
        s.team_abv,
        s.team_name,
        s.division,
        s.conference,
        p.games_played,
        p.points,
        p.wins,
        p.losses,
        p.ot_losses,
        p.record,
        p.goals_for,
        p.goals_against,
        p.goal_diff,
        p.point_pct,
        p.div_sequence,
        p.wc_sequence,
        p.conf_sequence,
        p.league_sequence
    from team_date_scaffold s
    left join parsed_standings p
        on s.date = p.date
        and s.team_abv = p.team_abv
        and s.season = p.season
),

-- Fill forward the standings data for days without games
filled_standings as (
    select
        date,
        season,
        team_abv,
        team_name,
        division,
        conference,
        -- Use last_value with ignore nulls to carry forward the last known value
        coalesce(
            games_played,
            last_value(games_played ignore nulls) over (
                partition by season, team_abv 
                order by date 
                rows between unbounded preceding and current row
            ),
            0
        ) as games_played,
        coalesce(
            points,
            last_value(points ignore nulls) over (
                partition by season, team_abv 
                order by date 
                rows between unbounded preceding and current row
            ),
            0
        ) as points,
        coalesce(
            wins,
            last_value(wins ignore nulls) over (
                partition by season, team_abv 
                order by date 
                rows between unbounded preceding and current row
            ),
            0
        ) as wins,
        coalesce(
            losses,
            last_value(losses ignore nulls) over (
                partition by season, team_abv 
                order by date 
                rows between unbounded preceding and current row
            ),
            0
        ) as losses,
        coalesce(
            ot_losses,
            last_value(ot_losses ignore nulls) over (
                partition by season, team_abv 
                order by date 
                rows between unbounded preceding and current row
            ),
            0
        ) as ot_losses,
        coalesce(
            record,
            last_value(record ignore nulls) over (
                partition by season, team_abv 
                order by date 
                rows between unbounded preceding and current row
            ),
            '0-0-0'
        ) as record,
        coalesce(
            goals_for,
            last_value(goals_for ignore nulls) over (
                partition by season, team_abv 
                order by date 
                rows between unbounded preceding and current row
            ),
            0
        ) as goals_for,
        coalesce(
            goals_against,
            last_value(goals_against ignore nulls) over (
                partition by season, team_abv 
                order by date 
                rows between unbounded preceding and current row
            ),
            0
        ) as goals_against,
        coalesce(
            goal_diff,
            last_value(goal_diff ignore nulls) over (
                partition by season, team_abv 
                order by date 
                rows between unbounded preceding and current row
            ),
            0
        ) as goal_diff,
        coalesce(
            point_pct,
            last_value(point_pct ignore nulls) over (
                partition by season, team_abv 
                order by date 
                rows between unbounded preceding and current row
            ),
            0
        ) as point_pct,
        coalesce(
            div_sequence,
            last_value(div_sequence ignore nulls) over (
                partition by season, team_abv 
                order by date 
                rows between unbounded preceding and current row
            )
        ) as div_sequence,
        coalesce(
            wc_sequence,
            last_value(wc_sequence ignore nulls) over (
                partition by season, team_abv 
                order by date 
                rows between unbounded preceding and current row
            )
        ) as wc_sequence,
        coalesce(
            conf_sequence,
            last_value(conf_sequence ignore nulls) over (
                partition by season, team_abv 
                order by date 
                rows between unbounded preceding and current row
            )
        ) as conf_sequence,
        coalesce(
            league_sequence,
            last_value(league_sequence ignore nulls) over (
                partition by season, team_abv 
                order by date 
                rows between unbounded preceding and current row
            )
        ) as league_sequence
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
    league_sequence
from filled_standings
order by 
    season,
    date,
    team_abv