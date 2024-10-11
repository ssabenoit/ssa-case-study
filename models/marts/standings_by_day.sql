-- models/marts/standings_by_day.sql
-- neatly compile the standings for every day in the data

with standings as (
    select *
    from {{ ref('stg_nhl__daily_standings') }}
)

select
    date,
    cast(seasonid as int) as season,
    teamname:default::STRING as team_name,
    teamabbrev:default::STRING as team_abv,
    cast(gamesplayed as int) as games_played,
    cast(points as int) as points,
    cast(wins as int) as wins,
    cast(losses as int) as losses,
    cast(otlosses as int) as ot_losses,
    concat(cast(wins as int), '-', cast(losses as int), '-', cast(otlosses as int)) as record,
    cast(goalfor as int) as goals_for,
    cast(goalagainst as int) as goals_against,
    cast(goalfor as int) - cast(goalagainst as int) as goal_diff,
    divisionname as division,
    conferencename as conference,
    cast(divisionsequence as int) as div_sequence,
    cast(wildcardsequence as int) as wc_sequence,
    cast(conferencesequence as int) as conf_sequence,
    cast(leaguesequence as int) as leaguesequence,
from standings