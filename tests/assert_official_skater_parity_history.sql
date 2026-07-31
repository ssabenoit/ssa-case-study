{{ config(severity='warn') }}

-- Historical parity monitor (warn-only): same comparison as the strict
-- recent-season test, across the full corpus. Early-era feeds carry known
-- small quirks; this keeps them visible without failing builds.

with ours as (
    select
        season,
        player_id,
        sum(goals) as goals,
        sum(assists) as assists,
        sum(points) as points
    from {{ ref('skaters_season_stats_regular') }}
    group by season, player_id
),

official as (
    select season, player_id, goals, assists, points
    from {{ ref('stg_nhl__official_summaries') }}
    where game_type_id = 2
)

select o.season, o.player_id, o.goals, f.goals as official_goals,
       o.assists, f.assists as official_assists
from ours o
inner join official f
    on f.season = o.season and f.player_id = o.player_id
where o.season < 20232024
    and (o.goals != f.goals or o.assists != f.assists or o.points != f.points)
