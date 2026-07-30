-- Continuous parity check: our derived regular-season skater totals must
-- match the NHL's official aggregates exactly for recent seasons. Any row
-- returned = a divergence between our pipeline and the league's own books.
-- (Older seasons are monitored by assert_official_skater_parity_history at
-- warn severity — early-era feeds have known small quirks.)

with ours as (
    select
        season,
        player_id,
        sum(goals) as goals,
        sum(assists) as assists,
        sum(points) as points,
        sum(game_winning_goals) as gwg
    from {{ ref('skaters_season_stats_regular') }}
    group by season, player_id
),

official as (
    select
        season,
        player_id,
        goals,
        assists,
        points,
        game_winning_goals as gwg
    from {{ ref('stg_nhl__official_summaries') }}
    where game_type_id = 2
)

select
    o.season,
    o.player_id,
    o.goals as our_goals,
    f.goals as official_goals,
    o.assists as our_assists,
    f.assists as official_assists,
    o.gwg as our_gwg,
    f.gwg as official_gwg
from ours o
inner join official f
    on f.season = o.season
    and f.player_id = o.player_id
where o.season >= 20232024
    and (o.goals != f.goals or o.assists != f.assists or o.points != f.points
         or o.gwg != f.gwg)
