-- models/analytics/mart_player_similarity.sql
-- "Players like X": nearest current-era comparables by cosine similarity
-- over normalized per-game production vectors at the same age.
-- Grain: top 5 comparables per (player, season) for recent seasons.

with vectors as (
    select
        season,
        player_id,
        full_name,
        position,
        season_age,
        points_per_game,
        goals_per_game,
        toi_minutes_per_game
    from {{ ref('mart_player_seasons') }}
    where games_played >= 30
        and season_age is not null
),

-- z-normalize within position so vectors are comparable
normalized as (
    select
        *,
        (points_per_game - avg(points_per_game) over (partition by position))
            / nullif(stddev(points_per_game) over (partition by position), 0) as z_ppg,
        (goals_per_game - avg(goals_per_game) over (partition by position))
            / nullif(stddev(goals_per_game) over (partition by position), 0) as z_gpg,
        (toi_minutes_per_game - avg(toi_minutes_per_game) over (partition by position))
            / nullif(stddev(toi_minutes_per_game) over (partition by position), 0) as z_toi
    from vectors
),

recent as (
    select * from normalized where season >= 20242025
),

pairs as (
    select
        a.season,
        a.player_id,
        a.full_name,
        b.player_id as comp_player_id,
        b.full_name as comp_full_name,
        b.season as comp_season,
        -- euclidean distance in z-space at matching age/position
        sqrt(power(a.z_ppg - b.z_ppg, 2) + power(a.z_gpg - b.z_gpg, 2)
             + power(a.z_toi - b.z_toi, 2)) as distance
    from recent a
    inner join normalized b
        on b.position = a.position
        and b.season_age = a.season_age
        and not (b.player_id = a.player_id and b.season = a.season)
)

select
    season,
    player_id,
    full_name,
    comp_player_id,
    comp_full_name,
    comp_season,
    {{ season_display('comp_season') }} as comp_season_display,
    round(distance, 3) as similarity_distance,
    row_number() over (partition by season, player_id order by distance) as similarity_rank
from pairs
qualify similarity_rank <= 5
