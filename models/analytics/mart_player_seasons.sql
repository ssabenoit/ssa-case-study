-- models/analytics/mart_player_seasons.sql
-- Career trajectories: one row per (player, season) with age-at-season and
-- per-game rates — the backbone of development curves and similarity.

with seasons as (
    select
        s.season,
        s.player_id,
        s.name,
        s.team_abv,
        s.games_played,
        s.goals,
        s.assists,
        s.points,
        s.points_per_game,
        s.goals_per_game,
        s.points_per_60,
        s.total_toi,
        s.faceoff_pct
    from {{ ref('skaters_season_stats_regular') }} s
)

select
    se.season,
    {{ season_display('se.season') }} as season_display,
    se.player_id,
    dp.full_name,
    dp.primary_position_code as position,
    -- age on Feb 1 of the season (standard hockey-age convention)
    floor(datediff(day, dp.birth_date::date,
                   to_date(substr(se.season::string, 5, 4) || '-02-01')) / 365.25)::int as season_age,
    dp.draft_year,
    dp.draft_overall_pick,
    se.team_abv,
    se.games_played,
    se.goals,
    se.assists,
    se.points,
    se.points_per_game,
    se.goals_per_game,
    se.points_per_60,
    round(se.total_toi / nullif(se.games_played, 0) / 60.0, 1) as toi_minutes_per_game,
    se.faceoff_pct,
    row_number() over (partition by se.player_id order by se.season) as career_season_number
from seasons se
left join {{ ref('dim_players') }} dp
    on dp.player_id = se.player_id
