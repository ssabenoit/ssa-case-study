-- models/marts/mart_three_stars.sql
-- Three Stars of each league game, with season totals per player.
-- Grain: one row per (game_id, star_number).

with

stars as (
    select ts.*
    from {{ ref('stg_nhl__game_three_stars') }} ts
    where ts.game_id in (select game_id from {{ ref('int__league_games') }})
)

select
    s.game_id,
    s.season,
    {{ season_display('s.season') }} as season_display,
    s.game_date,
    s.star_number,
    s.player_id,
    s.player_name,
    s.team_abv,
    s.position,
    s.goals,
    s.assists,
    s.points,
    s.gaa,
    s.save_pct,
    dp.headshot_url,
    count(*) over (partition by s.season, s.player_id) as star_selections_this_season,
    count_if(s.star_number = 1) over (partition by s.season, s.player_id) as first_star_selections
from stars s
left join {{ ref('dim_players') }} dp
    on dp.player_id = s.player_id
