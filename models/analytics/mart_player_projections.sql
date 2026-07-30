-- models/analytics/mart_player_projections.sql
-- Latest player season projections with bio context.

select
    p.run_date as projected_on,
    p.player_id,
    p.player_name,
    p.team_abv,
    dp.primary_position_code as position,
    dp.current_age,
    p.proj_gp,
    p.proj_goals,
    p.proj_assists,
    p.proj_points,
    dp.headshot_url
from {{ source('nhl_analytics_engine', 'player_projections') }} p
left join {{ ref('dim_players') }} dp
    on dp.player_id = p.player_id
qualify row_number() over (partition by p.player_id order by p.run_date desc) = 1
