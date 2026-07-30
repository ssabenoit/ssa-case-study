-- models/analytics/mart_game_projections.sql
-- Latest win probabilities for scheduled games (most recent run per game).

select
    p.game_id,
    p.game_date,
    p.season,
    {{ season_display('p.season') }} as season_display,
    p.home,
    p.away,
    p.home_elo,
    p.away_elo,
    p.p_home_win,
    1 - p.p_home_win as p_away_win,
    p.run_date as projected_on
from {{ source('nhl_analytics_engine', 'game_projections') }} p
qualify row_number() over (partition by p.game_id order by p.run_date desc) = 1
