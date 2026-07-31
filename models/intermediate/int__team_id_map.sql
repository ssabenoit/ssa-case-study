-- models/intermediate/int__team_id_map.sql
-- Season-aware map from feed team ids to franchise abbreviations. Team ids
-- are NOT stable across seasons for every franchise (Utah: 59 in 2024-25,
-- 68 from 2025-26) — any id-keyed join across feeds must route through this
-- map to land on the franchise's canonical dimension row.

select distinct
    season,
    home_team_id as team_id,
    home_team_abv as team_abv
from {{ ref('int__league_games') }}

union

select distinct
    season,
    away_team_id,
    away_team_abv
from {{ ref('int__league_games') }}
