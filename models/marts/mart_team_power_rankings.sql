-- models/marts/mart_team_power_rankings.sql
-- Presentation passthrough: the single source of truth for power rankings is
-- models/dimensional/metrics/team_power_rankings.sql. This view only exists
-- because the Metabase "SSA NHL Power Rankings" card queries this table name.
-- (The former 538-line diverged fork of the ranking logic was retired.)

select *
from {{ ref('team_power_rankings') }}
