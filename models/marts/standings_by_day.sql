-- models/marts/standings_by_day.sql
-- Presentation view of the gap-filled daily standings. The forward-fill
-- engine lives in int__standings_by_day (single source of truth); this mart
-- adds the display-season string and now exposes the home/road/L10/streak
-- splits that used to be dropped.

select
    *,
    {{ season_display('season') }} as season_display
from {{ ref('int__standings_by_day') }}
