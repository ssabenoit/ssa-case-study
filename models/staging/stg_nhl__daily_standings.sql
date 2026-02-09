-- models/staging/stg_nhl__daily_standings.sql
-- Standardizes daily standings data from the NHL API

with

source as (
    select * from {{ source('nhl_staging_data', 'daily_standings') }}
)

select
    *
from source
qualify row_number() over (partition by DATE, TEAMABBREV_DEFAULT order by _loaded_at desc) = 1