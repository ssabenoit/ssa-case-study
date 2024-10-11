-- models/staging/stg_nhl__daily_standings.sql

select *
from {{ source("nhl_staging_data", 'daily_standings') }}