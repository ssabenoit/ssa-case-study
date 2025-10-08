-- models/staging/stg_nhl__current_standings.sql
-- Standardizes and cleans current standings data from the NHL API

with

source as (
    select * from {{ source('nhl_staging_data', 'current_standings') }}
)

select
    *
from source