-- models/staging/stg_nhl__team_rosters.sql
-- Standardizes team roster data from the NHL API

with

source as (
    select *
    from {{ source('nhl_staging_data', 'team_rosters') }}
)

select * 
from source
