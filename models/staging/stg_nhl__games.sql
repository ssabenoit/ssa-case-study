-- models/staging/stg_nhl__games.sql
-- Standardizes game data from the NHL API

with

source as (
    select * 
    from {{ source('nhl_staging_data', 'games') }}
)

select * 
from source
