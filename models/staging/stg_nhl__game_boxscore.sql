-- models/staging/stg_nhl__game_boxscore.sql
-- Standardizes game box score data from the NHL API

with

source as (
    select * 
    from {{ source('nhl_staging_data', 'game_boxscore') }}
)

select * 
from source
