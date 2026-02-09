-- models/staging/stg_nhl__team_rosters.sql
-- Standardizes team roster data from the NHL API

with

source as (
    select *
    from {{ source('nhl_staging_data', 'team_rosters') }}
)

select
    TEAM_ABV as team_abv,
    parse_json(FORWARDS) as forwards,
    parse_json(DEFENSEMEN) as defensemen,
    parse_json(GOALIES) as goalies
from source
qualify row_number() over (partition by TEAM_ABV order by _loaded_at desc) = 1
