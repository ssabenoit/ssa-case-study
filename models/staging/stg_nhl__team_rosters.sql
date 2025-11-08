-- models/staging/stg_nhl__team_rosters.sql
-- Standardizes team roster data from the NHL API

with

source as (
    select *
    from {{ source('nhl_staging_data', 'team_rosters') }}
)

select
    "team_abv" as "team_abv",
    parse_json("forwards") as "forwards",
    parse_json("defensemen") as "defensemen",
    parse_json("goalies") as "goalies",
    "_etl_loaded_at" as "_etl_loaded_at"
from source
