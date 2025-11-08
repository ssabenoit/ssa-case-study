-- models/staging/stg_nhl__current_teams.sql
-- Standardizes and cleans raw team data from the NHL API

with

source as (
    select * from {{ source('nhl_staging_data', 'current_teams') }}
),

renamed as (
    select
        "id" as team_id,
        "abbrev" as team_abbrev,
        "name" as team_name,
        "placeName" as team_place_name,
        "commonName" as team_common_name,
        "french" as team_french_name,
        "logo" as team_logo_url
    from source
)

select
    team_id,
    team_abbrev,
    team_name,
    team_place_name,
    team_common_name,
    team_french_name,
    team_logo_url
from renamed