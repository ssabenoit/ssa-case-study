-- models/staging/stg_nhl__current_teams.sql
-- Standardizes and cleans raw team data from the NHL API

with

source as (
    select * from {{ source('nhl_staging_data', 'current_teams') }}
),

renamed as (
    select
        ID as team_id,
        ABBREV as team_abbrev,
        NAME_DEFAULT as team_name,
        PLACENAME_DEFAULT as team_place_name,
        COMMONNAME_DEFAULT as team_common_name,
        FRENCH as team_french_name,
        LOGO as team_logo_url,
        _loaded_at
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
qualify row_number() over (partition by team_id order by _loaded_at desc) = 1
