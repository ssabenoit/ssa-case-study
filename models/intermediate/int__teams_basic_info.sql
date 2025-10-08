-- models/intermediate/int__teams_basic_info.sql
-- Extracts basic team information and identifiers

with

raw_teams as (
    select * 
    from {{ ref("stg_nhl__current_teams") }}
)

select
    team_id::int as team_id,
    team_name::string as team_name,
    team_abbrev::string as team_abv,
    team_french_name::string as french_name,
    team_place_name::string as place_name,
    team_common_name::string as common_name,
    team_french_name::boolean as is_french,
    team_logo_url::string as logo_url
from raw_teams
