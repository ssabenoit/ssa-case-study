-- models/intermediate/int__teams_basic_info.sql

with raw_teams as (
    select * 
    from {{ ref("stg_nhl__current_teams") }}
)

select
    id::INT as team_id,
    name:default::STRING as team_name,
    abbrev as team_abv,
    name:fr::STRING as french_name,
    placename:default::STRING as place_name,
    commonname:default::STRING as common_name,
    french as is_french,
    logo as logo_url
from 
    raw_teams
