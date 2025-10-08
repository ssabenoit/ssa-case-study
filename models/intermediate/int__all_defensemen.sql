-- models/intermediate/int__all_defensemen.sql
-- Extracts and flattens all defensemen from team rosters

with

all_defensemen as (
    select 
        team_abv, 
        defensemen
    from {{ ref("stg_nhl__team_rosters") }}
)

select 
    team_abv,
    player.value:id::int as player_id,
    player.value:firstName.default::string as first_name,
    player.value:lastName.default::string as last_name,
    player.value:positionCode::string as position,
    player.value:sweaterNumber::int as number,
    player.value:heightInInches::int as height,
    player.value:weightInPounds::int as weight,
    player.value:shootsCatches::string as shoots,
    player.value:birthDate::string as birthdate,
    player.value:birthCity.default::string as birth_city,
    player.value:birthStateProvince.default::string as birth_state,
    player.value:birthCountry::string as birth_country,
    player.value:headshot::string as headshot_url
from 
    all_defensemen,
    lateral flatten(input => defensemen) player
