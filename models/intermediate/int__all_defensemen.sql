-- models/intermediate/int__all_defensemen.sql

with all_defensemen as (
    select 
        team_abv, 
        defensemen
    from {{ ref("stg_nhl__team_rosters") }}
)


SELECT 
    team_abv,
    player.value: id::INT as player_id,
    player.value: firstName.default::STRING as first_name,
    player.value: lastName.default::STRING as last_name,
    player.value: positionCode::STRING as position,
    player.value: sweaterNumber::INT as number,
    player.value: heightInInches::INT as height,
    player.value: weightInPounds::INT as weight,
    player.value: shootsCatches::STRING as shoots,
    player.value: birthDate::STRING as birthDate,
    player.value: birthCity.default::STRING as birth_city,
    player.value: birthStateProvince.default::STRING as birth_state,
    player.value: birthCountry::STRING as birth_country,
    player.value: headshot::STRING as headshot_url
FROM 
    all_defensemen,
    LATERAL FLATTEN(input => defensemen) player