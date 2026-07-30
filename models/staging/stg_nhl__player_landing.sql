-- models/staging/stg_nhl__player_landing.sql
-- Player bio, draft, and awards from the landing endpoint.
-- One row per player, latest load wins.

with

source as (
    select *
    from {{ source('nhl_staging_data', 'player_landing') }}
)

select
    PLAYERID::int as player_id,
    FIRSTNAME_DEFAULT::string as first_name,
    LASTNAME_DEFAULT::string as last_name,
    POSITION::string as position,
    HEIGHTININCHES::int as height_inches,
    WEIGHTINPOUNDS::int as weight_pounds,
    BIRTHDATE::date as birth_date,
    BIRTHCITY_DEFAULT::string as birth_city,
    BIRTHCOUNTRY::string as birth_country,
    SHOOTSCATCHES::string as shoots_catches,
    ISACTIVE::boolean as is_active,
    SWEATERNUMBER::int as sweater_number,
    HEADSHOT::string as headshot_url,
    CURRENTTEAMABBREV::string as current_team_abv,
    DRAFTDETAILS_YEAR::int as draft_year,
    DRAFTDETAILS_TEAMABBREV::string as draft_team_abv,
    DRAFTDETAILS_ROUND::int as draft_round,
    DRAFTDETAILS_PICKINROUND::int as draft_pick_in_round,
    DRAFTDETAILS_OVERALLPICK::int as draft_overall_pick,
    AWARDS::string as awards_json,
    CAREERTOTALS_REGULARSEASON_GAMESPLAYED::int as career_regular_games,
    CAREERTOTALS_PLAYOFFS_GAMESPLAYED::int as career_playoff_games,
    _loaded_at
from source
qualify row_number() over (partition by PLAYERID order by _loaded_at desc) = 1
