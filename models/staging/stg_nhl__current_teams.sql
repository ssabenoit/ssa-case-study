with

source as (

    select * from {{ source('nhl_staging_data', 'current_teams') }}

)

select * from source