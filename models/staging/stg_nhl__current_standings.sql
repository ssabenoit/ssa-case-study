with

source as (

    select * from {{ source('nhl_staging_data', 'current_standings') }}

)

select * from source