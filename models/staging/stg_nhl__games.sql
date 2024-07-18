with

source as (

    select * from {{ source('nhl_staging_data', 'games') }}

)

select * from source