with source as (

    select *
    from {{ source('nhl_staging_data', 'team_rosters') }}

)

select * 
from source