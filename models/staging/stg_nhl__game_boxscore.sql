with

source as (

    select * from {{ source('nhl_staging_data', 'game_boxscore') }}

)

select * from source limit 2