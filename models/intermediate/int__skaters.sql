
select * from {{ ref("stg_nhl__team_rosters") }}

/*
with skaters as (
    select JSON_EXTRACT(defensement, '$') AS defensemen_list
    from {{ ref("stg_nhl__team_rosters") }}
)

select * from skaters
*/