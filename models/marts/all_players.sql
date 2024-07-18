-- models/marts/all_players.sql
-- master table with all players for every team (forwards, d-men, and goalies)

with skaters as (
    select *
    from {{ ref("int__all_skaters") }}
),

goalies as (
    select *
    from {{ ref("int__all_goalies") }}
)

select *
from skaters
union all
select *
from goalies