-- models/intermediate/int__all_skaters.sql
-- Combines all forwards and defensemen into a consolidated skaters table

with

forwards as (
    select * 
    from {{ ref('int__all_forwards') }}
),

defensemen as (
    select *
    from {{ ref('int__all_defensemen') }}
)

select 
    * 
from forwards
union all
select 
    * 
from defensemen