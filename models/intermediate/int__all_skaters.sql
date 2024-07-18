-- models/intermediate/int__all_skaters.sql
-- combine all of the forwards and defensemen into one table

with forwards as (
    select *
    from {{ ref("int__all_forwards") }}
),

defensemen as (
    select *
    from {{ ref("int__all_defensemen")}}
)

select * from forwards
union all
select * from defensemen