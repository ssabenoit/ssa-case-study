{{ config(materialized='table') }}

-- models/intermediate/int__season_dates.sql
-- Defines the start and end dates for each season in the dataset

with

days as (
    select distinct 
        season, 
        date
    from {{ ref('int__standings_by_day') }}
)

select
    season,
    min(date) as start_date,
    max(date) as end_date
from days
group by season