{{ config(materialized='table') }}

-- models/dimensional/dimensions/dim_dates.sql
-- Date dimension table with NHL-specific attributes

with

date_spine as (
    select
        dateadd(day, seq4(), '2020-01-01'::date) as date
    from table(generator(rowcount => 3650))  -- 10 years of dates
),

dates_with_attributes as (
    select
        date,
        to_number(to_char(date, 'YYYYMMDD')) as date_key,
        extract(year from date) as year,
        extract(month from date) as month,
        extract(day from date) as day,
        extract(dayofweek from date) as day_of_week_num,
        dayname(date) as day_name,
        extract(week from date) as week_of_year,
        monthname(date) as month_name,
        extract(quarter from date) as quarter,
        case 
            when day_of_week_num in (0, 6) then true 
            else false 
        end as is_weekend,
        -- NHL season determination (Oct-June typically)
        case
            when month >= 10 then year || to_char(year + 1, 'FM0000')
            when month <= 6 then to_char(year - 1, 'FM0000') || year
            else null
        end::int as nhl_season,
        -- Season phase determination
        case
            when month in (7, 8, 9) then 'offseason'
            when month in (10, 11, 12, 1, 2, 3) then 'regular_season'
            when month in (4, 5, 6) then 'playoff_period'
            else 'transition'
        end as season_phase
    from date_spine
),

season_boundaries as (
    select
        season,
        start_date as season_start_date,
        end_date as season_end_date
    from {{ ref('season_dates') }}
),

dates_with_season_info as (
    select
        d.*,
        sb.season_start_date,
        sb.season_end_date,
        case
            when d.date >= sb.season_start_date 
                and d.date <= sb.season_end_date 
            then true 
            else false 
        end as is_in_season,
        case
            when d.date >= sb.season_start_date 
                and d.date <= sb.season_end_date 
            then datediff(day, sb.season_start_date, d.date)
            else null
        end as days_from_season_start,
        case
            when d.date >= sb.season_start_date 
                and d.date <= sb.season_end_date 
            then datediff(day, d.date, sb.season_end_date)
            else null
        end as days_to_season_end
    from dates_with_attributes d
    left join season_boundaries sb
        on d.nhl_season = sb.season
)

select
    date_key,
    date,
    year,
    month,
    day,
    day_of_week_num,
    day_name,
    week_of_year,
    month_name,
    quarter,
    is_weekend,
    nhl_season,
    season_phase,
    is_in_season,
    days_from_season_start,
    days_to_season_end,
    -- Additional useful flags
    case 
        when month = 12 and day >= 24 and day <= 26 then true  -- Christmas break
        when month = 2 and week_of_year in (7, 8) then true  -- All-Star break (approximate)
        else false
    end as is_holiday_period,
    case
        when day_name in ('Friday', 'Saturday') then true
        when day_name = 'Thursday' and month in (10, 11, 12, 1, 2, 3) then true  -- TNT Thursday
        else false
    end as is_prime_game_day
from dates_with_season_info
where date >= '2015-10-01' and date <= current_date() + 365
order by date_key