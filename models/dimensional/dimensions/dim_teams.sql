{{ config(materialized='table') }}

-- models/dimensional/dimensions/dim_teams.sql
-- Teams dimension table with team attributes and metadata.
-- Static attributes (colors, arenas, franchise history) live in seeds.
-- team_key is the NHL team id (globally stable natural key).

with

teams_base as (
    select
        team_id,
        team_name,
        team_abv,
        place_name,
        common_name,
        logo_url
    from {{ ref('int__teams_basic_info') }}
),

current_division_conference as (
    select distinct
        team_abv,
        division,
        division_abv,
        conference,
        conference_abv
    from {{ ref('int__current_standings') }}
),

team_colors as (
    select * from {{ ref('nhl_team_colors') }}
),

team_arenas as (
    select * from {{ ref('nhl_team_arenas') }}
),

team_history as (
    select * from {{ ref('nhl_team_history') }}
)

select
    -- natural key: NHL team ids are globally stable, so joins survive rebuilds
    t.team_id as team_key,
    t.team_id,
    t.team_abv,
    t.team_name::string as team_name,
    t.place_name::string as team_location,
    t.common_name::string as common_name,
    dc.conference,
    dc.conference_abv,
    dc.division,
    dc.division_abv,
    ta.arena_name,
    ta.arena_capacity,
    ta.city as arena_city,
    ta.state_province as arena_state_province,
    th.founded_year,
    th.previous_name,
    th.current_location_since,
    year(current_date())::int - th.founded_year as years_in_league,
    t.logo_url,
    tc.primary_color,
    tc.secondary_color,
    coalesce(th.is_original_or_relocated, false) as is_original_or_relocated,
    coalesce(th.is_expansion_team, false) as is_expansion_team,
    true as is_active  -- All current teams are active
from teams_base t
left join current_division_conference dc
    on t.team_abv = dc.team_abv
left join team_colors tc
    on t.team_abv = tc.team_abv
left join team_arenas ta
    on t.team_abv = ta.team_abv
left join team_history th
    on t.team_abv = th.team_abv
order by team_key
