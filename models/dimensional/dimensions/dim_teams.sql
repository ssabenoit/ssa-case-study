{{ config(materialized='table') }}

-- models/dimensional/dimensions/dim_teams.sql
-- Teams dimension table with team attributes and metadata.
-- Static attributes (colors, arenas, franchise history) live in seeds.
-- team_key is the NHL team id (globally stable natural key).

with

current_teams as (
    select
        team_id,
        team_name,
        team_abv,
        place_name,
        common_name,
        logo_url
    from {{ ref('int__teams_basic_info') }}
),

-- Franchises that appear in loaded seasons but no longer exist (e.g. ARI in
-- 2023-24). Identity comes from the games feed; display fields from the
-- standings of the season(s) they played.
historical_teams as (
    select
        ids.team_id,
        st.team_name,
        ids.team_abv,
        st.place_name,
        st.common_name,
        st.logo_url
    from (
        select AWAYTEAM_ABBREV::string as team_abv, AWAYTEAM_ID::int as team_id
        from {{ ref('stg_nhl__games') }}
        union
        select HOMETEAM_ABBREV::string, HOMETEAM_ID::int
        from {{ ref('stg_nhl__games') }}
    ) ids
    inner join (
        select
            TEAMABBREV_DEFAULT::string as team_abv,
            TEAMNAME_DEFAULT::string as team_name,
            PLACENAME_DEFAULT::string as place_name,
            TEAMCOMMONNAME_DEFAULT::string as common_name,
            TEAMLOGO::string as logo_url
        from {{ ref('stg_nhl__daily_standings') }}
        qualify row_number() over (partition by TEAMABBREV_DEFAULT order by DATE desc) = 1
    ) st
        on st.team_abv = ids.team_abv
    where ids.team_abv not in (select team_abv from current_teams)
),

teams_base as (
    select *, true as is_current_franchise from current_teams
    union all
    select *, false as is_current_franchise from historical_teams
),

-- Division/conference from the most recent standings the team appears in
-- (works for both active and defunct franchises)
current_division_conference as (
    select
        TEAMABBREV_DEFAULT::string as team_abv,
        DIVISIONNAME::string as division,
        DIVISIONABBREV::string as division_abv,
        CONFERENCENAME::string as conference,
        CONFERENCEABBREV::string as conference_abv
    from {{ ref('stg_nhl__daily_standings') }}
    qualify row_number() over (partition by TEAMABBREV_DEFAULT order by DATE desc) = 1
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
    t.is_current_franchise as is_active
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
