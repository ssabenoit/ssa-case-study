{{ config(materialized='table') }}

-- models/dimensional/dimensions/dim_teams.sql
-- Teams dimension table with team attributes and metadata

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
    -- Manually defined team colors for visualization
    select * from (
        values
            ('ANA', '#F47A38', '#B9975B'),
            ('BOS', '#FFB81C', '#000000'),
            ('BUF', '#002654', '#FCB514'),
            ('CGY', '#C8102E', '#F1BE48'),
            ('CAR', '#CE1126', '#000000'),
            ('CHI', '#CF0A2C', '#000000'),
            ('COL', '#6F263D', '#236192'),
            ('CBJ', '#002654', '#CE1126'),
            ('DAL', '#006847', '#8F8F8C'),
            ('DET', '#CE1126', '#FFFFFF'),
            ('EDM', '#FF4C00', '#041E42'),
            ('FLA', '#041E42', '#C8102E'),
            ('LAK', '#111111', '#A2AAAD'),
            ('MIN', '#A6192E', '#154734'),
            ('MTL', '#AF1E2D', '#192168'),
            ('NSH', '#FFB81C', '#041E42'),
            ('NJD', '#CE1126', '#000000'),
            ('NYI', '#00539B', '#F47D30'),
            ('NYR', '#0038A8', '#CE1126'),
            ('OTT', '#C52032', '#C2912C'),
            ('PHI', '#F74902', '#000000'),
            ('PIT', '#000000', '#FCB514'),
            ('SJS', '#006D75', '#EA7200'),
            ('SEA', '#001628', '#99D9D9'),
            ('STL', '#002F87', '#FCB514'),
            ('TBL', '#002868', '#FFFFFF'),
            ('TOR', '#00205B', '#FFFFFF'),
            ('VAN', '#00205B', '#00843D'),
            ('VGK', '#B4975A', '#333F42'),
            ('WSH', '#C8102E', '#041E42'),
            ('WPG', '#041E42', '#004C97')
    ) as t(team_abv, primary_color, secondary_color)
),

team_arenas as (
    -- Arena information for teams
    select * from (
        values
            ('ANA', 'Honda Center', 17174, 'Anaheim', 'CA'),
            ('BOS', 'TD Garden', 17565, 'Boston', 'MA'),
            ('BUF', 'KeyBank Center', 19070, 'Buffalo', 'NY'),
            ('CGY', 'Scotiabank Saddledome', 19289, 'Calgary', 'AB'),
            ('CAR', 'PNC Arena', 18680, 'Raleigh', 'NC'),
            ('CHI', 'United Center', 19717, 'Chicago', 'IL'),
            ('COL', 'Ball Arena', 18007, 'Denver', 'CO'),
            ('CBJ', 'Nationwide Arena', 18500, 'Columbus', 'OH'),
            ('DAL', 'American Airlines Center', 18532, 'Dallas', 'TX'),
            ('DET', 'Little Caesars Arena', 19515, 'Detroit', 'MI'),
            ('EDM', 'Rogers Place', 18347, 'Edmonton', 'AB'),
            ('FLA', 'FLA Live Arena', 19250, 'Sunrise', 'FL'),
            ('LAK', 'Crypto.com Arena', 18230, 'Los Angeles', 'CA'),
            ('MIN', 'Xcel Energy Center', 17954, 'St. Paul', 'MN'),
            ('MTL', 'Bell Centre', 21302, 'Montreal', 'QC'),
            ('NSH', 'Bridgestone Arena', 17113, 'Nashville', 'TN'),
            ('NJD', 'Prudential Center', 16514, 'Newark', 'NJ'),
            ('NYI', 'UBS Arena', 17113, 'Elmont', 'NY'),
            ('NYR', 'Madison Square Garden', 18006, 'New York', 'NY'),
            ('OTT', 'Canadian Tire Centre', 18652, 'Ottawa', 'ON'),
            ('PHI', 'Wells Fargo Center', 19543, 'Philadelphia', 'PA'),
            ('PIT', 'PPG Paints Arena', 18387, 'Pittsburgh', 'PA'),
            ('SJS', 'SAP Center', 17562, 'San Jose', 'CA'),
            ('SEA', 'Climate Pledge Arena', 17100, 'Seattle', 'WA'),
            ('STL', 'Enterprise Center', 18096, 'St. Louis', 'MO'),
            ('TBL', 'Amalie Arena', 19092, 'Tampa', 'FL'),
            ('TOR', 'Scotiabank Arena', 18819, 'Toronto', 'ON'),
            ('VAN', 'Rogers Arena', 18910, 'Vancouver', 'BC'),
            ('VGK', 'T-Mobile Arena', 17500, 'Las Vegas', 'NV'),
            ('WSH', 'Capital One Arena', 18573, 'Washington', 'DC'),
            ('WPG', 'Canada Life Centre', 15321, 'Winnipeg', 'MB')
    ) as t(team_abv, arena_name, arena_capacity, city, state_province)
),

team_history as (
    -- Team founding years and historical info
    select * from (
        values
            ('ANA', 1993, 'Mighty Ducks of Anaheim', 2006),
            ('BOS', 1924, null, 1924),
            ('BUF', 1970, null, 1970),
            ('CGY', 1972, 'Atlanta Flames', 1980),
            ('CAR', 1972, 'Hartford Whalers', 1997),
            ('CHI', 1926, null, 1926),
            ('COL', 1972, 'Quebec Nordiques', 1995),
            ('CBJ', 2000, null, 2000),
            ('DAL', 1967, 'Minnesota North Stars', 1993),
            ('DET', 1926, null, 1926),
            ('EDM', 1972, null, 1979),
            ('FLA', 1993, null, 1993),
            ('LAK', 1967, null, 1967),
            ('MIN', 2000, null, 2000),
            ('MTL', 1909, null, 1917),
            ('NSH', 1998, null, 1998),
            ('NJD', 1974, 'Kansas City Scouts/Colorado Rockies', 1982),
            ('NYI', 1972, null, 1972),
            ('NYR', 1926, null, 1926),
            ('OTT', 1992, null, 1992),
            ('PHI', 1967, null, 1967),
            ('PIT', 1967, null, 1967),
            ('SJS', 1991, null, 1991),
            ('SEA', 2021, null, 2021),
            ('STL', 1967, null, 1967),
            ('TBL', 1992, null, 1992),
            ('TOR', 1917, null, 1917),
            ('VAN', 1970, null, 1970),
            ('VGK', 2017, null, 2017),
            ('WSH', 1974, null, 1974),
            ('WPG', 1999, 'Atlanta Thrashers', 2011)
    ) as t(team_abv, founded_year, previous_name, current_location_since)
)

select
    row_number() over (order by t.team_id) as team_key,
    t.team_id,
    t.team_abv,
    parse_json(t.team_name):default::string as team_name,
    parse_json(t.place_name):default::string as team_location,
    parse_json(t.common_name):default::string as common_name,
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
    case
        when t.team_abv in ('ANA', 'BOS', 'BUF', 'CAR', 'CHI', 'COL', 
                            'DAL', 'DET', 'EDM', 'FLA', 'LAK', 'MIN',
                            'MTL', 'NSH', 'NJD', 'NYI', 'NYR', 'OTT',
                            'PHI', 'PIT', 'SJS', 'STL', 'TBL', 'TOR',
                            'VAN', 'WSH', 'WPG', 'CGY', 'CBJ')
        then true
        else false
    end as is_original_or_relocated,
    case
        when t.team_abv in ('VGK', 'SEA') then true
        else false
    end as is_expansion_team,
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