{{ config(materialized='table') }}

-- models/dimensional/dimensions/dim_players.sql
-- Players dimension table with player attributes.
-- Universe: every player on a current roster PLUS every player who has
-- appeared in a league game (so traded/departed players still resolve —
-- roster-only sourcing used to leave thousands of game rows keyless).
-- player_key is the NHL player id (globally stable natural key).

with

roster_players as (
    select distinct
        player_id,
        first_name,
        last_name,
        position,
        number,
        height,
        weight,
        shoots,
        birth_date,
        birth_city,
        birth_state,
        birth_country,
        headshot_url,
        team_abv
    from {{ ref('int__all_players') }}
),

league_games as (
    select game_id, game_date, season
    from {{ ref('int__league_games') }}
),

-- every appearance in a league game, skaters and goalies alike
game_appearances as (
    select
        s.player_id,
        s.name,
        s.position,
        s.team_abv,
        lg.season,
        lg.game_date
    from {{ ref('int__skaters_per_game_stats') }} s
    inner join league_games lg on lg.game_id = s.game_id

    union all

    select
        g.player_id,
        g.name,
        g.position,
        g.team_abv,
        lg.season,
        lg.game_date
    from {{ ref('int__goalies_per_game_stats') }} g
    inner join league_games lg on lg.game_id = g.game_id
),

game_players as (
    select
        player_id,
        max_by(name, game_date) as last_name_seen,
        max_by(position, game_date) as last_position_seen,
        max_by(team_abv, game_date) as last_team_abv,
        max(game_date) as last_game_date,
        max(season) as last_season
    from game_appearances
    group by player_id
),

latest_season as (
    select max(season) as max_season from league_games
),

-- one roster row per player; prefer the roster stint matching the team they
-- last played for (deterministic tie-break by team_abv)
roster_deduped as (
    select r.*
    from roster_players r
    left join game_players gp on gp.player_id = r.player_id
    qualify row_number() over (
        partition by r.player_id
        order by (r.team_abv = gp.last_team_abv)::int desc, r.team_abv
    ) = 1
),

player_universe as (
    select player_id from roster_deduped
    union
    select player_id from game_players
),

player_stats_summary as (
    select
        player_id,
        count(distinct season) as seasons_played,
        sum(games_played) as career_games
    from {{ ref('int__skaters_season_stats_regular') }}
    group by player_id
),

goalie_stats_summary as (
    select
        player_id,
        count(distinct season) as seasons_played,
        sum(gp) as career_games
    from {{ ref('int__goalies_season_stats_regular') }}
    group by player_id
),

players_with_attributes as (
    select
        u.player_id,
        r.first_name,
        r.last_name,
        -- boxscore names are "F. Lastname"; use them only when no roster row exists
        coalesce(r.first_name || ' ' || r.last_name, gp.last_name_seen) as full_name,
        r.birth_date,
        floor(datediff(day, r.birth_date::date, current_date()) / 365.25)::int as current_age,
        case
            when r.birth_date is not null
            then extract(year from r.birth_date::date) + 18  -- typical draft age
            else null
        end as approximate_draft_year,
        r.birth_city,
        r.birth_state,
        r.birth_country,
        case
            when r.birth_country = 'USA' then 'American'
            when r.birth_country = 'CAN' then 'Canadian'
            when r.birth_country = 'SWE' then 'Swedish'
            when r.birth_country = 'FIN' then 'Finnish'
            when r.birth_country = 'RUS' then 'Russian'
            when r.birth_country = 'CZE' then 'Czech'
            when r.birth_country = 'SVK' then 'Slovak'
            when r.birth_country = 'CHE' then 'Swiss'
            when r.birth_country = 'DEU' then 'German'
            when r.birth_country = 'DNK' then 'Danish'
            when r.birth_country = 'NOR' then 'Norwegian'
            when r.birth_country = 'LVA' then 'Latvian'
            when r.birth_country = 'AUT' then 'Austrian'
            when r.birth_country = 'FRA' then 'French'
            when r.birth_country = 'SVN' then 'Slovenian'
            else r.birth_country
        end as nationality,
        r.height as height_inches,
        r.weight as weight_pounds,
        r.shoots as shoots_catches,
        coalesce(r.position, gp.last_position_seen) as primary_position_code,
        case coalesce(r.position, gp.last_position_seen)
            when 'C' then 'Center'
            when 'L' then 'Left Wing'
            when 'R' then 'Right Wing'
            when 'D' then 'Defenseman'
            when 'G' then 'Goalie'
            else coalesce(r.position, gp.last_position_seen)
        end as primary_position_name,
        case
            when coalesce(r.position, gp.last_position_seen) in ('C', 'L', 'R') then 'Forward'
            when coalesce(r.position, gp.last_position_seen) = 'D' then 'Defenseman'
            when coalesce(r.position, gp.last_position_seen) = 'G' then 'Goalie'
            else 'Unknown'
        end as position_category,
        -- current team: the team they last played a league game for,
        -- falling back to their roster team (deterministic)
        coalesce(gp.last_team_abv, r.team_abv) as current_team_abv,
        r.number as jersey_number,
        r.headshot_url,
        case
            when coalesce(pss.career_games, 0) > 0 or coalesce(gss.career_games, 0) > 0 then true
            else false
        end as has_nhl_experience,
        coalesce(pss.seasons_played, gss.seasons_played, 0) as seasons_played,
        coalesce(pss.career_games, gss.career_games, 0) as career_games,
        coalesce(gp.last_season = ls.max_season, false) as is_active
    from player_universe u
    left join roster_deduped r on r.player_id = u.player_id
    left join game_players gp on gp.player_id = u.player_id
    left join player_stats_summary pss on pss.player_id = u.player_id
    left join goalie_stats_summary gss on gss.player_id = u.player_id
    cross join latest_season ls
)

select
    -- natural key: NHL player ids are globally stable, so joins survive rebuilds
    player_id as player_key,
    player_id,
    first_name,
    last_name,
    full_name,
    birth_date,
    current_age,
    approximate_draft_year as draft_year_estimate,
    birth_city,
    birth_state,
    birth_country,
    nationality,
    height_inches,
    weight_pounds,
    shoots_catches,
    primary_position_code,
    primary_position_name,
    position_category,
    current_team_abv,
    jersey_number,
    is_active,
    has_nhl_experience,
    seasons_played,
    career_games,
    headshot_url,
    case
        when career_games = 0 then 'Prospect'
        when career_games between 1 and 25 then 'Rookie'
        when career_games between 26 and 100 then 'Sophomore'
        when career_games between 101 and 300 then 'Experienced'
        when career_games between 301 and 500 then 'Veteran'
        when career_games > 500 then 'Elite Veteran'
        else 'Unknown'
    end as experience_level
from players_with_attributes
order by player_key
