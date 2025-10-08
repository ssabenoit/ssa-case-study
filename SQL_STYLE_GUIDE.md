# SQL Style Guide for NHL Analytics Project

This document outlines the SQL formatting and commenting standards for the NHL Analytics dbt project. Following these guidelines will ensure consistency across models and improve maintainability.

## File Structure

Each SQL file should follow this structure:

1. Header comment with file path and description
2. Configuration blocks (if applicable)
3. CTEs (with clause)
4. Main query

## Commenting

### File Headers
Every file must include a header comment with:
- File path
- Brief description of the model's purpose

Example:
```sql
-- models/staging/stg_nhl__current_teams.sql
-- Standardizes and cleans raw team data from the NHL API
```

### CTE Comments
Add comments for complex CTEs that explain their purpose or transformation logic.

Example:
```sql
-- Extract and clean team information from raw JSON
with team_data as (
    ...
)
```

### Inline Comments
Use inline comments to explain complex calculations or business logic.

Example:
```sql
-- Calculate points percentage (pts / max_possible_pts)
points / (games_played * 2.0) as points_pct,
```

## SQL Formatting

### CTEs
- Use lowercase `with` keyword
- Add a blank line after the opening `with`
- Place each CTE on a new line
- End each CTE with a comma
- Add a blank line between CTEs
- Indent CTE contents consistently

Example:
```sql
with

source as (
    select * from {{ source('nhl_staging_data', 'current_teams') }}
),

renamed as (
    select
        id as team_id,
        name as team_name
    from source
)
```

### SELECT Statements
- Align all columns with the SELECT keyword
- One column per line
- Comma at the beginning of each continued line
- Alias as needed using `as` keyword

Example:
```sql
select
    game_id,
    team_id,
    sum(goals) as total_goals,
    avg(shots) as avg_shots
from player_stats
```

### JOIN Statements
- Place each join on a new line
- Specify join type (LEFT JOIN, INNER JOIN, etc.)
- Use descriptive table aliases
- Indent join conditions

Example:
```sql
select
    g.game_id,
    t.team_name
from games g
left join teams t
    on g.team_id = t.team_id
```

### WHERE Clauses
- Place each condition on a new line
- Indent conditions consistently
- Use parentheses for complex conditions

Example:
```sql
select
    player_id,
    points
from player_stats
where
    season = '20232024'
    and points > 0
```

### CASE Statements
- Align WHEN, THEN, ELSE, and END keywords
- Indent WHEN and THEN conditions

Example:
```sql
case
    when goals > 0 then 'Scorer'
    when assists > 0 then 'Playmaker'
    else 'Defensive'
end as player_type
```

## dbt Reference Style

### Sources
Use consistent formatting for source references:

```sql
{{ source('nhl_staging_data', 'current_teams') }}
```

### Model References
Use consistent formatting for model references:

```sql
{{ ref('int__all_skaters') }}
```

## Configuration

Place config blocks at the top of the file, before any comments or SQL:

```sql
{{ config(
    materialized='table',
    sort='game_date',
    dist='team_id'
) }}
```

## Additional Guidelines

1. Use lowercase for all SQL keywords (SELECT, FROM, WHERE, etc.)
2. Use meaningful table aliases
3. Avoid commented-out code (remove or place in a documented testing area)
4. Add explicit datatype casts when needed for clarity
5. Be consistent with quotes (prefer single quotes for string literals)
6. Use meaningful column names that reflect the data content

By following these guidelines, we'll maintain a consistent, readable codebase that is easier to maintain and enhance over time.