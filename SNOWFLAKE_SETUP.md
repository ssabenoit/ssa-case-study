# NHL Data Extractor → Snowflake Setup

This guide shows how to extract NHL data and load it directly into your Snowflake `staging` schema, matching your dbt sources configuration.

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements_nhl_extractor.txt
```

### 2. Set Up Environment Variables

```bash
# Copy the example file
cp .env.example .env

# Edit .env and add your Snowflake password
# The other values are already set to match your screenshots
```

Or export them directly:

```bash
export SNOWFLAKE_ACCOUNT=jp55454.us-east-2.aws
export SNOWFLAKE_USER=ssabenoit
export SNOWFLAKE_PASSWORD=your_password_here
export SNOWFLAKE_WAREHOUSE=DBT_WH
export SNOWFLAKE_DATABASE=DBT_ANALYTICS
export SNOWFLAKE_SCHEMA=staging
export SNOWFLAKE_ROLE=ACCOUNTADMIN
```

### 3. Run the Extractor

```bash
# Extract last 7 days (default)
python nhl_to_snowflake.py

# Extract specific date range
python nhl_to_snowflake.py --start-date 2024-11-01 --end-date 2024-11-07

# Extract only specific streams (faster for testing)
python nhl_to_snowflake.py --streams games current_standings

# Skip dependent streams (much faster)
python nhl_to_snowflake.py --no-dependent
```

## Table Mapping

The extractor writes to these tables in your `staging` schema (matching `sources.yml`):

| Stream Name | Snowflake Table | Load Strategy | Merge Keys |
|-------------|----------------|---------------|------------|
| `current_standings` | `staging.current_standings` | Truncate | - |
| `current_teams` | `staging.current_teams` | Truncate | - |
| `games` | `staging.games` | Merge (upsert) | `id` |
| `daily_standings` | `staging.daily_standings` | Merge (upsert) | `date`, `team_id` |
| `team_rosters` | `staging.team_rosters` | Truncate | - |
| `season_schedules` | `staging.season_schedules` | Merge (upsert) | `id`, `season` |
| `game_boxscore` | `staging.game_boxscore` | Merge (upsert) | `id` |
| `game_summaries` | `staging.game_summaries` | Merge (upsert) | `id` |
| `play_by_play` | `staging.play_by_play` | Merge (upsert) | `id` |

## Load Modes

### Append (default)
Adds new records without removing existing data.

```bash
python nhl_to_snowflake.py --mode append
```

### Truncate
Deletes all existing data before loading (useful for full refreshes).

```bash
python nhl_to_snowflake.py --mode truncate
```

### Merge (upsert)
Updates existing records and inserts new ones based on merge keys.

```bash
python nhl_to_snowflake.py --mode merge
```

## Features

### Automatic Table Creation
Tables are created automatically if they don't exist, with schemas inferred from the API data.

### ETL Metadata
Every record includes `_etl_loaded_at` timestamp (matches `sources.yml` `loaded_at_field`).

### JSON/VARIANT Handling
Complex nested structures (rosters, play-by-play data) are stored as Snowflake VARIANT columns.

### Error Handling
- Automatic retries for API failures
- 404 errors logged but don't fail the job
- Batch inserts for performance

## Dagster Integration

For production use with scheduling and monitoring:

### Install Dagster

```bash
pip install dagster dagster-webserver dagster-snowflake
```

### Run Dagster UI

```bash
# Make sure SNOWFLAKE_PASSWORD is set
export SNOWFLAKE_PASSWORD=your_password

# Launch UI
dagster dev -f nhl_dagster_snowflake.py
```

Open http://localhost:3000 to see the asset graph and materialize assets.

### Benefits

1. **Dependency Graph**: Visualize data lineage
2. **Selective Materialization**: Run only what you need
3. **Asset Caching**: Reuse parent data without re-fetching
4. **Scheduling**: Automated daily runs
5. **dbt Integration**: Orchestrate extraction + transformation together

### Dagster + dbt

You can orchestrate both extraction and dbt in one pipeline:

```python
from dagster_dbt import dbt_assets, DbtProject

# Your NHL extraction assets
@asset
def games(...):
    # Extract games to Snowflake
    ...

# Your dbt transformations
@dbt_assets(...)
def dbt_project(...):
    # Run dbt models
    ...

# Dagster automatically handles dependencies
# games → staging.games → dbt models → analytics tables
```

## Verification

After running the extractor, verify data in Snowflake:

```sql
-- Check record counts
SELECT 'current_standings' as table_name, COUNT(*) as record_count FROM staging.current_standings
UNION ALL
SELECT 'current_teams', COUNT(*) FROM staging.current_teams
UNION ALL
SELECT 'games', COUNT(*) FROM staging.games
UNION ALL
SELECT 'daily_standings', COUNT(*) FROM staging.daily_standings
UNION ALL
SELECT 'team_rosters', COUNT(*) FROM staging.team_rosters
UNION ALL
SELECT 'season_schedules', COUNT(*) FROM staging.season_schedules
UNION ALL
SELECT 'game_boxscore', COUNT(*) FROM staging.game_boxscore
UNION ALL
SELECT 'game_summaries', COUNT(*) FROM staging.game_summaries
UNION ALL
SELECT 'play_by_play', COUNT(*) FROM staging.play_by_play;

-- Check data freshness
SELECT
    'current_standings' as table_name,
    MAX(_etl_loaded_at) as last_loaded
FROM staging.current_standings
UNION ALL
SELECT 'games', MAX(_etl_loaded_at) FROM staging.games
UNION ALL
SELECT 'daily_standings', MAX(_etl_loaded_at) FROM staging.daily_standings;

-- Sample records
SELECT * FROM staging.games LIMIT 10;
SELECT * FROM staging.current_standings LIMIT 10;
```

## Scheduling Options

### Option 1: Cron Job

```bash
# Add to crontab (daily at 3am)
0 3 * * * cd /path/to/project && python nhl_to_snowflake.py --start-date $(date -d "yesterday" +\%Y-\%m-\%d) --end-date $(date +\%Y-\%m-\%d)
```

### Option 2: Dagster Schedule

```python
from dagster import ScheduleDefinition

daily_nhl_schedule = ScheduleDefinition(
    job=...,
    cron_schedule="0 3 * * *",  # 3am daily
)
```

### Option 3: Airflow DAG

```python
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG('nhl_extraction', schedule_interval='0 3 * * *')

extract_task = BashOperator(
    task_id='extract_nhl_data',
    bash_command='python nhl_to_snowflake.py',
    dag=dag,
)
```

## Performance Considerations

### Speed Tips

1. **Skip dependent streams during development**
   ```bash
   python nhl_to_snowflake.py --no-dependent
   ```

2. **Extract specific streams only**
   ```bash
   python nhl_to_snowflake.py --streams games daily_standings
   ```

3. **Use smaller date ranges**
   ```bash
   python nhl_to_snowflake.py --start-date 2024-11-07 --end-date 2024-11-07
   ```

### Typical Runtime

| Scope | Approximate Time |
|-------|-----------------|
| Simple streams only | 5-10 seconds |
| Simple + incremental (1 day) | 15-30 seconds |
| All streams (1 day) | 2-5 minutes |
| All streams (7 days) | 10-30 minutes |
| All streams (full season) | 2-6 hours |

### Memory Usage

- Simple/incremental streams: < 100 MB
- With play-by-play (1 day): 200-500 MB
- With play-by-play (7 days): 1-2 GB
- Full season: 5-10 GB

## Troubleshooting

### Connection Issues

```python
# Test Snowflake connection
python -c "
from snowflake_writer import SnowflakeWriter
import os

config = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA'),
    'role': os.getenv('SNOWFLAKE_ROLE'),
}

with SnowflakeWriter(**config) as writer:
    print('✓ Successfully connected to Snowflake')
"
```

### API Rate Limiting

The NHL API doesn't require authentication but may rate limit. The extractor includes:
- Automatic retry with exponential backoff
- 1-second delays between retries

If you hit rate limits, add delays:
```python
# In nhl_extractor.py, increase retry_delay
client = NHLAPIClient(max_retries=5, retry_delay=2)
```

### Table Schema Issues

If API structure changes, you may need to recreate tables:

```sql
-- Drop and recreate table
DROP TABLE staging.games;

-- Re-run extractor to recreate with new schema
python nhl_to_snowflake.py --streams games
```

## Comparison with Airbyte

| Feature | Airbyte | This Extractor |
|---------|---------|----------------|
| Setup Complexity | Medium (UI config) | Low (Python script) |
| Customization | Limited | Full control |
| Monitoring | Built-in UI | Manual/Dagster |
| Schema Evolution | Automatic | Manual table drops |
| Cost | Free (self-hosted) | Free |
| Performance | Good | Good |
| Dependencies | Docker required | Python only |

## Next Steps

1. **Test the extraction** with a small date range
   ```bash
   python nhl_to_snowflake.py --start-date 2024-11-07 --end-date 2024-11-07 --no-dependent
   ```

2. **Verify in Snowflake**
   ```sql
   SELECT * FROM staging.games LIMIT 10;
   ```

3. **Run your dbt models**
   ```bash
   dbt run
   ```

4. **Set up scheduling** (cron, Dagster, or Airflow)

5. **Monitor data freshness** using dbt's source freshness checks
   ```bash
   dbt source freshness
   ```

## Questions?

- Check `nhl_extractor.py` for API extraction logic
- Check `snowflake_writer.py` for Snowflake write logic
- Check `nhl_to_snowflake.py` for orchestration
- Check `sources.yml` for expected table schemas
