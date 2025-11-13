# NHL Data Extractor

Python implementation of the Airbyte NHL API extraction logic. Supports both standalone usage and Dagster orchestration.

## Overview

This extractor reproduces the data extraction patterns from `airbyte_config.yml`, supporting:

- **9 data streams** from the NHL API
- **3 extraction patterns**: Simple, Incremental (date-ranged), and Dependent (nested)
- **Automatic retry logic** with rate limiting
- **Dependency management** between streams

## Streams

### Simple Streams (single API call)
- `current_standings` - Current NHL standings
- `current_teams` - Active teams from schedule calendar

### Incremental Streams (date-ranged)
- `games` - Game scores for each date
- `daily_standings` - Historical standings by date

### Dependent Streams (require parent data)
**Team-dependent** (uses `current_teams`):
- `team_rosters` - Roster for each team
- `season_schedules` - Season schedule for each team

**Game-dependent** (uses `games`):
- `game_boxscore` - Boxscore for each game
- `game_summaries` - Game story/summary for each game
- `play_by_play` - Play-by-play data for each game

## Installation

```bash
# Basic usage
pip install requests

# For Dagster integration
pip install dagster dagster-webserver
```

## Usage

### Option 1: Standalone Python Script

```python
from nhl_extractor import NHLExtractor

# Initialize with date range
extractor = NHLExtractor(
    start_date="2024-11-04",
    end_date="2024-11-07"
)

# Extract specific streams
standings = extractor.extract_stream('current_standings')
games = extractor.extract_stream('games')

# Extract all streams (warning: can be slow with dependent streams)
all_data = extractor.extract_all(include_dependent=True)

# Access data
for stream_name, records in all_data.items():
    print(f"{stream_name}: {len(records)} records")
```

Run the example:
```bash
python nhl_extractor.py
```

### Option 2: Dagster Orchestration

Dagster provides:
- Dependency graph visualization
- Incremental materialization
- Asset metadata and lineage
- Scheduling and monitoring

#### Run Dagster UI

```bash
dagster dev -f nhl_dagster.py
```

Open http://localhost:3000 to view the asset graph and materialize assets.

#### Programmatic Usage

```python
from dagster import materialize
from nhl_dagster import current_standings, games

# Materialize specific assets
result = materialize(
    [current_standings, games],
    run_config={
        "ops": {
            "current_standings": {
                "config": {
                    "start_date": "2024-11-04",
                    "end_date": "2024-11-07",
                    "output_dir": "nhl_data"
                }
            },
            "games": {
                "config": {
                    "start_date": "2024-11-04",
                    "end_date": "2024-11-07",
                    "output_dir": "nhl_data"
                }
            }
        }
    }
)
```

Or run the example:
```bash
python nhl_dagster.py
```

## Architecture

### Class Hierarchy

```
NHLAPIClient
  └── Handles HTTP requests, retry logic, field extraction

BaseStream (abstract)
  ├── SimpleStream - Single API call
  ├── IncrementalStream - Date iteration
  └── DependentStream - Parent data iteration

NHLExtractor
  └── Orchestrates all streams with dependency resolution
```

### Dependency Graph

```
current_teams ──┬──> team_rosters
                └──> season_schedules

games ──────────┬──> game_boxscore
                ├──> game_summaries
                └──> play_by_play

current_standings (independent)
daily_standings (independent)
```

## Configuration

### Date Ranges

```python
# Last 7 days (default)
extractor = NHLExtractor()

# Custom range
extractor = NHLExtractor(
    start_date="2024-10-01",
    end_date="2024-10-31"
)
```

### Stream Configuration

Each stream is configured with:
- `name` - Stream identifier
- `endpoint_template` - API endpoint (with optional placeholders)
- `field_path` - JSON path to extract records (e.g., `["standings"]` or `[]` for root)

Example:
```python
StreamConfig(
    name="games",
    endpoint_template="score/{date}",
    field_path=["games"]
)
```

## Performance Considerations

### Dependent Streams Can Be Slow

Dependent streams make one API call per parent record:
- `team_rosters` - ~32 API calls (one per team)
- `game_boxscore` - Variable (one per game in date range)

For faster extraction during development:
```python
# Skip dependent streams
all_data = extractor.extract_all(include_dependent=False)
```

### Dagster Benefits

For production use, Dagster provides:
- **Selective materialization** - Only run what you need
- **Caching** - Reuse parent data without re-fetching
- **Parallel execution** - Run independent streams concurrently
- **Scheduling** - Automated daily/hourly runs

## Output

### Standalone Script
Returns Python dictionaries/lists that can be:
- Saved to JSON files
- Inserted into databases
- Processed in-memory

### Dagster
By default saves to JSON files in `nhl_data/` directory. Customize the `save_to_json` function to:
- Write to PostgreSQL/DuckDB/Snowflake
- Upload to S3/GCS
- Send to data warehouse

## Comparison: Standalone vs Dagster

| Feature | Standalone | Dagster |
|---------|-----------|---------|
| Setup Complexity | Low | Medium |
| Dependency Management | Manual | Automatic |
| Incremental Runs | Manual | Built-in |
| Monitoring | Manual logging | UI + metadata |
| Scheduling | External (cron) | Built-in |
| Best For | Ad-hoc extraction, simple pipelines | Production, complex dependencies |

## Is Dagster Overkill?

**Use standalone if:**
- One-off data extraction
- Simple pipeline (< 5 streams)
- No scheduling needed
- Minimal dependencies

**Use Dagster if:**
- Production data pipeline
- Need scheduling/monitoring
- Complex dependencies (like your 9 streams)
- Want to integrate with dbt/other tools
- Team collaboration needed

For your use case with 9 interdependent streams, Dagster provides good value without being overkill.

## Next Steps

1. **Test the extraction** with a small date range
2. **Validate the data** matches your Airbyte output
3. **Choose output destination** (files, database, warehouse)
4. **Set up scheduling** if using Dagster
5. **Integrate with dbt** for transformations (Dagster can orchestrate both)

## Troubleshooting

### Rate Limiting
The NHL API doesn't require authentication but may rate limit. The client includes:
- Automatic retry with exponential backoff
- 1-second delays between retries

### 404 Errors
Some endpoints return 404 for:
- Invalid dates (off-season)
- Invalid game IDs
- Invalid team abbreviations

These are logged as warnings but don't fail the extraction.

### Memory Usage
Processing many games with play-by-play data can use significant memory. Consider:
- Processing smaller date ranges
- Using Dagster to materialize one asset at a time
- Streaming results to disk instead of holding in memory
