# NHL Analytics Dashboard

A comprehensive dbt project for analyzing NHL statistics, player performance, and game data. This project transforms raw NHL API data into analytics-ready datasets for exploring team performance, player statistics, and game insights.

## Project Overview

This NHL Analytics Dashboard project provides a structured data transformation pipeline using dbt Core, creating a foundation for NHL data analysis and visualization. The project includes:

- Staging models that standardize and clean raw NHL API data
- Intermediate models that join and enhance the data
- Marts models that present analytics-ready data for specific use cases
- Visualizations for exploring team performance, player stats, and game insights

## Data Model

The data model is structured in layers:

### Sources
- Raw NHL API data stored in the `dbt_analytics.staging` schema
- Includes game data, player statistics, standings, and team information

### Staging Models
- Structured, typed, and normalized representation of source data
- Minimal transformations, focused on data quality and consistency
- Located in `models/staging/`

### Intermediate Models
- Business logic layer that joins and enhances staging models
- Creates reusable data components for various analytics
- Located in `models/intermediate/`

### Marts Models
- Analytics-ready datasets organized by business domain
- Optimized for specific analytical use cases and visualizations
- Located in `models/marts/`

## Key Features

- **Team Performance Analysis**: Track team standings, performance trends, and statistical rankings
- **Player Statistics**: Detailed player statistics for skaters and goalies across regular season and playoffs
- **Game Analysis**: Game-by-game breakdowns with play-by-play data
- **Historical Tracking**: Historical performance data for teams and players
- **Upcoming Games**: Schedule information for future games

## Getting Started

### Prerequisites
- dbt Core installed
- Access to a data warehouse (Snowflake, BigQuery, etc.)
- Python environment for visualizations (optional)

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/nhl-analytics.git
   cd nhl-analytics
   ```

2. Set up your profiles.yml file with connection details to your data warehouse.

3. Install dbt dependencies:
   ```bash
   dbt deps
   ```

4. Run the models:
   ```bash
   dbt build
   ```

### Visualizations

The project includes several visualization options in the `visualization/` directory:

- Dashboard for team performance metrics
- Player comparison tools
- Game analysis visualizations

To run the visualizations:

1. Set up the Python environment:
   ```bash
   conda env create -f visualization/ssa_environment.yml
   conda activate ssa_environment
   ```

2. Run the dashboard:
   ```bash
   python visualization/dashboard.py
   ```

## Project Structure

```
.
├── analyses/           # Ad-hoc analyses
├── macros/             # Reusable SQL macros
├── models/             # dbt models organized in layers
│   ├── intermediate/   # Business logic layer
│   ├── marts/          # Analytics-ready data
│   ├── staging/        # Initial data structuring
│   └── sources.yml     # Source definitions
├── seeds/              # Static data files
├── snapshots/          # Historical data tracking
├── tests/              # Data quality tests
└── visualization/      # Dashboard and visualization code
```

## Data Dictionary

Key metrics and dimensions available in the models include:

- **Team Performance**: points, wins, losses, goals_for, goals_against, etc.
- **Player Stats**: goals, assists, points, shots, plus_minus, time_on_ice, etc.
- **Game Data**: scores, play-by-play events, shots, faceoffs, etc.

For a comprehensive data dictionary, see the schema.yml files in each model directory.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Roadmap

See [PRODUCT_ROADMAP.md](PRODUCT_ROADMAP.md) for the planned development roadmap.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.