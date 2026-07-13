# NHL Analytics — dbt Project

An end-to-end NHL analytics pipeline: Python extraction from the NHL API →
Snowflake → dbt transformations → a public Metabase dashboard.

## Architecture

```
NHL API ──nhl_to_parquet.py──▶ parquet ──parquet_to_snowflake.py──▶ DBT_ANALYTICS.STAGING
                                                                        │
   staging/        1:1 typed views over raw tables; dedup to the latest │ load
   (views)         _loaded_at per natural key; never filtered           ▼
   intermediate/   business logic. int__league_games is the canonical  dbt
   (views/tables)  game universe (NHL-vs-NHL, regular+playoff only) —
                   the single choke point that keeps All-Star and
                   4 Nations games out of every stat
   dimensional/    star schema (tables): dim_* / fct_* / metrics.
                   Natural keys (team_id, player_id, game_id) for dims,
                   hashed surrogate keys for composite fact grains
   marts/          presentation layer for Metabase (tables): stable
                   names, display columns (season_display), leaderboard
                   qualification flags
```

## Key modeling decisions

- **League-game decontamination**: every stats model filters through
  `int__league_games` (game types 2/3 between two franchises validated
  against that season's standings participants).
- **Derived team stats**: the loaded `game_summaries` table lacks
  `summary.teamGameStats`, so hits/blocks/giveaways/takeaways/PP goals are
  summed from player boxscores; PIM and times-shorthanded come from
  play-by-play penalty events (offsetting penalties cancel, misconducts
  excluded); faceoff counts come from play-by-play faceoff events. League
  PP goals ≡ league PK goals-against by construction.
- **Stable keys**: `team_key = team_id`, `player_key = player_id`,
  `game_key = game_id`; composite grains use `dbt_utils.generate_surrogate_key`.
  Keys survive rebuilds — no `row_number()` keys anywhere.
- **Play-by-play attribution**: `fct_plays` parses the 4-digit situation
  code (strength/PP/SH/empty-net) and carries scorer/assist/hitter/blocker/
  goalie player keys, enabling real GWG/OT/SH/EN goals, assist splits,
  faceoff counts, and Corsi/Fenwick/PDO (`mart_team_shot_metrics`).

## Running

```bash
# local dev (venv one level up)
../venv/bin/dbt deps
../venv/bin/dbt build            # dev target -> DBT_ANALYTICS.DBT_DEV
../venv/bin/dbt build --target prod
../venv/bin/dbt docs generate && ../venv/bin/dbt docs serve
```

Profiles live in `~/.dbt/profiles.yml` (profile `nhl_analytics`); CI uses
`ci/profiles.yml` with env-var credentials into the isolated `DBT_CI` schema.

## Seeds, macros, vars

- Seeds: `nhl_team_colors`, `nhl_team_arenas`, `nhl_team_history` (static
  franchise attributes incl. Utah).
- Macros: `safe_divide`, `parse_toi`, `season_display`.
- Vars: `regular_season_games` (82), `league_team_count` (32),
  `league_avg_save_pct` (0.910), `leaderboard_min_gp_skater` (10),
  `leaderboard_min_gp_goalie` (15).

## Testing

Grain uniqueness on every fact/mart, `relationships` from fact FKs to dims,
`accepted_values` on enums, `dbt_expectations` range guards on every
percentage metric (the regression class that bit this project), and dbt
unit tests for the trickiest logic (penalty offsetting, faceoff counting,
per-60 GAA). Tests fail builds; `severity: warn` is the exception, inline
and justified.

## Exposures

`models/exposures.yml` declares the public Metabase dashboard and the models
its cards query — check it before renaming or dropping anything.

## Data loading & backfill

```bash
python nhl_to_parquet.py --start-date 2024-10-01 --end-date 2025-06-30 \
    --output-dir ./data_backfill_s2425 --request-delay 0.3
# IMPORTANT: prune full-replace tables before loading a historical window,
# or you will clobber current rosters/standings/schedules:
rm ./data_backfill_s2425/{team_rosters,current_standings,current_teams,season_schedules}.parquet
python parquet_to_snowflake.py --input-dir ./data_backfill_s2425
```

`season_schedules` is full-replace by design and therefore only holds the
most recently extracted season — nothing downstream may depend on it for
historical game types (fct_games derives game type from the games feed).

## Housekeeping notes

- The Evidence.dev scaffold and standalone Python visualization apps were
  removed (2026-07); recover from git history if needed. The Python
  extractor (`nhl_extractor.py` + CLIs) remains the ingestion path.
- Source freshness reflects in-season expectations; offseason staleness
  warnings are expected.
