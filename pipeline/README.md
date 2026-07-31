# Pipeline

## `refresh.py` — the nightly entrypoint

One command refreshes everything: extract (sliding window) → flatten → load →
`dbt build --target prod` → Metabase schema sync → Omni schema refresh →
invariant checks. Platform-agnostic by design: it only needs Python 3.11+,
the packages in `requirements.txt`, dbt-snowflake, and environment variables.

**Contract (what any scheduler — autumneight, GitHub Actions, cron — must provide):**

| Aspect | Contract |
| --- | --- |
| Invocation | `python pipeline/refresh.py` from the repo root (nightly), `--refresh-reference` weekly or when the schedule/rosters change |
| Credentials | `SNOWFLAKE_ACCOUNT/USER/WAREHOUSE/DATABASE/SCHEMA/ROLE` + either `SNOWFLAKE_PRIVATE_KEY_PATH` (preferred, PKCS#8 PEM) or `SNOWFLAKE_PASSWORD`; optional `MB_API_KEY_FILE`, `OMNI_BASE_URL`/`OMNI_API_KEY`/`OMNI_CONNECTION_ID`, `ALERT_WEBHOOK_URL` |
| Output | one JSON object per line on stdout (`step`, `status`, timings, details) |
| Success | exit code 0; any failed required step → nonzero exit + webhook alert |
| Idempotence | safe to re-run for the same window; staging dedups on `_loaded_at` |
| Schedule | daily ~09:00 UTC (after West Coast games post final stats); Mondays add `--refresh-reference` automatically |

**Step semantics:** extract/flatten/load/dbt/invariants are required (failure
fails the run); Metabase/Omni syncs are best-effort (logged, never fatal).
Invariants enforced post-run: league PP goals ≡ league PK goals-against,
exactly 32 active franchises, and latest-completed-game freshness is logged.

## `flatten_extract.py`

The required bridge between the extractor's nested JSON-string parquet and
the warehouse's flat uppercase schema (see module docstring for the exact
convention). `refresh.py` calls it automatically; call it manually when
backfilling:

```bash
python nhl_to_parquet.py --start-date 2015-10-01 --end-date 2016-06-30 --output-dir ./x --request-delay 0.3
python pipeline/flatten_extract.py ./x ./x_flat
# prune full-replace tables before loading historical windows!
rm -f ./x_flat/{team_rosters,current_standings,current_teams,season_schedules}.parquet
python parquet_to_snowflake.py --input-dir ./x_flat
```
