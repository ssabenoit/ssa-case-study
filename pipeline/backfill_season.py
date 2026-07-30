#!/usr/bin/env python
"""Backfill one NHL season into the warehouse: extract -> flatten -> load.

Usage: python pipeline/backfill_season.py 2022    # the 2022-23 season

Extract window spans preseason through the Cup Final (Sep 15 -> Jul 5).
Only append tables are loaded — the four full-replace tables (schedules,
rosters, current standings/teams) are pruned so current-season reference
data is never clobbered by a historical extract. Safe to re-run: staging
dedups on _loaded_at. Designed as an autumneight chunk job (one season per
run, ~30-60 min at 0.3s request spacing).
"""
import json
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(REPO_ROOT / ".env")

from pipeline.flatten_extract import flatten_directory  # noqa: E402

FULL_REPLACE_TABLES = {"team_rosters", "current_standings", "current_teams", "season_schedules"}


def log(step, status, **fields):
    print(json.dumps({"ts": datetime.now(timezone.utc).isoformat(),
                      "step": step, "status": status, **fields}), flush=True)


def main():
    if len(sys.argv) != 2 or not sys.argv[1].isdigit():
        print(__doc__)
        sys.exit(2)
    start_year = int(sys.argv[1])
    season = f"{start_year}{start_year + 1}"
    start, end = f"{start_year}-09-15", f"{start_year + 1}-07-05"

    from nhl_to_parquet import extract_to_parquet
    from parquet_to_snowflake import load_parquet_to_snowflake

    work = Path(tempfile.mkdtemp(prefix=f"nhl_backfill_{season}_"))
    log("backfill", "started", season=season, window=f"{start} -> {end}")
    try:
        extract_to_parquet(start_date=start, end_date=end,
                           output_dir=str(work / "raw"), request_delay=0.3)
        log("extract", "ok", files=sorted(p.name for p in (work / "raw").glob("*.parquet")))

        counts = flatten_directory(str(work / "raw"), str(work / "flat"))
        log("flatten", "ok", rows=counts)

        for table in FULL_REPLACE_TABLES:
            (work / "flat" / f"{table}.parquet").unlink(missing_ok=True)
        load_parquet_to_snowflake(input_dir=str(work / "flat"))
        log("load", "ok", tables=sorted(p.stem for p in (work / "flat").glob("*.parquet")))
        log("backfill", "ok", season=season)
    except Exception as exc:  # noqa: BLE001
        log("backfill", "failed", season=season, error=str(exc)[:500])
        sys.exit(1)
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
