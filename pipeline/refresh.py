#!/usr/bin/env python
"""Nightly NHL data refresh — the platform-agnostic pipeline entrypoint.

Runs: extract (sliding window) -> flatten -> load -> dbt build (prod) ->
Metabase schema sync -> Omni schema refresh -> invariant checks.

Designed to run identically under autumneight, GitHub Actions, or a laptop:
- configuration via environment variables (.env supported)
- one JSON log line per step on stdout
- exit 0 only if every step succeeds; nonzero otherwise
- optional webhook alert (Slack-compatible) on completion/failure
- idempotent: staging dedup keys on _loaded_at, so replaying a window is safe

Usage:
  python pipeline/refresh.py                     # nightly window (2 days back)
  python pipeline/refresh.py --window-days 7     # wider catch-up window
  python pipeline/refresh.py --refresh-reference # also refresh schedules/rosters/
                                                 # standings/teams (full-replace);
                                                 # default on Mondays
  python pipeline/refresh.py --skip-dbt --skip-bi  # data-only run

Env:
  SNOWFLAKE_* (see parquet_to_snowflake.get_snowflake_config)
  DBT_BIN            path to dbt executable (default: ../venv/bin/dbt)
  MB_URL             Metabase base URL (default: expert-southshore instance)
  MB_API_KEY_FILE    file containing the Metabase API key
                     (default: ../.mb_api_key relative to repo root)
  OMNI_BASE_URL, OMNI_API_KEY, OMNI_CONNECTION_ID   optional Omni schema refresh
  ALERT_WEBHOOK_URL  optional Slack-compatible webhook
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(REPO_ROOT / ".env")

from pipeline.flatten_extract import flatten_directory  # noqa: E402

FULL_REPLACE_TABLES = {"team_rosters", "current_standings", "current_teams", "season_schedules"}
APPEND_TABLES = {"games", "game_boxscore", "game_summaries", "daily_standings", "play_by_play"}


def log(step: str, status: str, **fields):
    print(json.dumps({
        "ts": datetime.now(timezone.utc).isoformat(),
        "step": step,
        "status": status,
        **fields,
    }), flush=True)


def run_step(name: str, fn, results: list):
    started = time.monotonic()
    log(name, "started")
    try:
        detail = fn() or {}
        elapsed = round(time.monotonic() - started, 1)
        log(name, "ok", elapsed_s=elapsed, **detail)
        results.append((name, True, detail))
        return True
    except Exception as exc:  # noqa: BLE001 - we want the pipeline to report, not crash
        elapsed = round(time.monotonic() - started, 1)
        log(name, "failed", elapsed_s=elapsed, error=str(exc)[:500])
        results.append((name, False, {"error": str(exc)[:500]}))
        return False


def snowflake_connection():
    import snowflake.connector
    from parquet_to_snowflake import get_snowflake_config
    return snowflake.connector.connect(**get_snowflake_config())


def step_extract(work_dir: Path, start: date, end: date):
    from nhl_to_parquet import extract_to_parquet
    extract_to_parquet(
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        output_dir=str(work_dir / "raw"),
        request_delay=0.3,
    )
    files = sorted(p.name for p in (work_dir / "raw").glob("*.parquet"))
    return {"files": files, "window": f"{start} -> {end}"}


def step_flatten(work_dir: Path):
    counts = flatten_directory(str(work_dir / "raw"), str(work_dir / "flat"))
    return {"rows": counts}


def step_load(work_dir: Path, refresh_reference: bool):
    from parquet_to_snowflake import load_parquet_to_snowflake
    flat = work_dir / "flat"

    append_dir = work_dir / "load_append"
    append_dir.mkdir()
    for p in flat.glob("*.parquet"):
        if p.stem in APPEND_TABLES:
            shutil.copy(p, append_dir / p.name)
    load_parquet_to_snowflake(input_dir=str(append_dir))
    loaded = {"append": sorted(p.stem for p in append_dir.glob("*.parquet"))}

    if refresh_reference:
        ref_dir = work_dir / "load_reference"
        ref_dir.mkdir()
        for p in flat.glob("*.parquet"):
            if p.stem in FULL_REPLACE_TABLES:
                shutil.copy(p, ref_dir / p.name)
        if any(ref_dir.iterdir()):
            load_parquet_to_snowflake(input_dir=str(ref_dir))
            loaded["reference"] = sorted(p.stem for p in ref_dir.glob("*.parquet"))
    return loaded


def step_extras():
    """Tier-1 auxiliary streams: recent shift charts, current-season official
    aggregates, landing payloads for any new players. Best-effort."""
    results = {}
    season = os.getenv("CURRENT_SEASON", "20262027")
    for name, cmd in (
        ("shifts", ["shifts", "--window-days", "3"]),
        ("official", ["official-summaries", "--seasons", f"{season}..{season}"]),
        ("landing", ["player-landing"]),
    ):
        proc = subprocess.run(
            [sys.executable, str(REPO_ROOT / "pipeline" / "fetch_extras.py"), *cmd],
            capture_output=True, text=True,
        )
        results[name] = "ok" if proc.returncode == 0 else f"failed: {proc.stdout.splitlines()[-1:] or proc.returncode}"
    return results


def step_dbt():
    dbt_bin = os.getenv("DBT_BIN", str(REPO_ROOT.parent / "venv" / "bin" / "dbt"))
    proc = subprocess.run(
        [dbt_bin, "build", "--target", "prod", "--project-dir", str(REPO_ROOT)],
        capture_output=True, text=True,
    )
    tail = "\n".join(proc.stdout.splitlines()[-3:])
    if proc.returncode != 0:
        raise RuntimeError(f"dbt build failed (exit {proc.returncode}): {tail}")
    return {"dbt_tail": tail}


def step_metabase_sync():
    import requests
    mb_url = os.getenv("MB_URL", "https://expert-southshore.metabaseapp.com")
    key_file = os.getenv("MB_API_KEY_FILE", str(REPO_ROOT.parent / ".mb_api_key"))
    if not Path(key_file).exists():
        return {"skipped": "no Metabase API key file"}
    key = Path(key_file).read_text().strip()
    resp = requests.post(f"{mb_url}/api/database/2/sync_schema",
                         headers={"x-api-key": key}, timeout=60)
    resp.raise_for_status()
    return {"metabase": resp.json()}


def step_omni_refresh():
    import requests
    base, key, conn = (os.getenv("OMNI_BASE_URL"), os.getenv("OMNI_API_KEY"),
                       os.getenv("OMNI_CONNECTION_ID"))
    if not (base and key and conn):
        return {"skipped": "Omni env not configured"}
    resp = requests.post(
        f"{base}/api/v1/connections/{conn}/schema-refresh",
        headers={"Authorization": f"Bearer {key}"}, timeout=120,
    )
    resp.raise_for_status()
    return {"omni": resp.status_code}


def step_invariants():
    checks = {}
    conn = snowflake_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            select sum(pp_goals), sum(pk_goals_against)
            from DBT_ANALYTICS.PROD.INT__TEAM_PER_GAME_STATS
        """)
        pp, pk = cur.fetchone()
        checks["league_pp_equals_pk_ga"] = (pp == pk)
        if pp != pk:
            raise RuntimeError(f"invariant violated: league PP goals {pp} != PK GA {pk}")

        cur.execute("select count(*) from DBT_ANALYTICS.PROD.DIM_TEAMS where is_active")
        active = cur.fetchone()[0]
        checks["active_teams"] = active
        if active != 32:
            raise RuntimeError(f"invariant violated: {active} active teams (expected 32)")

        cur.execute("""
            select max(game_date) from DBT_ANALYTICS.PROD.INT__LEAGUE_GAMES
            where game_state in ('OFF', 'FINAL')
        """)
        checks["latest_completed_game"] = str(cur.fetchone()[0])
    finally:
        conn.close()
    return checks


def alert(payload: str):
    url = os.getenv("ALERT_WEBHOOK_URL")
    if not url:
        return
    try:
        import requests
        requests.post(url, json={"text": payload}, timeout=15)
    except Exception as exc:  # noqa: BLE001
        log("alert", "failed", error=str(exc)[:200])


def main():
    parser = argparse.ArgumentParser(description="Nightly NHL data refresh")
    parser.add_argument("--window-days", type=int, default=2,
                        help="extract this many days back through today (default 2)")
    parser.add_argument("--refresh-reference", action="store_true",
                        help="also refresh schedules/rosters/standings/teams "
                             "(full-replace tables); defaults on Mondays")
    parser.add_argument("--skip-extract", action="store_true")
    parser.add_argument("--skip-dbt", action="store_true")
    parser.add_argument("--skip-bi", action="store_true")
    args = parser.parse_args()

    today = date.today()
    start = today - timedelta(days=args.window_days)
    refresh_reference = args.refresh_reference or today.weekday() == 0

    results = []
    work_dir = Path(tempfile.mkdtemp(prefix="nhl_refresh_"))
    log("pipeline", "started", window=f"{start} -> {today}",
        refresh_reference=refresh_reference, work_dir=str(work_dir))

    ok = True
    try:
        if not args.skip_extract:
            ok = run_step("extract", lambda: step_extract(work_dir, start, today), results)
            ok = ok and run_step("flatten", lambda: step_flatten(work_dir), results)
            ok = ok and run_step("load", lambda: step_load(work_dir, refresh_reference), results)
            run_step("extras", step_extras, results)   # best-effort aux streams
        if ok and not args.skip_dbt:
            ok = run_step("dbt_build_prod", step_dbt, results)
        if ok and not args.skip_bi:
            run_step("metabase_sync", step_metabase_sync, results)   # non-fatal
            run_step("omni_refresh", step_omni_refresh, results)     # non-fatal
        if ok:
            ok = run_step("invariants", step_invariants, results)
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)

    failed = [name for name, success, _ in results if not success]
    summary = ("NHL refresh OK: " + ", ".join(n for n, s, _ in results if s)) if ok \
        else ("NHL refresh FAILED at: " + ", ".join(failed))
    log("pipeline", "ok" if ok else "failed", failed_steps=failed)
    alert(summary)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
