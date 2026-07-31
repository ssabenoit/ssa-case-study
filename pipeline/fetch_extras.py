#!/usr/bin/env python
"""Tier-1 auxiliary NHL streams: shift charts, official stat summaries,
player landing (draft/bio/awards).

Each subcommand fetches, flattens to the warehouse's uppercase convention,
writes parquet to a temp dir, and appends via parquet_to_snowflake (tables
auto-create on first load; staging dedups on _LOADED_AT).

Usage:
  python pipeline/fetch_extras.py shifts --season 20252026 [--limit N]
  python pipeline/fetch_extras.py shifts --window-days 3        # nightly mode
  python pipeline/fetch_extras.py official-summaries --seasons 20072008..20252026
  python pipeline/fetch_extras.py player-landing [--all | --missing-only]
"""
import argparse
import json
import shutil
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(REPO_ROOT / ".env")

from pipeline.flatten_extract import flatten_record  # noqa: E402

STATS = "https://api.nhle.com/stats/rest/en"
WEB = "https://api-web.nhle.com/v1"
REQUEST_DELAY = 0.4


def log(step, status, **fields):
    print(json.dumps({"ts": datetime.now(timezone.utc).isoformat(),
                      "step": step, "status": status, **fields}), flush=True)


def loaded_at():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")


def get_json(url, retries=5):
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code == 429:
                # rate-limited: back off hard before retrying
                wait = 30 * (attempt + 1)
                log("http", "rate_limited", url=url.split("?")[0], wait_s=wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception:  # noqa: BLE001
            if attempt == retries - 1:
                raise
            time.sleep(5 * (attempt + 1))
        finally:
            time.sleep(REQUEST_DELAY)
    raise RuntimeError(f"exhausted retries (rate limited): {url}")


def snowflake_query(sql):
    import snowflake.connector
    from parquet_to_snowflake import get_snowflake_config
    conn = snowflake.connector.connect(**get_snowflake_config())
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()


def table_exists(name):
    rows = snowflake_query(
        "select count(*) from information_schema.tables "
        f"where table_schema = 'STAGING' and table_name = '{name.upper()}'"
    )
    return rows[0][0] > 0


def load_rows(rows, table, work):
    if not rows:
        log("load", "skipped", table=table, reason="no rows")
        return 0
    from parquet_to_snowflake import load_parquet_to_snowflake
    out = work / "load" / f"{table}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(out, index=False)
    load_parquet_to_snowflake(input_dir=str(out.parent))
    out.unlink()
    log("load", "ok", table=table, rows=len(rows))
    return len(rows)


# ---------------------------------------------------------------- shifts ----
def cmd_shifts(args, work):
    if args.window_days:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=args.window_days)).date()
        game_sql = f"""
            select distinct ID from DBT_ANALYTICS.STAGING.GAMES
            where GAMETYPE in (2, 3) and GAMEDATE::date >= '{cutoff}'
        """
    else:
        game_sql = f"""
            select distinct ID from DBT_ANALYTICS.STAGING.GAMES
            where GAMETYPE in (2, 3) and SEASON = {args.season}
        """
    game_ids = [r[0] for r in snowflake_query(game_sql)]
    if table_exists("SHIFT_CHARTS"):
        have = {r[0] for r in snowflake_query(
            "select distinct GAMEID from DBT_ANALYTICS.STAGING.SHIFT_CHARTS")}
        game_ids = [g for g in game_ids if g not in have]
    if args.limit:
        game_ids = game_ids[: args.limit]
    log("shifts", "started", games=len(game_ids))

    batch, total, ts = [], 0, loaded_at()
    for i, gid in enumerate(game_ids, 1):
        payload = get_json(f"{STATS}/shiftcharts?cayenneExp=gameId={gid}")
        for shift in payload.get("data", []):
            row = flatten_record(shift)
            row["_LOADED_AT"] = ts
            batch.append(row)
        if len(batch) >= 200_000 or i == len(game_ids):
            total += load_rows(batch, "shift_charts", work)
            batch = []
        if i % 100 == 0:
            log("shifts", "progress", done=i, of=len(game_ids))
    log("shifts", "ok", games=len(game_ids), rows=total)


# ---------------------------------------------------- official summaries ----
def cmd_official(args, work):
    lo, hi = args.seasons.split("..") if ".." in args.seasons else (args.seasons, args.seasons)
    seasons = []
    y = int(lo[:4])
    while f"{y}{y+1}" <= hi:
        seasons.append(f"{y}{y+1}")
        y += 1
    ts = loaded_at()
    for kind in ("skater", "goalie", "team"):
        rows = []
        for season in seasons:
            for game_type in (2, 3):
                url = (f"{STATS}/{kind}/summary?limit=-1&cayenneExp="
                       f"seasonId={season}%20and%20gameTypeId={game_type}")
                payload = get_json(url)
                for rec in payload.get("data", []):
                    row = flatten_record(rec)
                    row["GAMETYPEID"] = game_type
                    row["_LOADED_AT"] = ts
                    rows.append(row)
        load_rows(rows, f"official_{kind}_summary", work)
    log("official", "ok", seasons=len(seasons))


# --------------------------------------------------------- player landing ----
def cmd_landing(args, work):
    player_ids = [r[0] for r in snowflake_query(
        "select distinct PLAYER_ID from DBT_ANALYTICS.PROD.DIM_PLAYERS")]
    if not args.all and table_exists("PLAYER_LANDING"):
        have = {r[0] for r in snowflake_query(
            "select distinct PLAYERID from DBT_ANALYTICS.STAGING.PLAYER_LANDING")}
        player_ids = [p for p in player_ids if p not in have]
    log("landing", "started", players=len(player_ids))

    keep = ["playerId", "firstName", "lastName", "position", "heightInInches",
            "weightInPounds", "birthDate", "birthCity", "birthCountry",
            "shootsCatches", "draftDetails", "isActive", "sweaterNumber",
            "headshot", "careerTotals", "awards", "currentTeamAbbrev"]
    batch, total, ts = [], 0, loaded_at()
    for i, pid in enumerate(player_ids, 1):
        try:
            payload = get_json(f"{WEB}/player/{pid}/landing")
        except Exception as exc:  # noqa: BLE001 — some historical ids 404
            log("landing", "skip_player", player_id=pid, error=str(exc)[:120])
            continue
        row = flatten_record({k: payload.get(k) for k in keep})
        row["_LOADED_AT"] = ts
        batch.append(row)
        if len(batch) >= 2000 or i == len(player_ids):
            total += load_rows(batch, "player_landing", work)
            batch = []
        if i % 250 == 0:
            log("landing", "progress", done=i, of=len(player_ids))
    log("landing", "ok", rows=total)


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("shifts")
    p.add_argument("--season", type=int)
    p.add_argument("--window-days", type=int)
    p.add_argument("--limit", type=int)

    p = sub.add_parser("official-summaries")
    p.add_argument("--seasons", required=True, help="e.g. 20072008..20252026")

    p = sub.add_parser("player-landing")
    p.add_argument("--all", action="store_true")

    args = parser.parse_args()
    work = Path(tempfile.mkdtemp(prefix="nhl_extras_"))
    try:
        {"shifts": cmd_shifts,
         "official-summaries": cmd_official,
         "player-landing": cmd_landing}[args.cmd](args, work)
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
