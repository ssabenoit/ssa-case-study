#!/usr/bin/env python
"""Parity check: canonical numbers through the Omni semantic layer vs Snowflake.

Queries the same figures through Omni's query API and directly against the
warehouse, plus fixed record-book anchors (Matthews 69 in 2023-24, Ovechkin 65
in 2007-08, 720 games in the 2012-13 lockout). Any drift between what Omni
serves and what dbt built is a failure.

Env (via repo .env): OMNI_BASE_URL, OMNI_MODEL_ID, OMNI_API_KEY or
OMNI_API_KEY_FILE, SNOWFLAKE_* (see parquet_to_snowflake.get_snowflake_config).
"""
import base64
import io
import json
import os
import sys
from pathlib import Path

import requests
import pyarrow.ipc as ipc

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
from dotenv import load_dotenv
load_dotenv(REPO_ROOT / ".env")

KEY = os.getenv("OMNI_API_KEY") or Path(os.environ["OMNI_API_KEY_FILE"]).read_text().strip()
BASE = os.environ["OMNI_BASE_URL"].rstrip("/") + "/api/v1"
H = {"Authorization": f"Bearer {KEY}"}
MODEL = os.environ["OMNI_MODEL_ID"]
P = "dbt_analytics_prod__"


def omni(table, fields, filters=None, limit=100):
    body = {"query": {"modelId": MODEL, "table": f"{P}{table}",
                      "fields": fields, "limit": limit}}
    if filters:
        body["query"]["filters"] = filters
    r = requests.post(f"{BASE}/query/run", headers=H, json=body, timeout=300)
    rows, sql = None, None
    for line in r.text.strip().splitlines():
        d = json.loads(line)
        if d.get("summary", {}).get("display_sql"):
            sql = d["summary"]["display_sql"]
        if "result" in d:
            buf = base64.b64decode(d["result"])
            rows = ipc.open_stream(io.BytesIO(buf)).read_all().to_pylist()
        if d.get("status") == "FAILED":
            raise RuntimeError(d.get("error_message", "query failed"))
    return rows, sql


def snowflake(sql):
    import snowflake.connector
    from parquet_to_snowflake import get_snowflake_config
    conn = snowflake.connector.connect(**get_snowflake_config())
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()


def f_bool(v): return {"type": "boolean", "is_negative": not v, "treat_nulls_as_false": True}
def f_str(*v): return {"type": "string", "kind": "EQUALS", "values": list(v)}
def f_num(*v): return {"type": "number", "kind": "EQUALS", "values": list(v)}


results = []

def check(name, omni_val, expected):
    ok = abs(float(omni_val) - float(expected)) < 0.01 if isinstance(expected, (int, float)) else omni_val == expected
    results.append((name, omni_val, expected, ok))
    print(f"{'PASS' if ok else 'FAIL'}  {name}: omni={omni_val} expected={expected}")


# 1. Active teams = 32
rows, sql = omni("dim_teams", [f"{P}dim_teams.team_count"],
                 {f"{P}dim_teams.is_active": f_bool(True)})
check("active teams", rows[0][f"{P}dim_teams.team_count"], 32)

# 2-3. MacKinnon 53 / Matthews 69 regular-season goals 2023-24 (skaters topic)
for player, goals in (("Nathan MacKinnon", 51), ("Auston Matthews", 69)):
    rows, _ = omni("fct_player_game_stats",
                   [f"{P}fct_player_game_stats.total_goals"],
                   {f"{P}dim_players.full_name": f_str(player),
                    f"{P}dim_seasons.season_id": f_num(20232024),
                    f"{P}fct_player_game_stats.game_type": f_str("regular")})
    check(f"{player} G 2023-24", rows[0][f"{P}fct_player_game_stats.total_goals"], goals)

# 4. Ovechkin 65 goals 2007-08 (deep backfill)
rows, _ = omni("fct_player_game_stats",
               [f"{P}fct_player_game_stats.total_goals"],
               {f"{P}dim_players.full_name": f_str("Alex Ovechkin"),
                f"{P}dim_seasons.season_id": f_num(20072008),
                f"{P}fct_player_game_stats.game_type": f_str("regular")})
check("Ovechkin G 2007-08", rows[0][f"{P}fct_player_game_stats.total_goals"], 65)

# 5. 2012-13 lockout: 720 games
rows, _ = omni("fct_games", [f"{P}fct_games.game_count"],
               {f"{P}fct_games.game_type": f_str("Regular"),
                f"{P}dim_seasons.season_id": f_num(20122013)})
# fct_games has season_key; join via dim_seasons requires relationship fct_games->dim_seasons... may fail
check("2012-13 lockout games", rows[0][f"{P}fct_games.game_count"], 720)

# 6. League invariant: total PP goals == total PK goals against (all seasons, regular)
rows, _ = omni("fct_team_game_stats",
               [f"{P}fct_team_game_stats.total_pp_goals",
                f"{P}fct_team_game_stats.total_pk_goals_against"])
pp = rows[0][f"{P}fct_team_game_stats.total_pp_goals"]
pk = rows[0][f"{P}fct_team_game_stats.total_pk_goals_against"]
check("league PP goals == PK GA", pp, pk)

# 7. NSH points 2024-25 vs Snowflake
sf = snowflake("""
    select sum(points_earned) from DBT_ANALYTICS.PROD.FCT_TEAM_GAME_STATS f
    join DBT_ANALYTICS.PROD.DIM_SEASONS s on s.season_key = f.season_key
    where f.team_abv = 'NSH' and s.season_id = 20242025 and f.game_type = 'regular'
""")[0][0]
rows, _ = omni("fct_team_game_stats",
               [f"{P}fct_team_game_stats.total_points_earned"],
               {f"{P}fct_team_game_stats.team_abv": f_str("NSH"),
                f"{P}dim_seasons.season_id": f_num(20242025),
                f"{P}fct_team_game_stats.game_type": f_str("regular")})
check("NSH points 2024-25", rows[0][f"{P}fct_team_game_stats.total_points_earned"], sf)

# 8. Hellebuyck SV% 2023-24 vs Snowflake ratio-of-sums
sf = snowflake("""
    select 1.0*sum(saves)/nullif(sum(shots_faced),0)
    from DBT_ANALYTICS.PROD.FCT_GOALIE_GAME_STATS f
    join DBT_ANALYTICS.PROD.DIM_PLAYERS p on p.player_key = f.player_key
    join DBT_ANALYTICS.PROD.DIM_SEASONS s on s.season_key = f.season_key
    where p.full_name = 'Connor Hellebuyck' and s.season_id = 20232024 and f.game_type = 'regular'
""")[0][0]
rows, _ = omni("fct_goalie_game_stats",
               [f"{P}fct_goalie_game_stats.save_pct_agg"],
               {f"{P}dim_players.full_name": f_str("Connor Hellebuyck"),
                f"{P}dim_seasons.season_id": f_num(20232024),
                f"{P}fct_goalie_game_stats.game_type": f_str("regular")})
check("Hellebuyck SV% 2023-24", round(float(rows[0][f"{P}fct_goalie_gsax.save_pct_agg" if False else f"{P}fct_goalie_game_stats.save_pct_agg"]), 6), round(float(sf), 6))

# 9. Cup odds sum to 1 (latest run)
rows, _ = omni("mart_playoff_odds",
               [f"{P}mart_playoff_odds.cup_odds", f"{P}mart_playoff_odds.team_abv"],
               {f"{P}mart_playoff_odds.is_latest": f_bool(True)}, limit=50)
cup_sum = sum(r[f"{P}mart_playoff_odds.cup_odds"] for r in rows)
check("cup odds sum (latest)", round(cup_sum, 4), 1.0)

# 10. Playoff odds sum to 16 (latest run)
rows, _ = omni("mart_playoff_odds",
               [f"{P}mart_playoff_odds.playoff_odds", f"{P}mart_playoff_odds.team_abv"],
               {f"{P}mart_playoff_odds.is_latest": f_bool(True)}, limit=50)
po_sum = sum(r[f"{P}mart_playoff_odds.playoff_odds"] for r in rows)
check("playoff odds sum (latest)", round(po_sum, 4), 16.0)

# 11. Total xG 2024-25 vs Snowflake
sf = snowflake("""
    select round(sum(xg_raw),2) from DBT_ANALYTICS.PROD.FCT_SHOTS_XG f
    join DBT_ANALYTICS.PROD.DIM_SEASONS s on s.season_key = f.season_key
    where s.season_id = 20242025
""")[0][0]
rows, _ = omni("fct_shots_xg", [f"{P}fct_shots_xg.total_xg"],
               {f"{P}dim_seasons.season_id": f_num(20242025)})
check("total raw xG 2024-25", round(float(rows[0][f"{P}fct_shots_xg.total_xg"]), 2), float(sf))

print()
fails = [r for r in results if not r[3]]
print(f"{len(results) - len(fails)}/{len(results)} PASS")
sys.exit(1 if fails else 0)
