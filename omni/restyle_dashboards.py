#!/usr/bin/env python
"""Apply proven chart configs (Dealio-style 'basic' visType) across the NHL dashboards."""
import json
import time

import requests

KEY = open("/Users/nickl/Projects/nhl-dashboard/.omni_api_key").read().strip()
H = {"Authorization": f"Bearer {KEY}", "Content-Type": "application/json"}
V2 = "https://southshorellc.omniapp.co/api/v2"
V1 = "https://southshorellc.omniapp.co/api/v1"
P = "dbt_analytics_prod__"

NAVY = "#1D3557"
BLUE = "#457B9D"
RED = "#E63946"
TEAL = "#2A9D8F"
GOLD = "#E9C46A"


def base_config():
    return {"_dependentAxis": "y", "behaviors": {"stackMultiMark": False},
            "configType": "cartesian", "version": 0,
            "y": {"axis": {"title": {"value": " "}}},
            "color": {"axis": {"title": {"value": " "}}, "legendPosition": "bottom"}}


def column(cat, series, colors):
    cfg = base_config()
    cfg["mark"] = {"type": "bar"}
    cfg["x"] = {"axis": {"title": {"value": " "}}, "field": {"name": cat}}
    cfg["series"] = [{"field": {"name": s}, "manual": True,
                      "mark": {"_mark_color": c, "type": "bar"}, "yAxis": "y"}
                     for s, c in zip(series, colors)]
    cfg["tooltip"] = [{"field": {"name": cat}}] + [{"field": {"name": s}} for s in series]
    return {"chartType": "column", "fields": [cat, *series], "version": 0,
            "visConfig": {"visType": "basic", "config": cfg}}


def line(x, series, colors, color_dim=None, chart="line"):
    cfg = base_config()
    cfg["mark"] = {"type": "line"}
    cfg["x"] = {"axis": {"title": {"value": " "}}, "field": {"name": x}}
    cfg["series"] = [{"field": {"name": s}, "manual": True,
                      "mark": {"_mark_color": c, "type": "line"}, "yAxis": "y"}
                     for s, c in zip(series, colors)]
    fields = [x, *series]
    tooltip = [{"field": {"name": x}}] + [{"field": {"name": s}} for s in series]
    if color_dim:
        cfg["color"] = {"field": {"name": color_dim}, "manual": True,
                        "legendPosition": "bottom"}
        fields.insert(1, color_dim)
        tooltip.insert(1, {"field": {"name": color_dim}})
        chart = "lineColor"
    cfg["tooltip"] = tooltip
    return {"chartType": chart, "fields": fields, "version": 0,
            "visConfig": {"visType": "basic", "config": cfg}}


def scatter(x, y, color_dim=None):
    cfg = base_config()
    cfg["mark"] = {"type": "point"}
    cfg["x"] = {"axis": {"title": {"value": " "}}, "field": {"name": x}}
    cfg["series"] = [{"field": {"name": y}, "manual": True,
                      "mark": {"type": "point"}, "yAxis": "y"}]
    fields = [x, y]
    chart = "point"
    if color_dim:
        cfg["color"] = {"field": {"name": color_dim}, "manual": True,
                        "legendPosition": "bottom"}
        fields.append(color_dim)
        chart = "pointColor"
    cfg["tooltip"] = [{"field": {"name": f}} for f in fields]
    return {"chartType": chart, "fields": fields, "version": 0,
            "visConfig": {"visType": "basic", "config": cfg}}


def render(ident, out):
    r = requests.post(f"{V1}/dashboards/{ident}/download", headers=H,
                      json={"format": "png"}, timeout=120)
    job = r.json()["job_id"]
    st = None
    for _ in range(50):
        time.sleep(5)
        st = requests.get(f"{V1}/dashboards/{ident}/download/{job}/status",
                          headers=H, timeout=30).json().get("status")
        if st not in ("in_progress", "pending", "queued"):
            break
    if st == "complete":
        rr = requests.get(f"{V1}/dashboards/{ident}/download/{job}", headers=H, timeout=120)
        open(out, "wb").write(rr.content)
    return st


def apply(ident, mutate, summary):
    doc = requests.get(f"{V2}/documents/{ident}", headers=H, timeout=60).json()
    qp = doc["queryPresentations"]["data"]
    patch = mutate(qp)
    r = requests.patch(f"{V2}/documents/{ident}/draft", headers=H,
                       json={"queryPresentations": {"data": patch}, "summary": summary},
                       timeout=120)
    p = requests.post(f"{V2}/documents/{ident}/draft/publish", headers=H, timeout=120)
    print(ident, "patch", r.status_code, "publish", p.status_code)
    if not r.ok:
        print("  ", r.text[:300])


def strip_pivots(tile):
    q = dict(tile["query"])
    q["pivots"] = []
    tile["query"] = q
    return tile


# ---------------- League Pulse cad0598c
def league_pulse(qp):
    patch = {}
    by_name = {t["name"]: k for k, t in qp.items()}

    k = by_name["Standings Race"]
    t = strip_pivots(dict(qp[k]))
    t["visConfig"] = line(f"{P}mart_standings_weekly.date",
                          [f"{P}mart_standings_weekly.points"], [BLUE],
                          color_dim=f"{P}mart_standings_weekly.team_abv")
    patch[k] = t

    k = by_name["Stanley Cup Odds (Latest Run)"]
    t = dict(qp[k])
    t["visConfig"] = column(f"{P}mart_playoff_odds.team_abv",
                            [f"{P}mart_playoff_odds.cup_odds"], [RED])
    patch[k] = t

    k = by_name["Elo Ratings"]
    t = strip_pivots(dict(qp[k]))
    t["visConfig"] = line(f"{P}mart_team_elo.game_date",
                          [f"{P}mart_team_elo.elo"], [BLUE],
                          color_dim=f"{P}mart_team_elo.team_abv")
    patch[k] = t

    k = by_name["Playoff Odds Over Time"]
    t = strip_pivots(dict(qp[k]))
    t["visConfig"] = line(f"{P}mart_playoff_odds.run_date",
                          [f"{P}mart_playoff_odds.playoff_odds"], [BLUE],
                          color_dim=f"{P}mart_playoff_odds.team_abv")
    patch[k] = t
    return patch


# ---------------- Team Page b2c5a0d5
def team_page(qp):
    patch = {}
    by_name = {t["name"]: k for k, t in qp.items()}

    k = by_name["Goal Differential by Game"]
    t = dict(qp[k])
    t["visConfig"] = column(f"{P}fct_team_game_stats.game_date",
                            [f"{P}fct_team_game_stats.goal_differential"], [TEAL])
    patch[k] = t

    k = by_name["Special Teams by Season"]
    t = dict(qp[k])
    t["visConfig"] = line(f"{P}dim_seasons.season_display_name",
                          [f"{P}fct_team_game_stats.power_play_pct",
                           f"{P}fct_team_game_stats.penalty_kill_pct_agg"],
                          [RED, NAVY])
    patch[k] = t

    k = by_name["Elo Trajectory (All Seasons)"]
    t = dict(qp[k])
    t["visConfig"] = line(f"{P}mart_team_elo.game_date",
                          [f"{P}mart_team_elo.elo"], [GOLD])
    patch[k] = t
    return patch


# ---------------- Player Explorer e10f16fa
def player_explorer(qp):
    patch = {}
    by_name = {t["name"]: k for k, t in qp.items()}

    k = by_name["Goal Leaders 2025-26"]
    t = dict(qp[k])
    t["visConfig"] = column(f"{P}dim_players.full_name",
                            [f"{P}fct_player_game_stats.total_goals"], [RED])
    patch[k] = t

    k = by_name["Finishing: Goals vs Expected 2025-26"]
    t = dict(qp[k])
    t["visConfig"] = scatter(f"{P}mart_skater_xg.expected_goals",
                             f"{P}mart_skater_xg.goals")
    patch[k] = t

    k = by_name["Aging Curves by Position"]
    t = strip_pivots(dict(qp[k]))
    t["visConfig"] = line(f"{P}mart_aging_curves.season_age",
                          [f"{P}mart_aging_curves.avg_points_per_game"], [BLUE],
                          color_dim=f"{P}mart_aging_curves.position")
    patch[k] = t
    return patch


# ---------------- Game Center 71430d9d
def game_center(qp):
    patch = {}
    by_name = {t["name"]: k for k, t in qp.items()}

    k = by_name["Shot Map (xG-weighted)"]
    t = dict(qp[k])
    t["visConfig"] = scatter(f"{P}fct_plays.x_coordinate",
                             f"{P}fct_plays.y_coordinate",
                             color_dim=f"{P}dim_teams.team_abv")
    patch[k] = t

    k = by_name["Team Shot Quality"]
    t = dict(qp[k])
    t["visConfig"] = column(f"{P}dim_teams.team_abv",
                            [f"{P}fct_shots_xg.total_xg", f"{P}fct_shots_xg.total_goals"],
                            [BLUE, RED])
    patch[k] = t
    return patch


SCRATCH = "/private/tmp/claude-501/-Users-nickl-Projects-nhl-dashboard/5efcdbd7-dd4a-45df-b617-562ab6c3c8eb/scratchpad"
jobs = [("cad0598c", league_pulse, "league_pulse.png"),
        ("b2c5a0d5", team_page, "team_page.png"),
        ("e10f16fa", player_explorer, "player_explorer.png"),
        ("71430d9d", game_center, "game_center.png")]

for ident, fn, png in jobs:
    apply(ident, fn, "chart restyle: basic visType cartesian configs")
    time.sleep(2)

for ident, fn, png in jobs:
    st = render(ident, f"{SCRATCH}/{png}")
    print(ident, png, "render:", st)
