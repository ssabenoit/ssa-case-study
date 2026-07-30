#!/usr/bin/env python
"""Create the five flagship NHL dashboards via the Omni documents API.

Dashboards-as-code: running this recreates League Pulse, Team Page, Player
Explorer, Game Center, and Projections Hub from scratch (31 tiles). After
creation, dashboard-level controls (Team / Game / Season pickers) are added
through the v2 draft API — see omni/README.md for the control grammar.

Env (via repo .env): OMNI_BASE_URL, OMNI_MODEL_ID, OMNI_API_KEY or OMNI_API_KEY_FILE.
"""
import json
import os
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")

KEY = os.getenv("OMNI_API_KEY") or Path(os.environ["OMNI_API_KEY_FILE"]).read_text().strip()
BASE = os.environ["OMNI_BASE_URL"].rstrip("/") + "/api/v1"
H = {"Authorization": f"Bearer {KEY}"}
MODEL = os.environ["OMNI_MODEL_ID"]
P = "dbt_analytics_prod__"
SEASON = "2025-26"
GAME = 2025030415  # 2026 Stanley Cup Final, CAR 4-2 VGK
TEAM = "NSH"

def s_eq(*v): return {"type": "string", "kind": "EQUALS", "values": list(v)}
def n_eq(*v): return {"type": "number", "kind": "EQUALS", "values": [str(x) for x in v]}
def n_gte(v): return {"type": "number", "kind": "GREATER_THAN", "values": [str(v)], "is_inclusive": True}
def b_true(): return {"type": "boolean", "is_negative": False, "treat_nulls_as_false": True}
def srt(col, desc=True): return [{"column_name": col, "sort_descending": desc}]


def tile(name, table, fields, vis="table", filters=None, sorts=None, limit=1000,
         topic=None, pivots=None, desc=None):
    q = {"table": f"{P}{table}", "fields": fields, "limit": limit}
    if filters: q["filters"] = filters
    if sorts: q["sorts"] = sorts
    if topic: q["join_paths_from_topic_name"] = topic
    if pivots: q["pivots"] = pivots
    t = {"name": name, "query": q, "visConfig": {"chartType": vis}}
    if desc: t["description"] = desc
    return t


def F(view, field): return f"{P}{view}.{field}"


DASHBOARDS = {}

# ---------------------------------------------------------------- League Pulse
DASHBOARDS["NHL League Pulse"] = [
    tile("Current Standings", "mart_team_form",
         [F("mart_team_form", c) for c in
          ("league_rank", "team_name", "record", "points", "point_pct", "goal_diff",
           "streak", "last_10_record", "home_record", "road_record")],
         sorts=srt(F("mart_team_form", "league_rank"), desc=False), limit=32),
    tile("Standings Race", "mart_standings_weekly",
         [F("mart_standings_weekly", "date"), F("mart_standings_weekly", "team_abv"),
          F("mart_standings_weekly", "points")],
         vis="lineColor",
         filters={F("mart_standings_weekly", "season_display"): s_eq(SEASON)},
         sorts=srt(F("mart_standings_weekly", "date"), desc=False),
         pivots=[F("mart_standings_weekly", "team_abv")], limit=5000),
    tile("Power Rankings", "mart_team_power_rankings",
         [F("mart_team_power_rankings", c) for c in
          ("ranking_date", "power_rank", "team_name", "power_score", "points_component",
           "goal_component", "momentum_component", "strength_of_schedule", "last_10_points_pct")],
         sorts=[{"column_name": F("mart_team_power_rankings", "ranking_date"), "sort_descending": True},
                {"column_name": F("mart_team_power_rankings", "power_rank"), "sort_descending": False}],
         limit=32),
    tile("Playoff Odds Over Time", "mart_playoff_odds",
         [F("mart_playoff_odds", "run_date"), F("mart_playoff_odds", "team_abv"),
          F("mart_playoff_odds", "playoff_odds")],
         vis="lineColor", pivots=[F("mart_playoff_odds", "team_abv")],
         sorts=srt(F("mart_playoff_odds", "run_date"), desc=False),
         topic="playoff_odds", limit=5000),
    tile("Stanley Cup Odds (Latest Run)", "mart_playoff_odds",
         [F("mart_playoff_odds", "team_abv"), F("mart_playoff_odds", "cup_odds")],
         vis="barColor", filters={F("mart_playoff_odds", "is_latest"): b_true()},
         sorts=srt(F("mart_playoff_odds", "cup_odds")), topic="playoff_odds", limit=16),
    tile("Elo Ratings", "mart_team_elo",
         [F("mart_team_elo", "game_date"), F("mart_team_elo", "team_abv"),
          F("mart_team_elo", "elo")],
         vis="lineColor", filters={F("mart_team_elo", "season_display"): s_eq(SEASON)},
         sorts=srt(F("mart_team_elo", "game_date"), desc=False),
         pivots=[F("mart_team_elo", "team_abv")], topic="elo", limit=10000),
]

# ---------------------------------------------------------------- Team Page
tp = "team_performance"
DASHBOARDS["NHL Team Page"] = [
    tile("Season Form", "mart_team_form",
         [F("mart_team_form", c) for c in
          ("team_name", "record", "points", "point_pct", "league_rank", "division_rank",
           "streak", "last_10_record", "home_record", "road_record", "goal_diff")],
         filters={F("mart_team_form", "team_abv"): s_eq(TEAM)}),
    tile("Goal Differential by Game", tp.replace(tp, "fct_team_game_stats"),
         [F("fct_team_game_stats", "game_date"), F("fct_team_game_stats", "goal_differential")],
         vis="barColor", topic=tp,
         filters={F("fct_team_game_stats", "team_abv"): s_eq(TEAM),
                  F("dim_seasons", "season_display_name"): s_eq(SEASON),
                  F("fct_team_game_stats", "game_type"): s_eq("regular")},
         sorts=srt(F("fct_team_game_stats", "game_date"), desc=False), limit=100),
    tile("Special Teams by Season", "fct_team_game_stats",
         [F("dim_seasons", "season_display_name"),
          F("fct_team_game_stats", "power_play_pct"),
          F("fct_team_game_stats", "penalty_kill_pct_agg")],
         topic=tp,
         filters={F("fct_team_game_stats", "team_abv"): s_eq(TEAM),
                  F("fct_team_game_stats", "game_type"): s_eq("regular")},
         sorts=srt(F("dim_seasons", "season_display_name"), desc=False), limit=25),
    tile("xG Profile by Season", "mart_team_xg",
         [F("mart_team_xg", "season_display"), F("mart_team_xg", "goals_for"),
          F("mart_team_xg", "xg_for"), F("mart_team_xg", "xg_against"),
          F("mart_team_xg", "xg_share"), F("mart_team_shot_metrics", "corsi_pct"),
          F("mart_team_shot_metrics", "fenwick_pct"), F("mart_team_shot_metrics", "pdo")],
         topic="team_season_analytics",
         filters={F("mart_team_xg", "team_abv"): s_eq(TEAM)},
         sorts=srt(F("mart_team_xg", "season_display"), desc=False), limit=25),
    tile("Head-to-Head", "mart_head_to_head",
         [F("mart_head_to_head", c) for c in
          ("opponent_abv", "games_played", "record", "points_earned", "goals_for",
           "goals_against", "goal_differential")],
         filters={F("mart_head_to_head", "team_abv"): s_eq(TEAM),
                  F("mart_head_to_head", "season_display"): s_eq(SEASON),
                  F("mart_head_to_head", "game_type"): s_eq("regular")},
         sorts=srt(F("mart_head_to_head", "points_earned")), limit=32),
    tile("Elo Trajectory (All Seasons)", "mart_team_elo",
         [F("mart_team_elo", "game_date"), F("mart_team_elo", "elo")],
         vis="lineColor", topic="elo",
         filters={F("mart_team_elo", "team_abv"): s_eq(TEAM)},
         sorts=srt(F("mart_team_elo", "game_date"), desc=False), limit=5000),
    tile("Upcoming Games", "fct_upcoming_games",
         [F("fct_upcoming_games", "game_date"), F("fct_upcoming_games", "start_time_eastern"),
          F("fct_upcoming_games", "away_abv"), F("fct_upcoming_games", "home_abv"),
          F("mart_game_projections", "p_away_win"), F("mart_game_projections", "p_home_win")],
         topic="upcoming_games",
         sorts=srt(F("fct_upcoming_games", "game_date"), desc=False), limit=20),
]

# ---------------------------------------------------------------- Player Explorer
DASHBOARDS["NHL Player Explorer"] = [
    tile(f"Points Leaders {SEASON}", "fct_player_game_stats",
         [F("dim_players", "full_name"), F("fct_player_game_stats", "team_abv"),
          F("fct_player_game_stats", "games_played"), F("fct_player_game_stats", "total_goals"),
          F("fct_player_game_stats", "total_assists"), F("fct_player_game_stats", "total_points"),
          F("fct_player_game_stats", "points_per_game"), F("fct_player_game_stats", "points_per_60")],
         topic="skaters",
         filters={F("dim_seasons", "season_display_name"): s_eq(SEASON),
                  F("fct_player_game_stats", "game_type"): s_eq("regular")},
         sorts=srt(F("fct_player_game_stats", "total_points")), limit=25),
    tile(f"Goal Leaders {SEASON}", "fct_player_game_stats",
         [F("dim_players", "full_name"), F("fct_player_game_stats", "total_goals")],
         vis="barColor", topic="skaters",
         filters={F("dim_seasons", "season_display_name"): s_eq(SEASON),
                  F("fct_player_game_stats", "game_type"): s_eq("regular")},
         sorts=srt(F("fct_player_game_stats", "total_goals")), limit=10),
    tile(f"Finishing: Goals vs Expected {SEASON}", "mart_skater_xg",
         [F("mart_skater_xg", "full_name"), F("mart_skater_xg", "expected_goals"),
          F("mart_skater_xg", "goals"), F("mart_skater_xg", "goals_above_expected"),
          F("mart_skater_xg", "shot_attempts")],
         vis="scatter", topic="skater_xg",
         filters={F("mart_skater_xg", "season_display"): s_eq(SEASON)},
         sorts=srt(F("mart_skater_xg", "goals")), limit=150),
    tile(f"Goalie Leaders {SEASON} (min 25 GP)", "fct_goalie_game_stats",
         [F("dim_players", "full_name"), F("fct_goalie_game_stats", "games_played"),
          F("fct_goalie_game_stats", "starts"), F("fct_goalie_game_stats", "save_pct_agg"),
          F("fct_goalie_game_stats", "gaa"), F("fct_goalie_game_stats", "quality_starts"),
          F("fct_goalie_game_stats", "shutouts")],
         topic="goalies",
         filters={F("dim_seasons", "season_display_name"): s_eq(SEASON),
                  F("fct_goalie_game_stats", "game_type"): s_eq("regular"),
                  F("fct_goalie_game_stats", "games_played"): n_gte(25)},
         sorts=srt(F("fct_goalie_game_stats", "save_pct_agg")), limit=15),
    tile(f"GSAx Leaders {SEASON}", "mart_goalie_gsax",
         [F("mart_goalie_gsax", "full_name"), F("mart_goalie_gsax", "shots_faced"),
          F("mart_goalie_gsax", "expected_goals_against"), F("mart_goalie_gsax", "goals_against"),
          F("mart_goalie_gsax", "goals_saved_above_expected"),
          F("mart_goalie_gsax", "gsax_per_100_shots")],
         topic="goalie_gsax",
         filters={F("mart_goalie_gsax", "season_display"): s_eq(SEASON)},
         sorts=srt(F("mart_goalie_gsax", "goals_saved_above_expected")), limit=15),
    tile("Aging Curves by Position", "mart_aging_curves",
         [F("mart_aging_curves", "season_age"), F("mart_aging_curves", "position"),
          F("mart_aging_curves", "avg_points_per_game")],
         vis="lineColor", pivots=[F("mart_aging_curves", "position")],
         sorts=srt(F("mart_aging_curves", "season_age"), desc=False), limit=200),
    tile("Milestone Watch", "mart_milestone_tracker",
         [F("mart_milestone_tracker", c) for c in
          ("full_name", "current_team_abv", "era_goals", "next_goal_milestone",
           "goals_to_milestone", "next_season_proj_goals", "projected_games_to_goal_milestone")],
         topic="milestones",
         sorts=srt(F("mart_milestone_tracker", "goals_to_milestone"), desc=False), limit=15),
]

# ---------------------------------------------------------------- Game Center
shot_types = s_eq("shot-on-goal", "missed-shot", "goal")
DASHBOARDS["NHL Game Center"] = [
    tile("Final Score", "fct_team_game_stats",
         [F("fct_team_game_stats", "team_abv"), F("fct_team_game_stats", "goals_for"),
          F("fct_team_game_stats", "shots_for"), F("fct_team_game_stats", "powerplay_goals"),
          F("fct_team_game_stats", "faceoff_win_pct")],
         topic=tp, filters={F("fct_team_game_stats", "game_id"): n_eq(GAME)}, limit=2),
    tile("Shot Map (xG-weighted)", "fct_plays",
         [F("fct_plays", "x_coordinate"), F("fct_plays", "y_coordinate"),
          F("fct_plays", "event_type_name"), F("dim_players", "full_name"),
          F("dim_teams", "team_abv"), F("fct_shots_xg", "xg_raw")],
         vis="scatter", topic="shots",
         filters={F("fct_games", "game_id"): n_eq(GAME),
                  F("fct_plays", "event_type_name"): shot_types},
         limit=300,
         desc="Every unblocked shot attempt; xG from the seeded logistic model."),
    tile("Goals", "fct_plays",
         [F("fct_plays", "period_number"), F("fct_plays", "time_in_period"),
          F("dim_teams", "team_abv"), F("dim_players", "full_name"),
          F("fct_plays", "strength_state")],
         topic="shots",
         filters={F("fct_games", "game_id"): n_eq(GAME),
                  F("fct_plays", "event_type_name"): s_eq("goal")},
         sorts=[{"column_name": F("fct_plays", "period_number"), "sort_descending": False},
                {"column_name": F("fct_plays", "time_in_period"), "sort_descending": False}],
         limit=30),
    tile("Team Shot Quality", "fct_plays",
         [F("dim_teams", "team_abv"), F("fct_shots_xg", "shot_count"),
          F("fct_shots_xg", "total_xg"), F("fct_shots_xg", "total_goals")],
         vis="barColor", topic="shots",
         filters={F("fct_games", "game_id"): n_eq(GAME),
                  F("fct_plays", "event_type_name"): shot_types}, limit=2),
    tile("Three Stars", "mart_three_stars",
         [F("mart_three_stars", c) for c in
          ("star_number", "player_name", "team_abv", "position", "goals", "assists",
           "points", "save_pct")],
         topic="three_stars",
         filters={F("mart_three_stars", "game_id"): n_eq(GAME)},
         sorts=srt(F("mart_three_stars", "star_number"), desc=False), limit=3),
]

# ---------------------------------------------------------------- Projections Hub
DASHBOARDS["NHL Projections Hub"] = [
    tile("Upcoming Games — Win Probabilities", "fct_upcoming_games",
         [F("fct_upcoming_games", "game_date"), F("fct_upcoming_games", "start_time_eastern"),
          F("fct_upcoming_games", "away_abv"), F("fct_upcoming_games", "home_abv"),
          F("mart_game_projections", "p_away_win"), F("mart_game_projections", "p_home_win")],
         topic="upcoming_games",
         sorts=srt(F("fct_upcoming_games", "game_date"), desc=False), limit=15),
    tile("Season Simulation (Latest Run)", "mart_playoff_odds",
         [F("mart_playoff_odds", c) for c in
          ("team_name", "exp_points", "playoff_odds", "division_odds",
           "presidents_odds", "cup_odds")],
         topic="playoff_odds", filters={F("mart_playoff_odds", "is_latest"): b_true()},
         sorts=srt(F("mart_playoff_odds", "playoff_odds")), limit=32),
    tile("Stanley Cup Odds", "mart_playoff_odds",
         [F("mart_playoff_odds", "team_abv"), F("mart_playoff_odds", "cup_odds")],
         vis="barColor", topic="playoff_odds",
         filters={F("mart_playoff_odds", "is_latest"): b_true()},
         sorts=srt(F("mart_playoff_odds", "cup_odds")), limit=16),
    tile("Playoff Odds Over Time", "mart_playoff_odds",
         [F("mart_playoff_odds", "run_date"), F("mart_playoff_odds", "team_abv"),
          F("mart_playoff_odds", "playoff_odds")],
         vis="lineColor", pivots=[F("mart_playoff_odds", "team_abv")],
         topic="playoff_odds",
         sorts=srt(F("mart_playoff_odds", "run_date"), desc=False), limit=5000),
    tile("Projected Player Scoring 2026-27", "mart_player_projections",
         [F("mart_player_projections", c) for c in
          ("player_name", "team_abv", "position", "current_age", "proj_gp",
           "proj_goals", "proj_assists", "proj_points")],
         topic="player_projections",
         sorts=srt(F("mart_player_projections", "proj_points")), limit=25),
    tile("Model Calibration (Brier by Month)", "mart_projection_accuracy",
         [F("mart_projection_accuracy", c) for c in
          ("run_month", "games_graded", "avg_predicted_home_win", "actual_home_win_rate",
           "brier_score", "favorite_hit_rate")],
         topic="projection_accuracy",
         sorts=srt(F("mart_projection_accuracy", "run_month"), desc=False), limit=50,
         desc="The engine grading itself: predicted vs actual home-win rates."),
]

# ---------------------------------------------------------------- create
created = {}
for name, tiles in DASHBOARDS.items():
    for attempt in range(5):
        r = requests.post(f"{BASE}/documents", headers=H,
                          json={"modelId": MODEL, "name": name,
                                "queryPresentations": tiles}, timeout=300)
        if r.status_code != 429:
            break
        time.sleep(20)
    if r.ok:
        d = r.json()
        doc = d.get("document", d)
        created[name] = {"identifier": doc.get("identifier"),
                         "dashboardId": doc.get("dashboardId"),
                         "workbookId": doc.get("workbookId")}
        print(f"OK   {name}: {json.dumps(created[name])}")
    else:
        print(f"FAIL {name}: {r.status_code} {r.text[:400]}")
    time.sleep(3)

print(json.dumps(created, indent=2))
