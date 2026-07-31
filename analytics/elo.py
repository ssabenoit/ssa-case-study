#!/usr/bin/env python
"""Team Elo ratings over every league game, written to ANALYTICS.TEAM_ELO_DAILY.

FiveThirtyEight-style NHL Elo: K scaled by margin of victory, home-ice
advantage in the expectation, 30% regression to the mean between seasons.
Ratings converge within ~2 seasons, so the 2007-08 start gives fully
converged modern ratings.

Usage: python analytics/elo.py [--schema PROD]
"""
import argparse
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(REPO_ROOT / ".env")

BASE = 1505.0
K = 12.0        # grid-searched on 2015+ log-loss
HOME_ADV = 25.0  # grid-searched on 2015+ log-loss
SEASON_CARRYOVER = 0.7


def expected(elo_a, elo_b):
    return 1.0 / (1 + 10 ** ((elo_b - elo_a) / 400.0))


def mov_multiplier(goal_diff, elo_diff_winner):
    # 538's autocorrelation-adjusted margin multiplier
    return 0.6686 * (abs(goal_diff) + 1) ** 0.8 / (7.5 + 0.006 * elo_diff_winner)


def connect():
    import snowflake.connector
    from parquet_to_snowflake import get_snowflake_config
    return snowflake.connector.connect(**get_snowflake_config())


def load_games(schema):
    conn = connect()
    try:
        return pd.read_sql(f"""
            select lg.game_id, lg.season, lg.game_date, lg.game_type,
                   lg.home_team_abv, lg.away_team_abv,
                   h.goals as home_goals, a.goals as away_goals,
                   coalesce(lg.last_period_type, 'REG') as last_period
            from DBT_ANALYTICS.{schema}.INT__LEAGUE_GAMES lg
            inner join DBT_ANALYTICS.{schema}.INT__TEAM_PER_GAME_STATS h
                on h.game_id = lg.game_id and h.type = 'home'
            inner join DBT_ANALYTICS.{schema}.INT__TEAM_PER_GAME_STATS a
                on a.game_id = lg.game_id and a.type = 'away'
            order by lg.game_date, lg.game_id
        """, conn)
    finally:
        conn.close()


def run_elo(games: pd.DataFrame):
    games.columns = [c.lower() for c in games.columns]
    ratings = defaultdict(lambda: BASE)
    current_season = None
    rows = []
    for g in games.itertuples(index=False):
        if g.season != current_season:
            for team in list(ratings):
                ratings[team] = BASE + SEASON_CARRYOVER * (ratings[team] - BASE)
            current_season = g.season

        home, away = g.home_team_abv, g.away_team_abv
        e_home = expected(ratings[home] + HOME_ADV, ratings[away])
        home_won = g.home_goals > g.away_goals
        margin = abs(g.home_goals - g.away_goals) or 1
        elo_diff_winner = (ratings[home] - ratings[away]) if home_won else (ratings[away] - ratings[home])
        shift = K * mov_multiplier(margin, elo_diff_winner) * ((1 if home_won else 0) - e_home)
        ratings[home] += shift
        ratings[away] -= shift

        rows.append((str(g.game_date), int(g.season), g.game_type, int(g.game_id),
                     home, round(ratings[home], 2), away, round(ratings[away], 2),
                     round(e_home, 4), bool(home_won)))
    return ratings, rows


def write_rows(rows, run_ts):
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute("create schema if not exists DBT_ANALYTICS.ANALYTICS")
        cur.execute("""
            create or replace table DBT_ANALYTICS.ANALYTICS.TEAM_ELO_DAILY (
                game_date date, season int, game_type string, game_id int,
                home_team string, home_elo_post float,
                away_team string, away_elo_post float,
                p_home_win float, home_won boolean, _loaded_at string
            )
        """)
        cur.executemany(
            "insert into DBT_ANALYTICS.ANALYTICS.TEAM_ELO_DAILY values "
            "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'" + run_ts + "')",
            rows,
        )
        print(f"wrote {len(rows):,} elo rows")
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", default="PROD")
    args = parser.parse_args()

    games = load_games(args.schema)
    print(f"loaded {len(games):,} games")
    ratings, rows = run_elo(games)
    run_ts = datetime.now(timezone.utc).isoformat()
    write_rows(rows, run_ts)

    top = sorted(ratings.items(), key=lambda kv: -kv[1])[:8]
    print("current top ratings:")
    for team, elo in top:
        print(f"  {team}: {elo:.0f}")

    # calibration: predicted home win prob vs actual, by decile
    df = pd.DataFrame(rows, columns=["d", "s", "t", "g", "h", "he", "a", "ae", "p", "won"])
    df["bucket"] = pd.cut(df["p"], bins=[0, .35, .45, .5, .55, .65, 1.0])
    rel = df.groupby("bucket", observed=True).agg(pred=("p", "mean"), actual=("won", "mean"), n=("won", "size"))
    print("home-win calibration:")
    for _, r in rel.iterrows():
        print(f"  pred {r['pred']:.3f} -> actual {r['actual']:.3f}  (n={int(r['n'])})")


if __name__ == "__main__":
    main()
