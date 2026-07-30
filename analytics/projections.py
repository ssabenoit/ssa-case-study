#!/usr/bin/env python
"""Projections engine v1: game win probabilities, Monte Carlo season
simulation (playoff/division/Presidents'/Cup odds), and player rest-of-season
projections. Appends run-dated rows to ANALYTICS.* so accuracy can be graded
over time.

Usage: python analytics/projections.py [--schema PROD] [--sims 10000]
"""
import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(REPO_ROOT / ".env")

HOME_ADV = 25.0
SEASON_CARRYOVER = 0.7
BASE = 1505.0
P_OVERTIME = 0.23          # share of games reaching OT/SO (loser point)
TEAM_NOISE_SD = 30.0       # per-sim rating uncertainty


def connect():
    import snowflake.connector
    from parquet_to_snowflake import get_snowflake_config
    return snowflake.connector.connect(**get_snowflake_config())


def q(conn, sql):
    df = pd.read_sql(sql, conn)
    df.columns = [c.lower() for c in df.columns]
    return df


def p_win(elo_a, elo_b):
    return 1.0 / (1 + 10 ** ((elo_b - elo_a) / 400.0))


def load_inputs(conn, schema):
    ratings = q(conn, """
        select home_team as team, home_elo_post as elo, game_date from DBT_ANALYTICS.ANALYTICS.TEAM_ELO_DAILY
        qualify row_number() over (partition by home_team order by game_date desc, game_id desc) = 1
        union all
        select away_team, away_elo_post, game_date from DBT_ANALYTICS.ANALYTICS.TEAM_ELO_DAILY
        qualify row_number() over (partition by away_team order by game_date desc, game_id desc) = 1
    """)
    ratings = ratings.sort_values("game_date").groupby("team").last()["elo"].to_dict()

    schedule = q(conn, f"""
        select id as game_id, season, game_date, home_abv, away_abv
        from DBT_ANALYTICS.{schema}.STG_NHL__SEASON_SCHEDULES
        where game_type = 2 and game_state in ('FUT', 'PRE')
        order by game_date, game_id
    """)

    divisions = q(conn, f"""
        select team_abv, division, conference
        from DBT_ANALYTICS.{schema}.DIM_TEAMS
        where is_active
    """)
    return ratings, schedule, divisions


def season_rollover(ratings):
    return {t: BASE + SEASON_CARRYOVER * (e - BASE) for t, e in ratings.items()}


def game_projections(ratings, schedule, run_ts):
    rows = []
    for g in schedule.itertuples(index=False):
        eh = ratings.get(g.home_abv, BASE)
        ea = ratings.get(g.away_abv, BASE)
        rows.append((run_ts[:10], int(g.game_id), str(g.game_date), int(g.season),
                     g.home_abv, g.away_abv, round(eh, 1), round(ea, 1),
                     round(p_win(eh + HOME_ADV, ea), 4)))
    return rows


def simulate_season(ratings, schedule, divisions, sims, rng):
    teams = sorted(divisions["team_abv"])
    t_idx = {t: i for i, t in enumerate(teams)}
    div_of = divisions.set_index("team_abv")["division"].to_dict()
    conf_of = divisions.set_index("team_abv")["conference"].to_dict()

    home_i = schedule["home_abv"].map(t_idx).to_numpy()
    away_i = schedule["away_abv"].map(t_idx).to_numpy()
    base_elo = np.array([ratings.get(t, BASE) for t in teams])

    n_teams = len(teams)
    playoff_ct = np.zeros(n_teams)
    div_win_ct = np.zeros(n_teams)
    pres_ct = np.zeros(n_teams)
    cup_ct = np.zeros(n_teams)
    points_sum = np.zeros(n_teams)

    div_teams = {d: [t_idx[t] for t in teams if div_of[t] == d] for d in set(div_of.values())}
    conf_divs = {}
    for d, ts in div_teams.items():
        conf = conf_of[teams[ts[0]]]
        conf_divs.setdefault(conf, []).append(d)

    for _ in range(sims):
        elo = base_elo + rng.normal(0, TEAM_NOISE_SD, n_teams)
        ph = 1.0 / (1 + 10 ** ((elo[away_i] - elo[home_i] - HOME_ADV) / 400.0))
        home_won = rng.random(len(ph)) < ph
        went_ot = rng.random(len(ph)) < P_OVERTIME

        pts = np.zeros(n_teams)
        np.add.at(pts, home_i, np.where(home_won, 2, np.where(went_ot, 1, 0)))
        np.add.at(pts, away_i, np.where(~home_won, 2, np.where(went_ot, 1, 0)))
        pts_j = pts + rng.random(n_teams) * 0.01  # tiebreak jitter
        points_sum += pts

        pres_ct[np.argmax(pts_j)] += 1
        qualified = {}
        for conf, divs in conf_divs.items():
            conf_qual = []
            wc_pool = []
            for d in divs:
                order = sorted(div_teams[d], key=lambda i: -pts_j[i])
                div_win_ct[order[0]] += 1
                conf_qual += order[:3]
                wc_pool += order[3:]
            wc = sorted(wc_pool, key=lambda i: -pts_j[i])[:2]
            qualified[conf] = conf_qual + wc
        for conf_q in qualified.values():
            playoff_ct[np.array(conf_q)] += 1

        # playoff bracket: seed by points within conference, best-of-7 sims
        finalists = []
        for conf_q in qualified.values():
            seeds = sorted(conf_q, key=lambda i: -pts_j[i])
            alive = seeds
            while len(alive) > 1:
                nxt = []
                half = len(alive) // 2
                for k in range(half):
                    a, b = alive[k], alive[-(k + 1)]
                    pa = 1.0 / (1 + 10 ** ((elo[b] - elo[a]) / 400.0))
                    wins_a = (rng.random(7) < pa).cumsum()
                    nxt.append(a if wins_a[-1] >= 4 else b)
                alive = sorted(nxt, key=lambda i: -pts_j[i])
            finalists.append(alive[0])
        a, b = finalists
        pa = 1.0 / (1 + 10 ** ((elo[b] - elo[a]) / 400.0))
        cup_ct[a if (rng.random(7) < pa).sum() >= 4 else b] += 1

    return pd.DataFrame({
        "team": teams,
        "exp_points": points_sum / sims,
        "playoff_odds": playoff_ct / sims,
        "division_odds": div_win_ct / sims,
        "presidents_odds": pres_ct / sims,
        "cup_odds": cup_ct / sims,
    })


def player_projections(conn, schema, run_ts):
    rates = q(conn, f"""
        with recent as (
            select season_key, player_id, player_name, team_abv,
                   count(*) as gp, sum(goals) as goals, sum(assists) as assists,
                   sum(points) as points,
                   case season_key when 20252026 then 3 when 20242025 then 2 else 1 end as w
            from DBT_ANALYTICS.{schema}.FCT_PLAYER_GAME_STATS
            where game_type = 'regular' and season_key >= 20232024
            group by 1, 2, 3, 4
        )
        select player_id,
               max_by(player_name, season_key) as player_name,
               max_by(team_abv, season_key) as team_abv,
               sum(gp * w) / sum(w) as w_gp,
               sum(goals * w) / nullif(sum(gp * w), 0) as g_rate,
               sum(assists * w) / nullif(sum(gp * w), 0) as a_rate,
               sum(gp) as sample_gp
        from recent
        group by player_id
        having sum(gp) >= 30
    """)
    # regress rates toward the population mean with a 40-game prior
    prior_g = (rates["g_rate"] * rates["sample_gp"]).sum() / rates["sample_gp"].sum()
    prior_a = (rates["a_rate"] * rates["sample_gp"]).sum() / rates["sample_gp"].sum()
    k = 40
    rates["g_rate_r"] = (rates["g_rate"] * rates["sample_gp"] + prior_g * k) / (rates["sample_gp"] + k)
    rates["a_rate_r"] = (rates["a_rate"] * rates["sample_gp"] + prior_a * k) / (rates["sample_gp"] + k)
    rates["proj_gp"] = rates["w_gp"].clip(upper=82).round(0)

    rows = []
    for r in rates.itertuples(index=False):
        pg = float(r.proj_gp)
        rows.append((run_ts[:10], int(r.player_id), r.player_name, r.team_abv, int(pg),
                     round(float(r.g_rate_r) * pg, 1), round(float(r.a_rate_r) * pg, 1),
                     round((float(r.g_rate_r) + float(r.a_rate_r)) * pg, 1)))
    return rows


def write(conn, table, ddl_cols, rows):
    cur = conn.cursor()
    cur.execute("create schema if not exists DBT_ANALYTICS.ANALYTICS")
    cur.execute(f"create table if not exists DBT_ANALYTICS.ANALYTICS.{table} ({ddl_cols})")
    placeholders = ",".join(["%s"] * len(rows[0]))
    cur.executemany(f"insert into DBT_ANALYTICS.ANALYTICS.{table} values ({placeholders})", rows)
    print(f"wrote {len(rows):,} rows -> {table}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", default="PROD")
    parser.add_argument("--sims", type=int, default=10000)
    args = parser.parse_args()

    run_ts = datetime.now(timezone.utc).isoformat()
    conn = connect()
    try:
        ratings, schedule, divisions = load_inputs(conn, args.schema)
        # projecting a not-yet-started season: apply the between-season regression
        latest_completed = 20252026
        if schedule["season"].max() > latest_completed:
            ratings = season_rollover(ratings)
        print(f"{len(schedule):,} scheduled games; {len(ratings)} rated teams")

        gp_rows = game_projections(ratings, schedule, run_ts)
        write(conn, "GAME_PROJECTIONS",
              "run_date date, game_id int, game_date string, season int, home string, "
              "away string, home_elo float, away_elo float, p_home_win float", gp_rows)

        rng = np.random.default_rng(20262027)
        sim = simulate_season(ratings, schedule, divisions, args.sims, rng)
        sim_rows = [(run_ts[:10], int(schedule["season"].max()), r.team,
                     round(r.exp_points, 1), round(r.playoff_odds, 4),
                     round(r.division_odds, 4), round(r.presidents_odds, 4),
                     round(r.cup_odds, 4))
                    for r in sim.itertuples(index=False)]
        write(conn, "SIM_TEAM_SEASON",
              "run_date date, season int, team string, exp_points float, playoff_odds float, "
              "division_odds float, presidents_odds float, cup_odds float", sim_rows)

        pl_rows = player_projections(conn, args.schema, run_ts)
        write(conn, "PLAYER_PROJECTIONS",
              "run_date date, player_id int, player_name string, team_abv string, "
              "proj_gp int, proj_goals float, proj_assists float, proj_points float", pl_rows)

        top = sim.sort_values("cup_odds", ascending=False).head(6)
        print("cup odds leaders:")
        for r in top.itertuples(index=False):
            print(f"  {r.team}: cup {r.cup_odds:.1%}  playoffs {r.playoff_odds:.1%}  exp pts {r.exp_points:.0f}")
        print(f"sanity: playoff odds sum = {sim['playoff_odds'].sum():.2f} (expect 16); "
              f"cup odds sum = {sim['cup_odds'].sum():.3f} (expect 1)")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
