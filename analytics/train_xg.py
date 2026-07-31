#!/usr/bin/env python
"""Train the expected-goals model and export coefficients as a dbt seed.

Logistic regression over int__shot_features (2010-11+ per the era-quality
rule; earlier seasons held out as a sanity check). The trained coefficients
land in seeds/xg_coefficients.csv, and scoring happens entirely in dbt
(fct_shots_xg) using the exact same feature model — training and serving
cannot drift.

Usage: python analytics/train_xg.py [--schema DBT_DEV] [--holdout 20252026]
"""
import argparse
import csv
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(REPO_ROOT / ".env")

SHOT_TYPES = ["wrist", "slap", "snap", "backhand", "tip-in", "deflected",
              "wrap-around"]  # 'unknown' + anything else = baseline bucket
NUMERIC = ["shot_distance", "log_shot_distance", "shot_angle_abs", "is_power_play",
           "is_shorthanded", "is_rebound", "is_rush"]


def load_shots(schema: str, min_season: int = 20102011) -> pd.DataFrame:
    import snowflake.connector
    from parquet_to_snowflake import get_snowflake_config
    cfg = get_snowflake_config()
    conn = snowflake.connector.connect(**cfg)
    try:
        query = f"""
            select season_key, is_goal, shot_distance, log_shot_distance,
                   shot_angle_abs, shot_type, is_power_play, is_shorthanded,
                   is_rebound, is_rush
            from DBT_ANALYTICS.{schema}.INT__SHOT_FEATURES
            where season_key >= {min_season}
        """.format(min_season=min_season)
        return pd.read_sql(query, conn)
    finally:
        conn.close()


def design_matrix(df: pd.DataFrame):
    df.columns = [c.lower() for c in df.columns]
    x = df[NUMERIC].astype(float).fillna(0.0).copy()
    for st in SHOT_TYPES:
        x[f"shot_type_{st}"] = (df["shot_type"] == st).astype(float)
    return x


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", default="DBT_DEV")
    parser.add_argument("--holdout", type=int, default=20252026)
    parser.add_argument("--min-season", type=int, default=20102011)
    parser.add_argument("--final", action="store_true",
                        help="fit on ALL seasons >= min-season (no holdout) for the production seed")
    args = parser.parse_args()

    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import roc_auc_score, brier_score_loss

    df = load_shots(args.schema, args.min_season)
    df.columns = [c.lower() for c in df.columns]
    print(f"loaded {len(df):,} shots ({df['season_key'].nunique()} seasons)")

    if args.final:
        train = df
        test = df[df["season_key"] == args.holdout]  # metrics only, in-sample
    else:
        train = df[df["season_key"] != args.holdout]
        test = df[df["season_key"] == args.holdout]
    x_train, y_train = design_matrix(train), train["is_goal"].astype(int)
    x_test, y_test = design_matrix(test), test["is_goal"].astype(int)

    model = LogisticRegression(max_iter=2000, C=1.0)
    model.fit(x_train, y_train)

    p_test = model.predict_proba(x_test)[:, 1]
    p_train = model.predict_proba(x_train)[:, 1]
    print(f"train AUC: {roc_auc_score(y_train, p_train):.4f}")
    print(f"holdout ({args.holdout}) AUC: {roc_auc_score(y_test, p_test):.4f}")
    print(f"holdout Brier: {brier_score_loss(y_test, p_test):.5f}")
    print(f"holdout total goals: actual {y_test.sum():,}  predicted {p_test.sum():,.0f} "
          f"({100 * p_test.sum() / max(y_test.sum(), 1) - 100:+.1f}%)")

    # reliability curve (deciles)
    dec = pd.DataFrame({"p": p_test, "y": y_test})
    dec["bucket"] = pd.qcut(dec["p"], 10, duplicates="drop")
    rel = dec.groupby("bucket", observed=True).agg(pred=("p", "mean"), actual=("y", "mean"))
    print("reliability (predicted vs actual by decile):")
    for _, row in rel.iterrows():
        print(f"  {row['pred']:.3f} -> {row['actual']:.3f}")

    out = REPO_ROOT / "seeds" / "xg_coefficients.csv"
    with open(out, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["feature", "coefficient"])
        writer.writerow(["intercept", round(float(model.intercept_[0]), 6)])
        for name, coef in zip(x_train.columns, model.coef_[0]):
            writer.writerow([name, round(float(coef), 6)])
    print(f"wrote {out} ({len(x_train.columns) + 1} coefficients)")


if __name__ == "__main__":
    main()
