#!/usr/bin/env python
"""Flatten nested NHL extraction parquet into the warehouse's flat schema.

The extractor (nhl_to_parquet.py) serializes nested API payloads as JSON
strings inside parquet columns, but the warehouse staging tables use the
Airbyte-style flat convention the project was originally loaded with:

- dicts flatten with '_' separators (awayTeam.placeName.default ->
  AWAYTEAM_PLACENAME_DEFAULT)
- lists become JSON strings (summary.threeStars -> SUMMARY_THREESTARS)
- all column names are uppercased (the loader's INSERT uses quoted-uppercase
  identifiers, so case must match exactly)
- play_by_play explodes to one row per event with GAME_ID carried over
- every row gets a _LOADED_AT timestamp (staging dedup keys on it)

This module is the only bridge between the extractor's output and
parquet_to_snowflake.py — run it on every extract directory before loading.

Usage: python pipeline/flatten_extract.py <in_dir> <out_dir>
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def maybe_json(value):
    """The extractor serializes nested payloads as JSON strings — parse them."""
    if isinstance(value, str) and value[:1] in "{[":
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            return value
    return value


def flatten_value(prefix: str, value, out: dict):
    value = maybe_json(value)
    if isinstance(value, dict):
        for k, v in value.items():
            flatten_value(f"{prefix}_{k}" if prefix else k, v, out)
    elif isinstance(value, (list, tuple)):
        out[prefix.upper()] = json.dumps(list(value), default=str)
    else:
        out[prefix.upper()] = value


def flatten_record(rec: dict) -> dict:
    out = {}
    for k, v in rec.items():
        # pandas gives numpy NaN for missing values; treat as null
        if v is None or (isinstance(v, float) and pd.isna(v)):
            out[k.upper()] = None
        else:
            flatten_value(k, v, out)
    return out


def explode_play_by_play(records: list, loaded_at: str) -> list:
    """One row per event, GAME_ID carried from the game-level record."""
    rows = []
    for rec in records:
        game_id = rec.get("id")
        plays = rec.get("plays")
        if plays is None or (isinstance(plays, float) and pd.isna(plays)):
            continue
        if isinstance(plays, str):
            plays = json.loads(plays)
        for play in list(plays):
            row = flatten_record(play if isinstance(play, dict) else {})
            row["GAME_ID"] = game_id
            row["_LOADED_AT"] = loaded_at
            rows.append(row)
    return rows


def flatten_directory(in_dir: str, out_dir: str) -> dict:
    """Flatten every parquet in in_dir into out_dir. Returns {table: rowcount}."""
    src, dst = Path(in_dir), Path(out_dir)
    dst.mkdir(parents=True, exist_ok=True)
    loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
    results = {}

    for pq in sorted(src.glob("*.parquet")):
        df = pd.read_parquet(pq)
        records = df.to_dict("records")
        if pq.stem == "play_by_play":
            rows = explode_play_by_play(records, loaded_at)
        else:
            rows = []
            for rec in records:
                row = flatten_record(rec)
                row["_LOADED_AT"] = loaded_at
                rows.append(row)

        flat = pd.DataFrame(rows)
        flat.to_parquet(dst / pq.name, index=False)
        results[pq.stem] = len(flat)
        print(f"{pq.stem}: {len(df)} -> {len(flat)} rows, {len(flat.columns)} cols")

    return results


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(2)
    flatten_directory(sys.argv[1], sys.argv[2])
