#!/usr/bin/env python
"""Push the version-controlled Omni semantic model (omni/model/) to the Omni API.

The files in omni/model/ mirror Omni YAML fileNames exactly:
  relationships                      global join graph
  DBT_ANALYTICS.<SCHEMA>/<t>.view    view extensions (measures, labels, hides)
  <name>.topic                       topics

Usage:
  python omni/push_model.py            # push all files, then validate
  python omni/push_model.py --dry-run  # list what would be pushed
  python omni/push_model.py --validate # validate only

Env (via repo .env): OMNI_BASE_URL, OMNI_MODEL_ID, OMNI_API_KEY or OMNI_API_KEY_FILE.
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = Path(__file__).resolve().parent / "model"
load_dotenv(REPO_ROOT / ".env")


def env():
    base = os.environ["OMNI_BASE_URL"].rstrip("/")
    model_id = os.environ["OMNI_MODEL_ID"]
    key = os.getenv("OMNI_API_KEY")
    key_file = os.getenv("OMNI_API_KEY_FILE")
    if not key and key_file:
        key = Path(key_file).read_text().strip()
    if not key:
        sys.exit("Set OMNI_API_KEY or OMNI_API_KEY_FILE")
    return base, model_id, {"Authorization": f"Bearer {key}"}


def push(base, model_id, headers):
    files = sorted(p for p in MODEL_DIR.rglob("*") if p.is_file())
    failures = []
    for p in files:
        file_name = str(p.relative_to(MODEL_DIR))
        for attempt in range(6):
            resp = requests.post(
                f"{base}/api/v1/models/{model_id}/yaml",
                headers=headers,
                json={
                    "fileName": file_name,
                    "yaml": p.read_text(),
                    "mode": "extension",
                    "commitMessage": f"sync {file_name} from repo omni/model/",
                },
                timeout=120,
            )
            if resp.status_code != 429:
                break
            wait = int(resp.headers.get("Retry-After", 15 * (attempt + 1)))
            print(f"{file_name}: rate limited, retrying in {wait}s")
            time.sleep(wait)
        status = "ok" if resp.ok else f"FAILED {resp.status_code}: {resp.text[:200]}"
        print(f"{file_name}: {status}")
        if not resp.ok:
            failures.append(file_name)
    return failures


def validate(base, model_id, headers):
    resp = requests.get(f"{base}/api/v1/models/{model_id}/validate",
                        headers=headers, timeout=300)
    resp.raise_for_status()
    issues = resp.json()
    print(json.dumps(issues, indent=2))
    return issues


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--validate", action="store_true", help="validate only")
    args = parser.parse_args()

    if args.dry_run:
        for p in sorted(MODEL_DIR.rglob("*")):
            if p.is_file():
                print(p.relative_to(MODEL_DIR))
        return

    base, model_id, headers = env()
    if not args.validate:
        failures = push(base, model_id, headers)
        if failures:
            sys.exit(f"{len(failures)} file(s) failed to push: {failures}")
    validate(base, model_id, headers)


if __name__ == "__main__":
    main()
