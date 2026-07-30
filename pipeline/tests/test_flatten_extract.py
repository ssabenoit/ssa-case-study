"""Unit tests for the flatten shim — the contract between the extractor's
nested output and the warehouse's flat uppercase schema."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.flatten_extract import explode_play_by_play, flatten_record


def test_nested_json_strings_flatten_with_underscores_and_uppercase():
    rec = {
        "id": 2023020001,
        "awayTeam": json.dumps({
            "id": 13, "abbrev": "FLA", "score": 2, "sog": 35,
            "placeName": {"default": "Florida"},
        }),
        "gameOutcome": json.dumps({"lastPeriodType": "REG"}),
    }
    out = flatten_record(rec)
    assert out["ID"] == 2023020001
    assert out["AWAYTEAM_ABBREV"] == "FLA"
    assert out["AWAYTEAM_PLACENAME_DEFAULT"] == "Florida"
    assert out["GAMEOUTCOME_LASTPERIODTYPE"] == "REG"


def test_lists_become_json_strings():
    rec = {"summary": json.dumps({"threeStars": [{"star": 1, "playerId": 8477492}]})}
    out = flatten_record(rec)
    stars = json.loads(out["SUMMARY_THREESTARS"])
    assert stars[0]["playerId"] == 8477492


def test_non_json_strings_pass_through():
    out = flatten_record({"gameDate": "2023-10-10", "venue": "[unusual but not json"})
    assert out["GAMEDATE"] == "2023-10-10"
    assert out["VENUE"] == "[unusual but not json"


def test_play_by_play_explodes_one_row_per_event_with_game_id():
    records = [{
        "id": 2023020001,
        "plays": json.dumps([
            {"eventId": 8, "typeDescKey": "faceoff",
             "details": {"eventOwnerTeamId": 13, "xCoord": 0}},
            {"eventId": 9, "typeDescKey": "shot-on-goal",
             "details": {"shootingPlayerId": 8477492}},
        ]),
    }]
    rows = explode_play_by_play(records, "2026-01-01T00:00:00+00:00")
    assert len(rows) == 2
    assert all(r["GAME_ID"] == 2023020001 for r in rows)
    assert rows[0]["DETAILS_EVENTOWNERTEAMID"] == 13
    assert rows[1]["DETAILS_SHOOTINGPLAYERID"] == 8477492
    assert rows[0]["_LOADED_AT"] == "2026-01-01T00:00:00+00:00"
