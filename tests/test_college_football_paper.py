from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pytest

from opportunity_lab.college_football_paper import (account, collect, decide, game_state, open_db,
                                                    quotes, report, step, value_cents)


CONFIG = json.loads((Path(__file__).resolve().parents[1] / "configs/ncaaf_smu_fsu_20260907.json").read_text())
NOW = datetime(2026, 9, 8, 0, 0, tzinfo=timezone.utc)


def payload():
    return {"header": {"id": CONFIG["event_id"], "competitions": [{
        "status": {"type": {"state": "in", "name": "STATUS_IN_PROGRESS", "completed": False}},
        "competitors": [{"homeAway": "home", "id": "52", "score": "7"},
                        {"homeAway": "away", "id": "2567", "score": "0"}]}]},
        "drives": {"current": {"plays": [{"id": "101", "homeScore": 7, "awayScore": 0,
                                          "wallclock": (NOW - timedelta(seconds=20)).isoformat()}]}},
        "winprobability": [{"playId": "101", "homeWinPercentage": .70, "tiePercentage": 0}]}


def observation():
    return {"game": game_state(payload(), CONFIG, NOW), "fee_metadata_matches": True,
            "transport": {"game": {"ok": True, "duration_seconds": .5}},
            "markets": {side: {"ticker": ticker, "status": "active", "result": "", "valid": True, "settlement_confirmed": True,
                               "quote": {"ask": .5 if side == "home" else .52,
                                         "bid": .49 if side == "home" else .51,
                                         "ask_size": 100, "bid_size": 100}}
                        for side, ticker in CONFIG["markets"].items()}}


def state():
    return {"cash_cents": 100000, "realized_cents": 0, "position": None, "pending": None,
            "entries": 0, "exits": 0, "peak_equity_cents": 100000, "max_drawdown_cents": 0,
            "halted": False, "last_entry_play": None, "last_exit_at": None}


def enter():
    s, obs = state(), observation()
    assert decide(s, obs, CONFIG, NOW)[-1]["action"] == "stage_buy"
    assert decide(s, obs, CONFIG, NOW + timedelta(seconds=15))[-1]["action"] == "buy"
    return s, obs


def test_game_identity_play_alignment_age_and_score_gates():
    good = payload()
    assert not game_state(good, CONFIG, NOW)["blockers"]
    changed = deepcopy(good)
    changed["header"]["competitions"][0]["competitors"][0]["id"] = "WRONG"
    assert "game_or_team_identity_mismatch" in game_state(changed, CONFIG, NOW)["blockers"]
    changed = deepcopy(good)
    changed["winprobability"][-1]["playId"] = "100"
    assert "probability_not_for_latest_play" in game_state(changed, CONFIG, NOW)["blockers"]
    assert "play_timestamp_stale" in game_state(good, CONFIG, NOW + timedelta(minutes=2))["blockers"]
    changed = deepcopy(good)
    changed["header"]["competitions"][0]["competitors"][0]["score"] = "14"
    assert "score_and_probability_out_of_sync" in game_state(changed, CONFIG, NOW)["blockers"]


def test_no_pregame_fallback_and_invalid_probability():
    data = payload()
    data["winprobability"] = []
    data["predictor"] = {"homeTeam": {"gameProjection": "90"}}
    assert game_state(data, CONFIG, NOW)["home_probability"] is None
    data = payload()
    data["winprobability"][0]["homeWinPercentage"] = float("nan")
    assert game_state(data, CONFIG, NOW)["home_probability"] is None


def test_costs_and_orderbook_conversion():
    assert value_cents(.5, 10, CONFIG, True) == 528
    assert value_cents(.5, 10, CONFIG, False) == 472
    q = quotes({"orderbook_fp": {"yes_dollars": [[".49", "10"]], "no_dollars": [[".50", "20"]]}})
    assert q == {"ask": .5, "bid": .49, "ask_size": 20, "bid_size": 10}
    assert quotes({"orderbook_fp": {"yes_dollars": [[".6", "10"]], "no_dollars": [[".6", "20"]]}}) is None


def test_confirmation_waits_and_cancels_when_price_or_signal_changes():
    s, obs = state(), observation()
    decide(s, obs, CONFIG, NOW)
    assert s["position"] is None
    assert decide(s, obs, CONFIG, NOW + timedelta(seconds=5))[-1]["reason"] == "confirmation_delay"
    obs["markets"]["home"]["quote"]["ask"] = .56
    assert decide(s, obs, CONFIG, NOW + timedelta(seconds=15))[-1]["action"] == "cancel_paper_intent"
    assert s["cash_cents"] == 100000
    s, obs = state(), observation()
    decide(s, obs, CONFIG, NOW)
    obs["game"]["blockers"] = ["stale"]
    assert decide(s, obs, CONFIG, NOW + timedelta(seconds=15))[-1]["action"] == "cancel_paper_intent"


def test_fair_value_exit_realizes_only_after_second_quote_and_fees():
    s, obs = enter()
    assert s["cash_cents"] == 99472
    assert s["realized_cents"] == 0
    obs["markets"]["home"]["quote"].update(bid=.71, ask=.72)
    assert decide(s, obs, CONFIG, NOW + timedelta(seconds=30))[-1]["action"] == "stage_sell"
    result = decide(s, obs, CONFIG, NOW + timedelta(seconds=45))[-1]
    assert result["action"] == "sell"
    assert s["position"] is None
    assert s["cash_cents"] == 100000 + result["profit_cents"]
    assert s["realized_cents"] == result["profit_cents"]
    assert result["profit_cents"] == 157


def test_failed_exit_does_not_invent_fill_and_stale_feed_requests_exit():
    s, obs = enter()
    obs["game"]["blockers"] = ["stale"]
    result = decide(s, obs, CONFIG, NOW + timedelta(seconds=30))[-1]
    assert result["reason"] == "signal_unavailable"
    obs["markets"]["home"]["quote"]["bid_size"] = 0
    assert decide(s, obs, CONFIG, NOW + timedelta(seconds=45))[-1]["action"] == "cancel_paper_intent"
    assert s["position"] is not None
    assert s["realized_cents"] == 0


def test_limits_pause_cooldown_and_partial_depth():
    s, obs = state(), observation()
    assert decide(s, obs, CONFIG, NOW, paused=True)[-1]["reason"] == "operator_pause"
    obs["markets"]["home"]["quote"]["ask_size"] = 2.5
    decide(s, obs, CONFIG, NOW)
    assert s["pending"]["count"] == 2
    s, obs = state(), observation()
    s["entries"] = CONFIG["max_entries"]
    assert decide(s, obs, CONFIG, NOW)[-1]["reason"] == "entry_gate"
    s["entries"] = 0
    s["cash_cents"] -= CONFIG["max_loss_cents"]
    decide(s, obs, CONFIG, NOW)
    assert s["halted"] and s["pending"] is None
    s, obs = enter()
    s["position"] = None
    assert decide(s, obs, CONFIG, NOW)[-1]["reason"] == "cooldown_or_same_play"


def test_settlement_only_kalshi_and_idempotent():
    s, obs = enter()
    obs["game"]["completed"] = True
    obs["markets"]["home"]["valid"] = False
    decide(s, obs, CONFIG, NOW + timedelta(seconds=30))
    assert s["realized_cents"] == 0
    obs["markets"]["home"].update(status="finalized", result="yes")
    assert decide(s, obs, CONFIG, NOW + timedelta(seconds=45))[0]["action"] == "settle"
    assert s["cash_cents"] == 100472
    decide(s, obs, CONFIG, NOW + timedelta(seconds=60))
    assert s["cash_cents"] == 100472


def test_restart_persists_intent_and_config_cannot_drift(tmp_path):
    path = tmp_path / "paper.db"
    db = open_db(path, CONFIG)
    step(db, observation(), CONFIG, NOW)
    db.close()
    db = open_db(path, CONFIG)
    result = step(db, observation(), CONFIG, NOW + timedelta(seconds=15))
    assert result["actions"][-1]["action"] == "buy"
    assert report(db)["samples"] == 2
    db.close()
    with pytest.raises(ValueError, match="different config"):
        open_db(path, {**CONFIG, "entry_edge": .01})


def test_collector_preserves_transport_errors_and_no_entries(monkeypatch):
    monkeypatch.setattr("opportunity_lab.college_football_paper.fetch",
                        lambda url: {"ok": False, "error": "offline", "received_at": NOW.isoformat()})
    obs = collect(CONFIG)
    s = state()
    assert decide(s, obs, CONFIG, NOW)[-1]["reason"] == "entry_gate"
    assert s["position"] is None
