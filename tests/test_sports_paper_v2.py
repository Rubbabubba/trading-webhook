from datetime import datetime, timedelta, timezone

import pytest

from opportunity_lab.sports_paper_v2 import (DEFAULTS, decide, entry_check, fee_supported, initial_state,
    open_db, parse_game, report, step, validate)
from opportunity_lab.sports_paper_suite import match_event

NOW = datetime(2026, 9, 8, 23, tzinfo=timezone.utc)
CONFIG = {**DEFAULTS, "league": "mlb", "event_id": "1", "home_team_id": "2", "away_team_id": "3",
          "game": "Test", "market_event": "EVENT", "markets": {"home": "EVENT-H", "away": "EVENT-A"},
          "kickoff": NOW.isoformat(), "stop_at": (NOW + timedelta(hours=12)).isoformat()}


def payload():
    return {"header": {"id": "1", "competitions": [{"status": {"type": {"name": "STATUS_IN_PROGRESS", "state": "in"}},
              "competitors": [{"id": "2", "homeAway": "home", "score": "1"}, {"id": "3", "homeAway": "away", "score": "0"}]}]},
            "plays": [{"id": "1057", "atBatId": "10", "homeScore": 1, "awayScore": 0,
                       "wallclock": (NOW - timedelta(seconds=15)).isoformat(), "outs": 1},
                      {"id": "1099", "atBatId": "10", "homeScore": 1, "awayScore": 0, "outs": 1,
                       "type": {"type": "end-batterpitcher"}}],
            "winprobability": [{"playId": "1057", "homeWinPercentage": .75, "tiePercentage": 0}]}


def observation():
    return {"at": NOW.isoformat(), "game": parse_game(payload(), CONFIG, NOW), "game_transport_ok": True,
            "fee_ok": True, "transport": {"game": {"ok": True}},
            "markets": {s: {"ticker": t, "valid": True, "settlement_ok": False, "result": "",
                            "quote": {"ask": .60 if s == "home" else .62, "bid": .59 if s == "home" else .61,
                                      "ask_size": 100, "bid_size": 100}} for s, t in CONFIG["markets"].items()}}


def enter():
    state, obs = initial_state(CONFIG), observation()
    assert decide(state, obs, CONFIG, NOW)[0]["action"] == "stage_buy"
    assert decide(state, obs, CONFIG, NOW + timedelta(seconds=20))[0]["action"] == "buy"
    return state, obs


def test_mlb_probability_marker_allowed_but_next_pitch_rejected():
    data = payload()
    assert parse_game(data, CONFIG, NOW)["blockers"] == []
    data["plays"].append({"id": "1101", "atBatId": "11", "homeScore": 1, "awayScore": 0, "type": {"type": "pitch"}})
    assert "probability_play_mismatch" in parse_game(data, CONFIG, NOW)["blockers"]


def test_nfl_drive_parser_and_identity():
    data = payload()
    data["drives"] = {"previous": [{"plays": data["plays"][:1]}]}
    c = {**CONFIG, "league": "nfl"}
    assert not parse_game(data, c, NOW)["blockers"]
    data["header"]["competitions"][0]["competitors"][0]["id"] = "WRONG"
    assert "identity_mismatch" in parse_game(data, c, NOW)["blockers"]


def test_timestamps_never_repaired_and_scores_must_match():
    data = payload()
    data["plays"][0]["wallclock"] = (NOW - timedelta(days=1)).isoformat()
    data["plays"][0]["modified"] = NOW.isoformat()
    assert "old_or_inconsistent_play_timestamp" in parse_game(data, CONFIG, NOW)["blockers"]
    data = payload()
    data["header"]["competitions"][0]["competitors"][0]["score"] = "2"
    assert "score_mismatch" in parse_game(data, CONFIG, NOW)["blockers"]


def test_nfl_tie_probability_and_nan_fail_closed():
    data = payload()
    data["winprobability"][0]["tiePercentage"] = .01
    assert parse_game(data, CONFIG, NOW)["home_probability"] is None
    data["winprobability"][0].update(tiePercentage=0, homeWinPercentage=float("nan"))
    assert parse_game(data, CONFIG, NOW)["home_probability"] is None


def test_entry_requires_margin_before_stop_and_exit_depth():
    m = observation()["markets"]["home"]
    assert entry_check(m, .75, 10, CONFIG, 100000)
    m["quote"].update(ask=.2, bid=.19)
    assert entry_check(m, .6, 10, CONFIG, 100000) is None
    m["quote"].update(ask=.5, bid=.49, bid_size=0)
    assert entry_check(m, .75, 10, CONFIG, 100000) is None


def test_confirmation_rechecks_liquidation_even_with_valid_edge():
    s, o = initial_state(CONFIG), observation()
    decide(s, o, CONFIG, NOW)
    o["markets"]["home"]["quote"]["bid"] = .57
    assert decide(s, o, CONFIG, NOW + timedelta(seconds=20))[0]["action"] == "cancel"
    assert s["cash_cents"] == 100000 and s["position"] is None


def test_brief_feed_loss_holds_but_persistent_failure_exits():
    s, o = enter()
    o["game"]["blockers"] = ["old_or_inconsistent_play_timestamp"]
    assert decide(s, o, CONFIG, NOW + timedelta(seconds=40))[0]["reason"] == "feed_grace"
    assert decide(s, o, CONFIG, NOW + timedelta(seconds=341))[0]["reason"] == "persistent_feed_failure"


def test_healthy_break_does_not_exit_or_allow_entry():
    s, o = enter()
    o["game"].update(scheduled_break=True, blockers=["not_active_play", "old_or_inconsistent_play_timestamp"])
    assert decide(s, o, CONFIG, NOW + timedelta(seconds=40))[0]["reason"] == "scheduled_break"
    assert decide(s, o, CONFIG, NOW + timedelta(minutes=20))[0]["reason"] == "scheduled_break"
    assert s["bad_since"] is None
    fresh = initial_state(CONFIG)
    assert decide(fresh, o, CONFIG, NOW)[0]["action"] == "wait"


def test_loss_stop_still_operates_during_feed_grace():
    s, o = enter()
    o["game"]["blockers"] = ["missing"]
    o["markets"]["home"]["quote"].update(bid=.3, ask=.31)
    assert decide(s, o, CONFIG, NOW + timedelta(seconds=40))[0]["reason"] == "position_loss_limit"


def test_roundtrip_cash_and_fees_reconcile_and_mark_after_fill(tmp_path):
    db = open_db(tmp_path / "paper.db", CONFIG)
    o = observation()
    step(db, o, CONFIG, NOW)
    entered = step(db, o, CONFIG, NOW + timedelta(seconds=20))
    assert entered["account"]["cash_cents"] == 99373
    assert entered["account"]["mark_equity_cents"] == 99936
    o["markets"]["home"]["quote"].update(bid=.78, ask=.79)
    assert step(db, o, CONFIG, NOW + timedelta(seconds=40))["actions"][0]["action"] == "stage_sell"
    sold = step(db, o, CONFIG, NOW + timedelta(seconds=60))
    assert sold["actions"][0]["action"] == "sell"
    assert sold["account"]["cash_cents"] == 100000 + sold["actions"][0]["profit_cents"]
    assert sold["account"]["position"] is None
    db.close()


def test_settlement_requires_market_confirmation():
    s, o = enter()
    o["game"]["completed"] = True
    o["markets"]["home"].update(valid=False, result="yes")
    decide(s, o, CONFIG, NOW + timedelta(seconds=30))
    assert s["realized_cents"] == 0
    o["markets"]["home"]["settlement_ok"] = True
    decide(s, o, CONFIG, NOW + timedelta(seconds=40))
    assert s["cash_cents"] == 100373 and s["position"] is None
    decide(s, o, CONFIG, NOW + timedelta(seconds=50))
    assert s["cash_cents"] == 100373


def test_signals_include_skipped_entries_markouts_do_not_inform_decisions(tmp_path):
    db = open_db(tmp_path / "paper.db", CONFIG)
    o = observation()
    o["game"]["home_probability"] = .5
    step(db, o, CONFIG, NOW)
    assert report(db)["signals"] == 1
    assert report(db)["account"]["entries"] == 0
    step(db, o, CONFIG, NOW + timedelta(seconds=60))
    assert report(db)["markouts"] == 2
    step(db, o, CONFIG, NOW + timedelta(seconds=65))
    assert report(db)["markouts"] == 2
    db.close()
    db = open_db(tmp_path / "paper.db", CONFIG)
    assert report(db)["signals"] == 1
    db.close()
    with pytest.raises(ValueError, match="Frozen"):
        open_db(tmp_path / "paper.db", {**CONFIG, "entry_edge": .07})


def test_mapping_uses_both_teams_and_exact_doubleheader_time():
    event = {"id": "1", "name": "Test", "date": "2026-09-08T23:40Z", "competitions": [{"competitors": [
        {"id": "2", "homeAway": "home", "team": {"abbreviation": "CHW"}},
        {"id": "3", "homeAway": "away", "team": {"abbreviation": "PIT"}}]}]}
    stem = "KXMLBGAME-26SEP081940PITCWS"
    markets = {stem + "-" + team: {"event_ticker": stem} for team in ("PIT", "CWS")}
    assert match_event(event, markets, "mlb")["markets"]["home"] == stem + "-CWS"
    event["date"] = "2026-09-08T17:40Z"
    assert match_event(event, markets, "mlb") is None


def test_invalid_risk_configuration_rejected():
    validate(CONFIG)
    with pytest.raises(ValueError):
        validate({**CONFIG, "max_entry_loss_fraction": .3})


def test_fee_model_is_upper_bound_and_unknown_fees_block():
    series = {"ticker": "KXMLBGAME", "fee_type": "quadratic_with_maker_fees", "fee_multiplier": .5}
    assert fee_supported(series, "KXMLBGAME", CONFIG)
    assert fee_supported({**series, "fee_multiplier": 1}, "KXMLBGAME", CONFIG)
    for value in (2, None, -1, float("nan")):
        assert not fee_supported({**series, "fee_multiplier": value}, "KXMLBGAME", CONFIG)
    assert not fee_supported({**series, "fee_type": "unknown"}, "KXMLBGAME", CONFIG)
    assert not fee_supported(series, "KXNFLGAME", CONFIG)


def test_restart_resumes_pending_once_and_pause_cancels(tmp_path):
    db = open_db(tmp_path / "paper.db", CONFIG)
    o = observation()
    step(db, o, CONFIG, NOW)
    db.close()
    db = open_db(tmp_path / "paper.db", CONFIG)
    assert step(db, o, CONFIG, NOW + timedelta(seconds=20))["account"]["entries"] == 1
    assert step(db, o, CONFIG, NOW + timedelta(seconds=40))["account"]["entries"] == 1
    db.close()
    s = initial_state(CONFIG)
    decide(s, o, CONFIG, NOW)
    assert decide(s, o, CONFIG, NOW + timedelta(seconds=20), paused=True)[0]["reason"] == "operator_pause"
    assert s["pending"] is None and s["entries"] == 0


def test_transport_failure_during_break_uses_short_grace():
    s, o = enter()
    o["game"].update(scheduled_break=True, blockers=["not_active_play"])
    o["game_transport_ok"] = False
    decide(s, o, CONFIG, NOW + timedelta(seconds=40))
    assert decide(s, o, CONFIG, NOW + timedelta(seconds=341))[0]["reason"] == "persistent_feed_failure"
