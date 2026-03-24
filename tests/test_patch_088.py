
import app

def test_patch_version_088():
    assert app.PATCH_VERSION == "patch-088-alpaca-payload-scope-hotfix"

def test_execute_entry_signal_quote_missing_does_not_raise_unboundlocal(monkeypatch):
    monkeypatch.setattr(app, "ALLOWED_SYMBOLS", ["NET"])
    monkeypatch.setattr(app, "KILL_SWITCH", False)
    monkeypatch.setattr(app, "ONLY_MARKET_HOURS", False)
    monkeypatch.setattr(app, "ALLOW_SHORT", False)
    monkeypatch.setattr(app, "ENABLE_IDEMPOTENCY", False)
    monkeypatch.setattr(app, "MAX_OPEN_POSITIONS", 2)
    monkeypatch.setattr(app, "ENTRY_REQUIRE_QUOTE", True)
    monkeypatch.setattr(app, "ENTRY_REQUIRE_FRESH_QUOTE", True)
    monkeypatch.setattr(app, "ENTRY_MAX_SPREAD_PCT", 0.003)
    monkeypatch.setattr(app, "TRADE_PLAN", {})
    monkeypatch.setattr(app, "DEDUP_CACHE", {})
    monkeypatch.setattr(app, "record_decision", lambda *a, **k: None)
    monkeypatch.setattr(app, "soften_symbol_lock", lambda *a, **k: None)
    monkeypatch.setattr(app, "daily_halt_active", lambda: False)
    monkeypatch.setattr(app, "daily_stop_hit", lambda: False)
    monkeypatch.setattr(app, "in_market_hours", lambda: True)
    monkeypatch.setattr(app, "get_position", lambda symbol: (0.0, "flat"))
    monkeypatch.setattr(app, "max_open_positions_reached", lambda: False)
    monkeypatch.setattr(app, "take_symbol_lock", lambda symbol, sec: True)
    monkeypatch.setattr(app, "effective_entry_dry_run", lambda source: False)
    monkeypatch.setattr(app, "get_latest_quote_snapshot", lambda symbol: {
        "price": 100.0,
        "quote_ok": False,
        "fresh": True,
        "spread_pct": 0.0,
    })

    out = app.execute_entry_signal("NET", "buy", "daily_breakout", "worker_scan", meta={})
    assert out["ok"] is False
    assert out["reason"] == "quote_missing"
