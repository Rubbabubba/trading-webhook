import os

import runtime_defaults


def test_runtime_defaults_never_contain_render_managed_values():
    assert runtime_defaults.RENDER_MANAGED_KEYS.isdisjoint(runtime_defaults.PRODUCTION_DEFAULTS)
    assert "REGIME_INTRADAY_LIVE_ENABLED" in runtime_defaults.RENDER_MANAGED_KEYS


def test_runtime_defaults_do_not_override_real_environment(monkeypatch):
    monkeypatch.setenv("DATA_FEED", "sip")
    runtime_defaults.apply_production_defaults()
    assert os.environ["DATA_FEED"] == "sip"


def test_runtime_defaults_preserve_current_production_profile(monkeypatch):
    monkeypatch.delenv("SWING_RISK_PER_TRADE_DOLLARS", raising=False)
    runtime_defaults.apply_production_defaults()
    assert os.environ["SWING_RISK_PER_TRADE_DOLLARS"] == "30"
