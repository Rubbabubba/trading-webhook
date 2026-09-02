import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


def test_patch_195_guarded_fetch_chunks_and_limits(monkeypatch):
    calls = []

    def fake_fetch(symbols, lookback_days=1, limit_per_symbol=None):
        calls.append((tuple(symbols), lookback_days, limit_per_symbol))
        return {
            sym: [{"ts_utc": i, "close": float(i)} for i in range(10)]
            for sym in symbols
        }

    monkeypatch.setattr(app, "SCANNER_FETCH_CHUNK_SIZE", 2)
    monkeypatch.setattr(app, "SCANNER_MAX_1M_BARS_PER_SYMBOL", 7)
    monkeypatch.setattr(app, "fetch_1m_bars_multi", fake_fetch)
    monkeypatch.setattr(app.gc, "collect", lambda: 0)

    out = app.fetch_1m_bars_multi_guarded(["AAPL", "MSFT", "NVDA"], lookback_days=30)

    assert calls == [(('AAPL', 'MSFT'), 30, 7), (('NVDA',), 30, 7)]
    assert list(out) == ["AAPL", "MSFT", "NVDA"]
    assert all(len(rows) == 7 for rows in out.values())
    assert out["AAPL"][0]["ts_utc"] == 3


def test_patch_195_scan_history_results_are_compacted_by_default(monkeypatch):
    monkeypatch.setattr(app, "SCAN_HISTORY_STORE_FULL_RESULTS", False)
    monkeypatch.setattr(app, "SCAN_HISTORY_RESULT_LIMIT", 1)
    rows = [
        {
            "symbol": "AAPL",
            "action": "hold",
            "reason": "no_signal:vwap_pullback",
            "price": 123.45,
            "diagnostics": {
                "vwap_pullback": {
                    "reason": "macro_not_ready",
                    "score": 42,
                    "dist_to_vwap_pct": 0.12,
                    "component_reasons": ["large", "payload"],
                    "trend_components": {"fallback_ready": True, "extra": "drop"},
                }
            },
            "large_unused_payload": [1, 2, 3],
        },
        {"symbol": "MSFT", "action": "hold"},
    ]

    stored = app._scan_history_results_for_storage(rows)

    assert len(stored) == 1
    assert stored[0]["symbol"] == "AAPL"
    assert stored[0]["vwap_pullback"]["reason"] == "macro_not_ready"
    assert stored[0]["vwap_pullback"]["fallback_ready"] is True
    assert "diagnostics" not in stored[0]
    assert "large_unused_payload" not in stored[0]


def test_patch_195_scanner_enabled_prefers_canonical_env(monkeypatch):
    monkeypatch.setenv("SCANNER_ENABLED", "false")
    monkeypatch.setenv("SWING_SCANNER_ENABLED", "true")
    assert app.env_bool_any("SCANNER_ENABLED", "SWING_SCANNER_ENABLED", default="false") is False


def test_patch_195_diagnostics_scanner_includes_memory_profile(monkeypatch):
    monkeypatch.setattr(app, "SCANNER_MAX_SYMBOLS_PER_CYCLE", 15)
    monkeypatch.setattr(app, "SCANNER_CANDIDATE_LIMIT", 10)
    monkeypatch.setattr(app, "SCANNER_LOOKBACK_DAYS", 30)
    monkeypatch.setattr(app, "SCANNER_FETCH_CHUNK_SIZE", 5)
    monkeypatch.setattr(app, "SCANNER_MAX_1M_BARS_PER_SYMBOL", 1500)

    out = app.diagnostics_scanner()
    profile = out["memory_profile"]

    assert profile["scanner_max_symbols_per_cycle"] == 15
    assert profile["scanner_candidate_limit"] == 10
    assert profile["scanner_lookback_days"] == 30
    assert profile["scanner_fetch_chunk_size"] == 5
    assert profile["scanner_max_1m_bars_per_symbol"] == 1500