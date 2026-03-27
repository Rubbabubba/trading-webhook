import app
from fastapi import Request


def _dummy_request(path: str = "/dashboard"):
    return Request({"type": "http", "headers": [], "query_string": b"", "method": "GET", "path": path})


def test_non_admin_auth_shim_is_noop_when_admin_secret_is_set(monkeypatch):
    monkeypatch.setattr(app, "ADMIN_SECRET", "secret-123")
    assert app.require_admin_if_configured(_dummy_request()) is None


def test_explicit_admin_routes_still_require_valid_header(monkeypatch):
    monkeypatch.setattr(app, "ADMIN_SECRET", "secret-123")
    try:
        app.require_admin(_dummy_request("/admin/release/promote/live_guarded"))
        assert False, "expected invalid admin secret"
    except Exception as exc:
        assert getattr(exc, "status_code", None) == 401


def test_universe_shadow_snapshot_defines_regime_mode(monkeypatch):
    monkeypatch.setattr(app, 'ALLOWED_SYMBOLS', {'AAPL'})
    monkeypatch.setattr(app, 'SCANNER_MAX_SYMBOLS_PER_CYCLE', 10)
    monkeypatch.setattr(app, 'SWING_INDEX_SYMBOL', 'SPY')
    monkeypatch.setattr(app, 'SCANNER_LOOKBACK_DAYS', 20)
    monkeypatch.setattr(app, 'SWING_REGIME_SLOW_MA_DAYS', 20)
    monkeypatch.setattr(app, 'REGIME_BREADTH_RETURN_LOOKBACK_DAYS', 20)
    monkeypatch.setattr(app, 'SWING_SLOW_MA_DAYS', 20)
    monkeypatch.setattr(app, 'SWING_BREAKOUT_LOOKBACK_DAYS', 20)
    monkeypatch.setattr(app, 'SWING_REQUIRE_INDEX_ALIGNMENT', False)
    monkeypatch.setattr(app, 'SWING_ALLOW_NEW_ENTRIES_IN_WEAK_TAPE', False)
    monkeypatch.setattr(app, 'SWING_REGIME_MODE_SWITCHING_ENABLED', True)
    monkeypatch.setattr(app, 'universe_symbols', lambda: ['AAPL'])
    monkeypatch.setattr(app, 'fetch_daily_bars_multi', lambda syms, lookback_days=0: {s: [] for s in syms})
    monkeypatch.setattr(app, '_index_alignment_ok', lambda bars: True)
    monkeypatch.setattr(app, '_build_swing_regime', lambda bars, daily_map, symbols: {'favorable': False})
    monkeypatch.setattr(app, '_get_regime_mode', lambda regime, index_ok: 'defensive')
    monkeypatch.setattr(app, 'evaluate_daily_breakout_candidate', lambda sym, bars, index_ok, regime_mode='trend': {
        'symbol': sym,
        'eligible': False,
        'rank_score': 1.0,
        'rejection_reasons': [f'regime_mode:{regime_mode}'],
    })

    snap = app._universe_shadow_snapshot(limit=5)
    top = snap['runtime_universe']['top_candidates'][0]
    assert top['symbol'] == 'AAPL'
    assert 'regime_mode:defensive' in top['rejection_reasons']
