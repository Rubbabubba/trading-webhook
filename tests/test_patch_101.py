from app import _entry_spread_override_decision


def test_patch_101_override_allows_liquid_iex_quote():
    snap = {
        "spread_pct": 0.03,
        "trade_price": 207.64,
        "mid": 207.105,
        "quote_debug": {"feed": "iex"},
    }
    meta = {"avg_dollar_volume_20d": 155_000_000}
    out = _entry_spread_override_decision(snap, meta=meta)
    assert out["allowed"] is True
    assert out["selected_path"] == "iex_liquidity_override"
