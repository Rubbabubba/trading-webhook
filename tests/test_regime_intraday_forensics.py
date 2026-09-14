from regime_intraday_forensics import roundtrip_forensic_report


def _legs(closing=False):
    return ([{"symbol": "LONG", "side": "sell", "filled_qty": 1, "filled_avg_price": 1.2}, {"symbol": "SHORT", "side": "buy", "filled_qty": 1, "filled_avg_price": .7}]
            if closing else [{"symbol": "LONG", "side": "buy", "filled_qty": 1, "filled_avg_price": 1.0}, {"symbol": "SHORT", "side": "sell", "filled_qty": 1, "filled_avg_price": .7}])


def test_forensics_attributes_every_verified_trade_and_cohorts():
    ledger = {"orders": {"sig-1": {"status": "filled_closed", "signal": {"strategy": "vwap_mean_reversion", "underlying_side": "buy"},
        "plan": {"underlying": "SPY", "legs": [{"symbol": "LONG", "side": "buy"}, {"symbol": "SHORT", "side": "sell"}]},
        "broker": {"filled_at": "2026-09-09T14:00:00Z", "legs": _legs()},
        "close_order": {"reason": "underlying_target", "broker": {"filled_at": "2026-09-09T14:05:00Z", "legs": _legs(True)}}}}}
    report = roundtrip_forensic_report(ledger)
    assert report["status"] == "complete"
    assert report["gross_pnl_dollars"] == 20.0
    assert report["net_after_estimated_fees_dollars"] == 18.7
    assert report["by_symbol"]["SPY"]["win_rate"] == 1.0
    assert report["trades"][0]["hold_minutes"] == 5.0
    assert report["trades"][0]["attribution"] == "profitable_roundtrip"
