from regime_intraday_email import build_signal_email, send_signal_email


def test_email_contains_actionable_risk_and_never_claims_submission():
    signal = {"signal_id": "abc", "symbol": "SPY", "strategy": "vwap_mean_reversion", "underlying_side": "buy", "entry_price": 100, "stop_price": 99, "target_price": 102}
    plan = {"expiration": "2026-09-18", "limit_debit": 0.42, "max_loss_dollars": 42, "max_profit_dollars": 58, "quote_basis": {"selection_source": "near_money_fallback"}, "legs": [{"side": "buy", "symbol": "LONG"}, {"side": "sell", "symbol": "SHORT"}]}
    message = build_signal_email(signal, plan)
    assert "$42.00" in message["text"]
    assert "BUY LONG" in message["text"]
    assert "No order has been sent" in message["text"]


def test_missing_email_configuration_is_safe_noop():
    assert send_signal_email(api_key="", to_email="", from_email="", signal={}, plan={}) == {"sent": False, "reason": "email_not_configured"}
