from regime_intraday_email import build_entry_lifecycle_email, build_exit_email, build_signal_email, send_signal_email


def test_email_contains_actionable_risk_and_never_claims_submission():
    signal = {"signal_id": "abc", "symbol": "SPY", "strategy": "vwap_mean_reversion", "underlying_side": "buy", "entry_price": 100, "stop_price": 99, "target_price": 102}
    plan = {"expiration": "2026-09-18", "limit_debit": 0.42, "max_loss_dollars": 42, "max_profit_dollars": 58, "quote_basis": {"selection_source": "near_money_fallback"}, "legs": [{"side": "buy", "symbol": "LONG"}, {"side": "sell", "symbol": "SHORT"}]}
    message = build_signal_email(signal, plan)
    assert "$42.00" in message["text"]
    assert "BUY LONG" in message["text"]
    assert "not an order or fill confirmation" in message["text"]


def test_missing_email_configuration_is_safe_noop():
    assert send_signal_email(api_key="", to_email="", from_email="", signal={}, plan={}) == {"sent": False, "reason": "email_not_configured"}


def test_exit_email_is_actionable_and_does_not_claim_a_close():
    record = {"plan": {"underlying": "SPY", "limit_debit": 0.40}, "valuation": {"liquidation_credit": 0.61, "unrealized_dollars": 21}, "exit_decision": {"exit": True, "reason": "take_profit"}}
    message = build_exit_email("sig-1", record)
    assert "take_profit" in message["subject"]
    assert "$21.00" in message["text"]
    assert "does not confirm a submission or fill" in message["text"]


def test_entry_lifecycle_emails_distinguish_submission_from_fill():
    record = {"order_id": "order-1", "status": "new", "plan": {"underlying": "SPY", "limit_debit": .77,
              "max_loss_dollars": 77, "legs": [{"side": "buy", "symbol": "LONG"}, {"side": "sell", "symbol": "SHORT"}]},
              "broker": {"status": "filled", "filled_qty": "1", "filled_avg_price": "0.76", "filled_at": "now"}}
    submitted = build_entry_lifecycle_email("sig-1", record, "submitted")
    filled = build_entry_lifecycle_email("sig-1", record, "filled")
    assert "does not confirm a fill" in submitted["text"]
    assert "PAPER ENTRY FILLED" in filled["subject"]
    assert "Average fill debit: $0.76" in filled["text"]
