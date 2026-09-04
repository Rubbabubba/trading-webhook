from regime_intraday_option_replay import replay_option_batch, replay_option_execution


def quote(timestamp, *, long_bid=2.00, long_ask=2.05, short_bid=1.55, short_ask=1.60, **events):
    return {"timestamp": timestamp, "long_bid": long_bid, "long_ask": long_ask,
            "short_bid": short_bid, "short_ask": short_ask, **events}


def base(**overrides):
    case = {"data_source": "indicative", "entry_limit_debit": .55,
            "quotes": [quote("10:00"), quote("10:01"), quote("10:02")],
            "option_stop_confirmations": 2}
    case.update(overrides)
    return case


def test_target_and_end_of_day_close_scenarios():
    target = replay_option_execution(base(quotes=[quote("10:00"), quote("10:01", underlying_target_hit=True)]))
    eod = replay_option_execution(base(quotes=[quote("10:00"), quote("15:45", end_of_day=True)]))
    assert target["status"] == "closed" and target["reason"] == "underlying_target"
    assert eod["status"] == "closed" and eod["reason"] == "end_of_day"


def test_option_stop_requires_configured_consecutive_quotes():
    breach = dict(long_bid=1.80, long_ask=1.85, short_bid=1.55, short_ask=1.60)
    one = replay_option_execution(base(quotes=[quote("10:00"), quote("10:01", **breach)]))
    two = replay_option_execution(base(quotes=[quote("10:00"), quote("10:01", **breach), quote("10:02", **breach)]))
    assert one["status"] == "open_at_end_of_data"
    assert two["status"] == "closed" and two["reason"] == "confirmed_option_stop"


def test_underlying_stop_is_immediate():
    result = replay_option_execution(base(quotes=[quote("10:00"), quote("10:01", underlying_stop_hit=True)]))
    assert result["status"] == "closed" and result["reason"] == "underlying_stop"


def test_no_fill_partial_rejection_and_wide_spread_are_explicit():
    no_fill = replay_option_execution(base(entry_limit_debit=.40))
    partial = replay_option_execution(base(broker_entry_status="partially_filled"))
    rejected = replay_option_execution(base(broker_entry_status="rejected"))
    wide = replay_option_execution(base(quotes=[quote("10:00", long_bid=1, long_ask=2)]))
    assert no_fill["status"] == "no_fill"
    assert partial["reason"] == "partial_fill"
    assert rejected["reason"] == "broker_rejected"
    assert wide["reason"] == "wide_leg_spread"


def test_latency_fees_slippage_and_missing_exit_quote_are_modeled():
    latency = replay_option_execution(base(entry_latency_minutes=1, exit_latency_minutes=1,
        slippage_per_side=.01, roundtrip_fees_dollars=2,
        quotes=[quote("10:00"), quote("10:01"), quote("10:02", underlying_target_hit=True), quote("10:03")]))
    missing = replay_option_execution(base(quotes=[quote("10:00"), quote("10:01", long_bid=0, short_ask=0, underlying_stop_hit=True)]))
    assert latency["entry_latency_minutes"] == 1 and latency["fees_dollars"] == 2
    assert latency["net_pnl_dollars"] == latency["gross_pnl_dollars"] - 2
    assert missing["status"] == "exit_requires_attention"


def test_opra_claim_requires_provenance_and_batch_stays_paper_only():
    rejected = replay_option_execution(base(data_source="opra"))
    batch = replay_option_batch({"cases": [base(), base(data_source="opra", opra_provenance={"provider": "alpaca"})]})
    assert rejected["reason"] == "opra_provenance_required"
    assert batch["case_count"] == 2
    assert batch["actual_opra_case_count"] == 1
    assert batch["live_submission"] is False
