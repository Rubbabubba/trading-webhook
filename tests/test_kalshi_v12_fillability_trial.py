from opportunity_lab.kalshi_v12_fillability_trial import (
    FILL_RATE_CHECKPOINT_ATTEMPTS,
    MAX_FILLS,
    MAX_ORDER_ATTEMPTS,
    improved_quote,
    trial_decision,
)


def passing_shadow():
    return {
        "strategy_id": "microprice_value_maker_v12_shadow",
        "execution_enabled": False,
        "automatic_rejection_triggered": False,
        "signals": 130,
        "complete_signals": 127,
        "independent_events": 47,
        "markout_records": {"5": 129, "30": 129, "300": 127},
        "stressed_markout_pnl_cents": {"5": 306, "30": 306, "300": 300},
        "event_cluster_lcb_cents": {"5": 1.83, "30": 1.83, "300": 1.82},
    }


def signal(gross="4.25", price=40, spread=6):
    return {
        "strategy_id": "microprice_value_maker_v12_shadow",
        "outcome": "yes",
        "price_cents": price,
        "gross_edge_cents": gross,
        "stressed_edge_cents": str(float(gross) - 2),
        "spread_cents": spread,
        "yes_mid": "43/100",
    }


def test_improved_quote_preserves_one_cent_after_cost_stress():
    quote, reason = improved_quote(signal())
    assert reason == "eligible"
    assert quote["source_signal_price_cents"] == 40
    assert quote["price_cents"] == 41
    assert quote["post_only"] is True
    assert quote["stressed_edge_after_improvement_cents"] == "1.25"


def test_improved_quote_rejects_edge_or_spread_that_cannot_support_it():
    assert improved_quote(signal(gross="3.99"))[1] == "insufficient_edge_for_improvement"
    assert improved_quote(signal(spread=1))[1] == "spread_too_narrow_for_post_only_improvement"


def test_fillability_trial_is_bounded_and_stops_for_low_fill_rate():
    base = dict(
        shadow=passing_shadow(), attempts=0, fills=0,
        start_balance_cents=49952, current_flat_balance_cents=49952,
        event_locked=False,
    )
    assert trial_decision(**base)["allowed"]
    cases = (
        ({"attempts": MAX_ORDER_ATTEMPTS}, "trial_order_cap_reached"),
        ({"fills": MAX_FILLS}, "trial_fill_cap_reached"),
        ({"attempts": FILL_RATE_CHECKPOINT_ATTEMPTS, "fills": 4}, "insufficient_fill_rate"),
        ({"current_flat_balance_cents": 49852}, "trial_loss_stop"),
        ({"event_locked": True}, "event_already_entered"),
    )
    for update, reason in cases:
        assert trial_decision(**(base | update))["reason"] == reason


def test_fillability_trial_continues_at_checkpoint_with_minimum_fills():
    result = trial_decision(
        shadow=passing_shadow(), attempts=FILL_RATE_CHECKPOINT_ATTEMPTS, fills=5,
        start_balance_cents=49952, current_flat_balance_cents=49960,
        event_locked=False,
    )
    assert result["allowed"]
    assert result["fill_rate"] == .05
    assert result["flat_pnl_cents"] == 8
