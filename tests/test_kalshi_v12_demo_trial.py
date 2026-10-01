from opportunity_lab.kalshi_v12_demo_trial import (
    MAX_FILLS,
    MAX_ORDER_ATTEMPTS,
    shadow_gate_passed,
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


def test_shadow_gate_requires_complete_positive_evidence():
    shadow = passing_shadow()
    assert shadow_gate_passed(shadow)
    shadow["event_cluster_lcb_cents"]["300"] = 0
    assert not shadow_gate_passed(shadow)


def test_trial_allows_only_bounded_flat_demo_entry():
    result = trial_decision(
        shadow=passing_shadow(), attempts=0, fills=0,
        start_balance_cents=49952, current_flat_balance_cents=49952,
        event_locked=False,
    )
    assert result["allowed"] and result["reason"] == "authorized"


def test_trial_caps_orders_fills_loss_and_repeat_events():
    base = dict(
        shadow=passing_shadow(), attempts=0, fills=0,
        start_balance_cents=49952, current_flat_balance_cents=49952,
        event_locked=False,
    )
    for update, reason in (
        ({"attempts": MAX_ORDER_ATTEMPTS}, "trial_order_cap_reached"),
        ({"fills": MAX_FILLS}, "trial_fill_cap_reached"),
        ({"current_flat_balance_cents": 49927}, "trial_loss_stop"),
        ({"event_locked": True}, "event_already_entered"),
    ):
        assert trial_decision(**(base | update))["reason"] == reason
