from opportunity_lab.kalshi_maker_v12 import shadow_decision
from opportunity_lab.kalshi_v12_execution_feasibility import crossing_diagnostic


def _frame(yes_bid, no_bid, yes_depth, no_depth):
    return {
        "orderbook_fp": {
            "yes_dollars": [[f"{yes_bid / 100:.2f}", str(yes_depth)]],
            "no_dollars": [[f"{no_bid / 100:.2f}", str(no_depth)]],
        }
    }


def test_v12_crossing_cannot_retain_its_microprice_edge():
    for frame in (_frame(46, 46, 12, 4), _frame(46, 46, 4, 12)):
        signal, reason = shadow_decision([], frame)
        assert reason == "signal"
        result = crossing_diagnostic(signal)
        assert result["eligible"] is False
        assert result["gross_crossing_edge_cents"] < 0
        assert result["execution_enabled"] is False


def test_crossing_diagnostic_rejects_malformed_inputs():
    assert crossing_diagnostic(None)["reason"] == "not_v12_signal"
    assert crossing_diagnostic({"strategy_id": "microprice_value_maker_v12_shadow"})["reason"] == "malformed_signal"
