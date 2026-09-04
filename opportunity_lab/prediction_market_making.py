"""Conservative snapshot screen for two-sided prediction-market quoting."""

from __future__ import annotations


SCENARIOS = (
    ("optimistic", 0.25, 0.005, 0.01),
    ("base", 0.10, 0.01, 0.02),
    ("conservative", 0.05, 0.02, 0.03),
)


def screen_market_making(events: list[dict], *, quote_size: float = 10.0, maker_fee_coefficient: float = 0.0175) -> dict:
    """Estimate one snapshot horizon; this is not a historical fill backtest."""
    quote_size = max(0.01, float(quote_size))
    rows = []
    for event in events:
        for market in event.get("markets") or []:
            ticker = str(market.get("ticker") or "").strip()
            bid = _number(market.get("yes_bid_dollars"))
            ask = _number(market.get("yes_ask_dollars"))
            bid_depth = _number(market.get("yes_bid_size_fp"))
            ask_depth = _number(market.get("yes_ask_size_fp"))
            if not ticker or bid is None or ask is None or not 0 < bid < ask < 1 or bid_depth is None or ask_depth is None:
                continue
            size = min(quote_size, bid_depth, ask_depth)
            scenarios = {}
            for name, fill_probability, adverse_cost, unpaired_cost in SCENARIOS:
                completed = size * fill_probability * fill_probability
                unpaired = size * 2 * fill_probability * (1 - fill_probability)
                expected_fills = completed * 2 + unpaired
                average_fee = maker_fee_coefficient * ((bid * (1 - bid) + ask * (1 - ask)) / 2)
                net = completed * ((ask - bid) - adverse_cost) - expected_fills * average_fee - unpaired * unpaired_cost
                capital = size * ask
                scenarios[name] = {
                    "fill_probability_per_side": fill_probability,
                    "expected_completed_round_trips": round(completed, 6),
                    "expected_unpaired_fills": round(unpaired, 6),
                    "adverse_selection_per_round_trip": adverse_cost,
                    "unpaired_inventory_cost": unpaired_cost,
                    "estimated_net_profit": round(net, 6),
                    "estimated_roi_on_quote_capital_pct": round(net / capital * 100, 6) if capital else None,
                }
            conservative = scenarios["conservative"]
            rows.append({
                "event_ticker": event.get("event_ticker"), "ticker": ticker,
                "title": market.get("title") or market.get("yes_sub_title") or event.get("title"),
                "category": event.get("category"), "yes_bid": bid, "yes_ask": ask,
                "spread_dollars": round(ask - bid, 6), "bid_depth": bid_depth, "ask_depth": ask_depth,
                "modeled_quote_size": size, "queue_position_modeled": False, "scenarios": scenarios,
                "conservative_positive": conservative["estimated_net_profit"] > 0,
                "eligible": False,
                "blockers": ["snapshot_not_fill_history", "queue_position_unknown", "series_fee_not_verified",
                             "account_and_jurisdiction_not_verified"],
            })
    rows.sort(key=lambda row: row["scenarios"]["conservative"]["estimated_roi_on_quote_capital_pct"], reverse=True)
    return {
        "strategy": "prediction_market_two_sided_maker_screen", "market_count": len(rows),
        "conservative_positive_count": sum(row["conservative_positive"] for row in rows),
        "candidates": rows, "maker_fee_coefficient": maker_fee_coefficient,
        "fee_model": "conservative quadratic maker formula; actual applicability varies by series",
        "research_only": True, "execution_enabled": False,
    }


def _number(value) -> float | None:
    try:
        number = float(value)
        return number if number > 0 else None
    except (TypeError, ValueError):
        return None
