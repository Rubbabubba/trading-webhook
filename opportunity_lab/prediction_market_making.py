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


def replay_quote(previous: dict, current: dict, trades: list[dict], *, maker_fee_coefficient: float = 0.0175,
                 queue_ahead_fraction: float = 1.0) -> dict:
    """Replay public prints against one earlier quote, requiring queue-ahead clearance."""
    ticker = previous.get("ticker")
    bid, ask = float(previous["yes_bid"]), float(previous["yes_ask"])
    size = float(previous["modeled_quote_size"])
    queue_ahead_fraction = max(0.0, min(1.0, float(queue_ahead_fraction)))
    displayed_bid_depth, displayed_ask_depth = float(previous["bid_depth"]), float(previous["ask_depth"])
    buy_volume = sell_volume = 0.0
    trade_count = 0
    for trade in trades:
        if trade.get("ticker") != ticker:
            continue
        price, count = _number(trade.get("yes_price_dollars")), _number(trade.get("count_fp"))
        if price is None or count is None:
            continue
        trade_count += 1
        taker_side = str(trade.get("taker_side") or trade.get("taker_outcome_side") or "").lower()
        if taker_side == "no" and price <= bid:
            buy_volume += count
        elif taker_side == "yes" and price >= ask:
            sell_volume += count
    current_bid, current_ask = float(current["yes_bid"]), float(current["yes_ask"])
    def outcome(fraction: float) -> dict:
        bid_queue, ask_queue = displayed_bid_depth * fraction, displayed_ask_depth * fraction
        buy_fills = min(size, max(0.0, buy_volume - bid_queue))
        sell_fills = min(size, max(0.0, sell_volume - ask_queue))
        paired = min(buy_fills, sell_fills)
        long_inventory, short_inventory = buy_fills - paired, sell_fills - paired
        gross = paired * (ask - bid) + long_inventory * (current_bid - bid) + short_inventory * (ask - current_ask)
        fees = (maker_fee_coefficient * buy_fills * bid * (1 - bid)
                + maker_fee_coefficient * sell_fills * ask * (1 - ask))
        return {"queue_ahead_fraction": fraction, "bid_queue_ahead": bid_queue, "ask_queue_ahead": ask_queue,
                "simulated_buy_fills": buy_fills, "simulated_sell_fills": sell_fills,
                "paired_round_trips": paired, "ending_long_inventory": long_inventory,
                "ending_short_inventory": short_inventory, "gross_marked_pnl": gross,
                "estimated_maker_fees": fees, "net_marked_pnl": gross - fees}
    selected = outcome(queue_ahead_fraction)
    sensitivity = {str(fraction): {key: round(value, 6) if isinstance(value, float) else value
                                   for key, value in outcome(fraction).items()}
                   for fraction in (0.0, .25, .5, 1.0)}
    buy_fills, sell_fills = selected["simulated_buy_fills"], selected["simulated_sell_fills"]
    paired = selected["paired_round_trips"]
    long_inventory, short_inventory = selected["ending_long_inventory"], selected["ending_short_inventory"]
    gross, fees, net = selected["gross_marked_pnl"], selected["estimated_maker_fees"], selected["net_marked_pnl"]
    capital = max(size * ask, 0.01)
    return {
        "ticker": ticker, "trade_count": trade_count, "qualifying_buy_volume": round(buy_volume, 6),
        "qualifying_sell_volume": round(sell_volume, 6), "queue_ahead_fraction": queue_ahead_fraction,
        "bid_queue_ahead": round(selected["bid_queue_ahead"], 6),
        "ask_queue_ahead": round(selected["ask_queue_ahead"], 6), "simulated_buy_fills": round(buy_fills, 6),
        "simulated_sell_fills": round(sell_fills, 6), "paired_round_trips": round(paired, 6),
        "ending_long_inventory": round(long_inventory, 6), "ending_short_inventory": round(short_inventory, 6),
        "gross_marked_pnl": round(gross, 6), "estimated_maker_fees": round(fees, 6),
        "net_marked_pnl": round(net, 6), "roi_on_quote_capital_pct": round(net / capital * 100, 6),
        "queue_position_sensitivity": sensitivity,
        "profitable": net > 0, "eligible": False,
        "blockers": ["public_trade_inference_not_account_fill", "quote_persistence_between_snapshots_unknown",
                     "series_fee_not_verified", "account_and_jurisdiction_not_verified"],
    }


def _number(value) -> float | None:
    try:
        number = float(value)
        return number if number > 0 else None
    except (TypeError, ValueError):
        return None
