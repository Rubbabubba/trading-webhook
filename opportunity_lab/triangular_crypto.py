"""Read-only Kraken triangular-arbitrage research."""

from __future__ import annotations

from datetime import datetime, timezone

from .cross_exchange_crypto import fetch_kraken_pair


def collect_triangular(*, starting_usd: float = 1000, taker_fee: float = .008) -> dict:
    books, transports = {}, {}
    for base, quote in (("BTC", "USD"), ("ETH", "USD"), ("ETH", "BTC")):
        key = f"{base}/{quote}"
        books[key], transports[key] = fetch_kraken_pair(base, quote)
    if any(row.get("error") for row in transports.values()):
        return {"ok": False, "venue": "kraken", "transports": transports, "execution_enabled": False}
    return {"ok": True, "venue": "kraken", "observed_at": datetime.now(timezone.utc).isoformat(),
            "transports": transports, "scan": scan_triangular(books, starting_usd=starting_usd, taker_fee=taker_fee),
            "execution_enabled": False}


def scan_triangular(books: dict, *, starting_usd: float = 1000, taker_fee: float = .008) -> dict:
    cycles = [
        _cycle("USD_BTC_ETH_USD", starting_usd, taker_fee, [
            (books["BTC/USD"], "buy", "USD", "BTC"),
            (books["ETH/BTC"], "buy", "BTC", "ETH"),
            (books["ETH/USD"], "sell", "ETH", "USD"),
        ]),
        _cycle("USD_ETH_BTC_USD", starting_usd, taker_fee, [
            (books["ETH/USD"], "buy", "USD", "ETH"),
            (books["ETH/BTC"], "sell", "ETH", "BTC"),
            (books["BTC/USD"], "sell", "BTC", "USD"),
        ]),
    ]
    cycles.sort(key=lambda row: row["net_profit_usd"], reverse=True)
    return {"strategy": "triangular_crypto", "venue": "kraken", "starting_usd": starting_usd,
            "taker_fee_per_leg": taker_fee, "cycle_count": len(cycles), "cycles": cycles,
            "best_cycle": cycles[0], "profitable_cycle_count": sum(row["profitable"] for row in cycles),
            "eligible": False,
            "blockers": ["three_leg_atomicity_unavailable", "latency_and_book_change_risk", "account_fee_tier_not_verified"],
            "research_only": True, "execution_enabled": False}


def _cycle(name: str, starting: float, fee: float, legs: list[tuple]) -> dict:
    amount, details, complete = float(starting), [], True
    for book, side, source, target in legs:
        before = amount
        amount, consumed, leg_complete, average = _convert(book, side, amount)
        fee_amount = amount * fee
        amount -= fee_amount
        complete = complete and leg_complete
        details.append({"pair": book.get("symbol"), "side": side, "from": source, "to": target,
                        "input_amount": round(before, 10), "input_consumed": round(consumed, 10),
                        "average_price": round(average, 10) if average is not None else None,
                        "fee_in_output_asset": round(fee_amount, 10), "output_after_fee": round(amount, 10),
                        "depth_complete": leg_complete})
        if not leg_complete:
            break
    final_usd = amount if complete and len(details) == 3 else 0.0
    net = final_usd - starting
    return {"cycle": name, "complete": complete and len(details) == 3, "final_usd": round(final_usd, 6),
            "net_profit_usd": round(net, 6), "roi_pct": round(net / starting * 100, 6) if starting else 0,
            "profitable": complete and len(details) == 3 and net > 0, "legs": details}


def _convert(book: dict, side: str, input_amount: float) -> tuple[float, float, bool, float | None]:
    rows = book.get("asks" if side == "buy" else "bids") or []
    remaining, consumed, output, weighted = input_amount, 0.0, 0.0, 0.0
    for row in rows:
        price, base_size = float(row["price"]), float(row["size"])
        capacity = base_size * price if side == "buy" else base_size
        used = min(remaining, capacity)
        if used <= 0:
            continue
        produced = used / price if side == "buy" else used * price
        consumed += used; output += produced; weighted += produced * price
        remaining -= used
        if remaining <= 1e-10:
            break
    average = weighted / output if output else None
    return output, consumed, remaining <= 1e-8, average
