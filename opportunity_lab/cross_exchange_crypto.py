"""Read-only, depth-aware cross-exchange spot arbitrage research."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


COINBASE_BOOK_URL = "https://api.coinbase.com/api/v3/brokerage/market/product_book"
KRAKEN_BOOK_URL = "https://api.kraken.com/0/public/PreTrade"


def fetch_coinbase_book(symbol: str, *, limit: int = 50) -> tuple[dict, dict]:
    product_id = f"{symbol.upper()}-USD"
    payload, transport = _get(COINBASE_BOOK_URL, {"product_id": product_id, "limit": max(1, min(100, limit))})
    book = payload.get("pricebook") or {}
    normalized = _book("coinbase", symbol, book.get("bids"), book.get("asks"), "price", "size", book.get("time"))
    if not normalized["bids"] or not normalized["asks"]:
        transport = {**transport, "error": "empty_or_unrecognized_order_book"}
    return normalized, {**transport, "venue": "coinbase", "product_id": product_id, "authenticated": False}


def fetch_kraken_book(symbol: str) -> tuple[dict, dict]:
    product_id = f"{symbol.upper()}/USD"
    payload, transport = _get(KRAKEN_BOOK_URL, {"symbol": product_id})
    result = payload.get("result") or {}
    book = result if result.get("bids") is not None else result.get(product_id) or next((value for value in result.values() if isinstance(value, dict)), {})
    normalized = _book("kraken", symbol, book.get("bids"), book.get("asks"), "price", "qty", None)
    if payload.get("error") or not normalized["bids"] or not normalized["asks"]:
        transport = {**transport, "error": "empty_or_unrecognized_order_book"}
    return normalized, {**transport, "venue": "kraken", "product_id": product_id, "authenticated": False}


def scan_cross_exchange(symbol: str, coinbase: dict, kraken: dict, *, max_notional: float = 1000,
                        coinbase_taker_fee: float = .012, kraken_taker_fee: float = .008) -> dict:
    """Compare both immediate-taker directions using executable depth."""
    fees = {"coinbase": float(coinbase_taker_fee), "kraken": float(kraken_taker_fee)}
    books = {"coinbase": coinbase, "kraken": kraken}
    results = []
    for buy_venue, sell_venue in (("coinbase", "kraken"), ("kraken", "coinbase")):
        result = _evaluate_direction(books[buy_venue], books[sell_venue], max_notional, fees[buy_venue], fees[sell_venue])
        result.update({"buy_venue": buy_venue, "sell_venue": sell_venue})
        results.append(result)
    results.sort(key=lambda row: row["net_profit"], reverse=True)
    best = results[0]
    return {
        "strategy": "cross_exchange_crypto", "symbol": symbol.upper(), "max_notional_per_leg": max_notional,
        "fee_rates": fees, "directions": results, "best_direction": best,
        "profitable_direction_count": sum(row["profitable"] for row in results),
        "eligible": False,
        "blockers": ["simultaneous_fill_not_guaranteed", "venue_balances_not_verified", "withdrawal_and_rebalancing_costs_not_modeled"],
        "research_only": True, "execution_enabled": False,
    }


def collect_cross_exchange(symbol: str, *, max_notional: float = 1000,
                           coinbase_taker_fee: float = .012, kraken_taker_fee: float = .008) -> dict:
    coinbase, coinbase_transport = fetch_coinbase_book(symbol)
    kraken, kraken_transport = fetch_kraken_book(symbol)
    transports = {"coinbase": coinbase_transport, "kraken": kraken_transport}
    if any(row.get("error") for row in transports.values()):
        return {"ok": False, "symbol": symbol.upper(), "transports": transports, "execution_enabled": False}
    return {"ok": True, "symbol": symbol.upper(), "observed_at": datetime.now(timezone.utc).isoformat(),
            "transports": transports,
            "scan": scan_cross_exchange(symbol, coinbase, kraken, max_notional=max_notional,
                                        coinbase_taker_fee=coinbase_taker_fee, kraken_taker_fee=kraken_taker_fee),
            "execution_enabled": False}


def _evaluate_direction(buy: dict, sell: dict, max_notional: float, buy_fee: float, sell_fee: float) -> dict:
    asks, bids = list(buy.get("asks") or []), list(sell.get("bids") or [])
    if not asks or not bids or max_notional <= 0:
        return {"base_quantity": 0, "buy_cost": 0, "sell_proceeds": 0, "fees": 0, "net_profit": 0,
                "roi_on_fully_collateralized_capital_pct": 0, "profitable": False, "complete": False}
    ai = bi = 0
    ask_left, bid_left = asks[0]["size"], bids[0]["size"]
    quantity = buy_cost = sell_proceeds = 0.0
    while ai < len(asks) and bi < len(bids):
        remaining_cost = max_notional - buy_cost
        if remaining_cost <= 1e-9:
            break
        qty = min(ask_left, bid_left, remaining_cost / asks[ai]["price"])
        if qty <= 0:
            break
        quantity += qty
        buy_cost += qty * asks[ai]["price"]
        sell_proceeds += qty * bids[bi]["price"]
        ask_left -= qty
        bid_left -= qty
        if ask_left <= 1e-12:
            ai += 1
            if ai < len(asks): ask_left = asks[ai]["size"]
        if bid_left <= 1e-12:
            bi += 1
            if bi < len(bids): bid_left = bids[bi]["size"]
    fees = buy_cost * buy_fee + sell_proceeds * sell_fee
    net = sell_proceeds - buy_cost - fees
    deployed = buy_cost * (1 + buy_fee) + sell_proceeds
    return {"base_quantity": round(quantity, 10), "average_buy_price": round(buy_cost / quantity, 8) if quantity else None,
            "average_sell_price": round(sell_proceeds / quantity, 8) if quantity else None,
            "buy_cost": round(buy_cost, 6), "sell_proceeds": round(sell_proceeds, 6), "fees": round(fees, 6),
            "gross_spread_profit": round(sell_proceeds - buy_cost, 6), "net_profit": round(net, 6),
            "roi_on_fully_collateralized_capital_pct": round(net / deployed * 100, 6) if deployed else 0,
            "profitable": net > 0, "complete": quantity > 0}


def _book(venue: str, symbol: str, bids, asks, price_key: str, size_key: str, timestamp) -> dict:
    def levels(rows):
        output = []
        for row in rows or []:
            try:
                price = float(row.get(price_key)); size = float(row.get(size_key))
                if price > 0 and size > 0: output.append({"price": price, "size": size})
            except (AttributeError, TypeError, ValueError):
                continue
        return output
    return {"venue": venue, "symbol": symbol.upper(), "timestamp": timestamp,
            "bids": sorted(levels(bids), key=lambda x: x["price"], reverse=True),
            "asks": sorted(levels(asks), key=lambda x: x["price"])}


def _get(url: str, params: dict) -> tuple[dict, dict]:
    request = Request(f"{url}?{urlencode(params)}", headers={"Accept": "application/json", "Cache-Control": "no-cache", "User-Agent": "OpportunityLab/1.0"})
    try:
        with urlopen(request, timeout=20) as response:
            return json.loads(response.read().decode("utf-8")), {"status_code": response.status, "method": "public_rest"}
    except HTTPError as exc:
        return {}, {"status_code": exc.code, "error": f"http_{exc.code}"}
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        return {}, {"error": f"transport_error:{type(exc).__name__}"}
