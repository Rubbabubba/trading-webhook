"""Replay reconciled YES fills with exact FIFO basis and Central-day realized P&L.

No prices, fills or settlements are invented. Unsettled inventory stays at cost.
Fractions preserve exact proportional fee allocation; decimal strings are display.
"""
from .kalshi_order_direction import outcome_for_book
from collections import defaultdict, deque
from datetime import datetime, timezone
from decimal import Decimal, localcontext
from fractions import Fraction
from zoneinfo import ZoneInfo

CENTRAL = ZoneInfo("America/Chicago")


def amount(value):
    if not isinstance(value, str) or len(value) > 40:
        raise ValueError("invalid_accounting_amount")
    try:
        number = Decimal(value)
    except Exception:
        raise ValueError("invalid_accounting_amount") from None
    if not number.is_finite() or number < 0 or number > 1_000_000:
        raise ValueError("invalid_accounting_amount")
    return Fraction(number)


def display(value):
    with localcontext() as context:
        context.prec = 28
        return str(Decimal(value.numerator) / Decimal(value.denominator))


def timestamp(value):
    if not isinstance(value, str):
        raise ValueError("missing_fill_timestamp")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("invalid_fill_timestamp") from None
    if result.tzinfo is None:
        raise ValueError("naive_fill_timestamp")
    return result.astimezone(timezone.utc)


def replay(records, evidence, *, as_of=None, settlements=()):
    now = as_of or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("naive_accounting_time")
    day = now.astimezone(CENTRAL).date().isoformat()
    events, seen, pending = [], set(), Fraction(0)
    for index, record in enumerate(records):
        p = record["payload"]
        if record["state"] != "terminal":
            # Deliberately conservative for partial orders: retain their full reserve
            # in addition to basis from previously reconciled fills.
            pending += Fraction(record["reserve"], 100)
        detail = evidence.get(p["client_order_id"])
        if detail is None:
            if record["filled"] or record["state"] in ("terminal", "working"):
                raise ValueError("missing_accounting_evidence")
            continue
        total, gross, fees = 0, Fraction(0), Fraction(0)
        for fill in detail["fills"]:
            fid = fill.get("fill_id")
            if not isinstance(fid, str) or not fid or fid in seen:
                raise ValueError("duplicate_accounting_fill")
            seen.add(fid)
            if (fill.get("order_id") != record["broker_id"] or fill.get("ticker") != p["ticker"]
                    or fill.get("outcome_side") != outcome_for_book(p["side"]) or fill.get("book_side") != p["side"]
                    or type(fill.get("subaccount_number")) is not int or fill["subaccount_number"] != 0):
                raise ValueError("accounting_identity_mismatch")
            count, price, fee = amount(fill["count_fp"]), amount(fill["yes_price_dollars"]), amount(fill["fee_cost"])
            if count.denominator != 1 or count <= 0 or price > 1:
                raise ValueError("unsupported_accounting_fill")
            at = timestamp(fill.get("created_time"))
            if at > now:
                raise ValueError("future_fill_evidence")
            total += int(count); gross += count * price; fees += fee
            events.append((at, index, fid, p["ticker"], p["side"], int(count), count * price, fee))
        if total != record["filled"] or gross != amount(detail["gross_dollars"]) or fees != amount(detail["fees_dollars"]):
            raise ValueError("accounting_totals_mismatch")
    settled = set()
    for row in settlements:
        ticker = row.get("ticker")
        if not isinstance(ticker, str) or not ticker or ticker in settled:
            raise ValueError("duplicate_settlement")
        settled.add(ticker)
        trades = [e for e in events if e[3] == ticker]
        at = timestamp(row.get("settled_time"))
        if at > now or not trades or any(e[0] >= at for e in trades):
            raise ValueError("invalid_settlement_time")
        if any(
                r["state"] != "terminal" for r in records if r["payload"]["ticker"] == ticker):
            raise ValueError("unsupported_settlement_history")
        # Independently reconcile the remaining gross FIFO basis against the
        # exchange. A different basis convention stays blocked, not approximated.
        remaining_lots = deque()
        for trade in sorted(trades):
            if trade[4] == "bid":
                remaining_lots.append([trade[5], trade[6]])
            elif trade[4] == "ask":
                needed = trade[5]
                while needed:
                    if not remaining_lots:
                        raise ValueError("sale_without_owned_inventory")
                    lot = remaining_lots[0]
                    take = min(needed, lot[0])
                    lot[1] -= lot[1] * Fraction(take, lot[0])
                    lot[0] -= take
                    needed -= take
                    if not lot[0]:
                        remaining_lots.popleft()
            else:
                raise ValueError("unsupported_accounting_side")
        count = sum(lot[0] for lot in remaining_lots)
        gross = sum((lot[1] for lot in remaining_lots), Fraction(0))
        if not count:
            raise ValueError("unsupported_settlement_history")
        fees = sum((e[7] for e in trades), Fraction(0))
        result = row.get("market_result")
        revenue = row.get("revenue")
        if (type(row.get("exchange_index")) is not int or row["exchange_index"] != 0
                or result not in ("yes", "no") or type(revenue) is not int
                or revenue != count * (100 if result == "yes" else 0)
                or amount(row.get("yes_count_fp")) != count
                or amount(row.get("no_count_fp")) != 0
                or amount(row.get("no_total_cost_dollars")) != 0
                or amount(row.get("yes_total_cost_dollars")) != gross
                or amount(row.get("fee_cost")) != fees):
            raise ValueError("settlement_not_reconciled")
        if row.get("value") is not None and (type(row["value"]) is not int
                or row["value"] != (100 if result == "yes" else 0)):
            raise ValueError("settlement_value_mismatch")
        # Trading fees already entered basis on each buy; never charge them twice.
        events.append((at, len(records), ticker, ticker, "ask", count, Fraction(revenue, 100), Fraction(0)))
    lots, daily, lows, all_fees = defaultdict(deque), defaultdict(Fraction), defaultdict(Fraction), Fraction(0)
    cashflow = Fraction(0)
    for at, _, _, ticker, side, count, gross, fee in sorted(events):
        all_fees += fee
        if side == "bid":
            lots[ticker].append([count, gross + fee])
            cashflow -= gross + fee
        elif side == "ask":
            needed, basis = count, Fraction(0)
            while needed:
                if not lots[ticker]:
                    raise ValueError("sale_without_owned_inventory")
                lot = lots[ticker][0]
                take = min(needed, lot[0]); allocated = lot[1] * Fraction(take, lot[0])
                basis += allocated; lot[0] -= take; lot[1] -= allocated; needed -= take
                if not lot[0]:
                    lots[ticker].popleft()
            cashflow += gross - fee
            event_day = at.astimezone(CENTRAL).date().isoformat()
            daily[event_day] += gross - fee - basis
            lows[event_day] = min(lows[event_day], daily[event_day])
        else:
            raise ValueError("unsupported_accounting_side")
    basis = sum((lot[1] for queue in lots.values() for lot in queue), Fraction(0))
    return {"day": day, "daily_realized": daily[day], "daily_low": lows[day], "realized": sum(daily.values(), Fraction(0)),
            "open_basis": basis, "pending_reserves": pending, "capital_at_risk": basis + pending,
            "fees": all_fees, "cashflow": cashflow,
            "positions": {ticker: sum(lot[0] for lot in queue) for ticker, queue in lots.items() if queue},
            "daily": dict(daily)}


def report(state):
    return {key: display(value) if isinstance(value, Fraction) else
            {k: display(v) for k, v in value.items()} if key == "daily" else value
            for key, value in state.items()}
