"""Liquidity-confirmed momentum signal for the prospective v4 paper challenger."""
from __future__ import annotations

from decimal import Decimal
from fractions import Fraction

from .kalshi_shadow import cost, price_book
from .kalshi_shadow_live import Coordinator, epoch, identity


def momentum_entry_signal(history, frame, config):
    """Follow a persistent 4-7 minute move only after a separate recent confirmation.

    The filter deliberately rejects extreme prices, wide or shallow books, and
    single-print jumps.  A fill is still simulated only on a later distinct book.
    """
    at = frame["received_at"]
    anchors = [mid for when, mid in history if 240 <= at - when <= 420]
    confirmations = [mid for when, mid in history if 30 <= at - when <= 120]
    if not anchors or not confirmations:
        return None
    anchor, confirmation = anchors[-1], confirmations[-1]
    current_yes_bid, current_yes_ask, _, _ = price_book(frame, "yes")
    current = (current_yes_bid + current_yes_ask) / 2
    total_move = current - anchor
    recent_move = current - confirmation
    if abs(total_move) < Fraction(6, 100) or abs(total_move) > Fraction(20, 100):
        return None
    if total_move * recent_move <= 0 or abs(recent_move) < Fraction(2, 100):
        return None
    side = "yes" if total_move > 0 else "no"
    bid, ask, bid_size, ask_size = price_book(frame, side)
    midpoint = (bid + ask) / 2
    if not Fraction(20, 100) <= midpoint <= Fraction(80, 100):
        return None
    if ask - bid > Fraction(4, 100) or bid_size < 3 or ask_size < 3:
        return None
    # Directional top-of-book support reduces entries against a one-sided queue.
    if bid_size < ask_size * Fraction(5, 4):
        return None
    coefficient = Decimal(frame["fee_coefficient"])
    debit = cost(ask, coefficient, config["slippage_cents"], True)
    if debit >= 90:
        return None
    return {"side": side, "strategy_id": config["strategy_id"]}


class MomentumCoordinator(Coordinator):
    """Coordinator that preserves the public-book safeguards with the v4 signal."""

    def observe(self, frame, market, series, *, now):
        ticker = frame["ticker"]
        expected = self.universe[ticker]
        if identity(market) != identity(expected["market"]):
            raise ValueError("market_identity_changed")
        if market.get("status") != "active" or market.get("market_type") != "binary":
            raise ValueError("market_unavailable_inventory_retained")
        if series.get("ticker") != expected["series"]["ticker"] \
                or series.get("fee_type") not in ("quadratic", "quadratic_with_maker_fees"):
            raise ValueError("unsupported_series")
        coefficient = Decimal(".07") * Decimal(str(series["fee_multiplier"]))
        if not coefficient.is_finite() or not 0 <= coefficient <= Decimal(".14"):
            raise ValueError("unsupported_fee")
        frame = dict(frame, fee_coefficient=str(coefficient.normalize()))
        yes_bid, yes_ask, _, _ = price_book(frame, "yes")
        state = self.engine.state()
        active = state["position"] or state["pending"]
        if active and active["ticker"] != ticker:
            return {"action": "portfolio_busy", "execution_enabled": False}
        if state["realized_cents"] <= -500:
            self.engine.db.execute("INSERT OR IGNORE INTO runtime VALUES('loss_stop','true')")
        stopped = self.engine.db.execute("SELECT 1 FROM runtime WHERE name='loss_stop'").fetchone()
        closing = now >= min(epoch(market["close_time"]) - 1800, self.stop_at - 1200)
        if closing:
            frame["signal_valid"] = False
        rows = self.engine.db.execute(
            "SELECT at,mid FROM history WHERE ticker=? AND at>=? ORDER BY at", (ticker, now - 600)
        ).fetchall()
        history = [(when, Fraction(mid)) for when, mid in rows]
        signal = None if closing or stopped or frame.get("signal_valid") is False \
            or state["position"] else momentum_entry_signal(history, frame, self.engine.config)
        if state["pending"] and state["pending"]["kind"] == "entry" and (
                signal is None or signal["side"] != state["pending"]["side"]):
            frame["signal_valid"] = False
        result = self.engine.step(frame, now=now, signal=signal)
        self.engine.db.execute(
            "INSERT INTO history VALUES(?,?,?)", (frame["received_at"], ticker, str((yes_bid + yes_ask) / 2))
        )
        self.engine.db.execute("DELETE FROM history WHERE at<?", (now - 600,))
        return result
