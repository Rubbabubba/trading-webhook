from __future__ import annotations

import logging
import os
import re
from typing import Optional

from book import ScanResult
from strategy_api import OrderIntent, PositionSnapshot, RiskContext

logger = logging.getLogger(__name__)

STRAT_ID = "e1"

# ---- Env knobs (all optional) ---------------------------------------------
E1_ALLOW_SHORTS = str(os.getenv("E1_ALLOW_SHORTS", "true")).lower() in ("1","true","yes","y")
E1_VWAP_TOL_ATR = float(os.getenv("E1_VWAP_TOL_ATR", "0.20"))  # exit when within this ATR of VWAP
E1_MIN_SCORE    = float(os.getenv("E1_MIN_SCORE", "0.50"))     # score threshold for entries
E1_ENABLE_SIGNAL_EXIT = str(os.getenv("E1_ENABLE_SIGNAL_EXIT","true")).lower() in ("1","true","yes","y")


def _parse_float(label: str, text: str) -> Optional[float]:
    # expects fragments like "vwap=123.45" or "close=123.45"
    m = re.search(rf"\b{re.escape(label)}=([-+]?[0-9]*\.?[0-9]+)\b", text or "")
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def _is_flat(position: Optional[PositionSnapshot]) -> bool:
    if not position:
        return True
    return abs(float(position.qty or 0.0)) < 1e-10


class E1Strategy:
    """
    e1 — VWAP mean-reversion with BOS confirmation (equities)

    Inputs come from book.scan() for strat_id 'e1'.
    For e1, ScanResult.reason includes `vwap=<float>` and `close=<float>`.

    Entry
      - If FLAT and scan.score >= E1_MIN_SCORE:
          * scan.action == "buy"  -> open long
          * scan.action == "sell" -> open short (if E1_ALLOW_SHORTS)

    Exit
      - If in a position:
          * Signal exit: opposite action from scan (optional)
          * VWAP exit: close within E1_VWAP_TOL_ATR * ATR of VWAP (parsed from reason)
    """

    def entry_signal(
        self,
        scan: ScanResult,
        position: Optional[PositionSnapshot] = None,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        if not isinstance(scan, ScanResult) or not scan.selected:
            return None
        if not _is_flat(position):
            return None

        if float(scan.score or 0.0) < E1_MIN_SCORE:
            return None

        if scan.action == "buy":
            return OrderIntent(
                symbol=scan.symbol,
                strategy=STRAT_ID,
                side="buy",
                qty=float(scan.qty or 0.0),
                notional=float(scan.notional or 0.0),
                reason=scan.reason or "e1_entry_long",
            )

        if scan.action == "sell" and E1_ALLOW_SHORTS:
            # For shorts, qty should be positive in the ScanResult; broker layer applies side
            return OrderIntent(
                symbol=scan.symbol,
                strategy=STRAT_ID,
                side="sell",
                qty=float(scan.qty or 0.0),
                notional=float(scan.notional or 0.0),
                reason=scan.reason or "e1_entry_short",
            )

        return None

    def exit_signal(
        self,
        scan: ScanResult,
        position: Optional[PositionSnapshot] = None,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        if _is_flat(position) or not position:
            return None

        # VWAP target exit (primary)
        vwap = _parse_float("vwap", scan.reason or "")
        close = _parse_float("close", scan.reason or "")
        atr = float(scan.atr or 0.0)

        if vwap is not None and close is not None and atr > 0:
            tol = E1_VWAP_TOL_ATR * atr
            if abs(close - vwap) <= tol:
                side = "sell" if position.qty > 0 else "buy"
                return OrderIntent(
                    symbol=position.symbol,
                    strategy=STRAT_ID,
                    side=side,
                    qty=abs(float(position.qty)),
                    notional=None,
                    reason=f"e1_vwap_exit close={close:.4f} vwap={vwap:.4f} tol_atr={E1_VWAP_TOL_ATR}",
                )

        # Optional: opposite signal exit
        if E1_ENABLE_SIGNAL_EXIT and isinstance(scan, ScanResult):
            if position.qty > 0 and scan.action == "sell":
                return OrderIntent(
                    symbol=position.symbol,
                    strategy=STRAT_ID,
                    side="sell",
                    qty=abs(float(position.qty)),
                    notional=None,
                    reason="e1_signal_exit_long",
                )
            if position.qty < 0 and scan.action == "buy":
                return OrderIntent(
                    symbol=position.symbol,
                    strategy=STRAT_ID,
                    side="buy",
                    qty=abs(float(position.qty)),
                    notional=None,
                    reason="e1_signal_exit_short",
                )

        return None

    def profit_take_rule(self, position: PositionSnapshot, risk: Optional[RiskContext] = None) -> Optional[OrderIntent]:
        # Keep numeric TP centralized in risk.json (profit_lock / etc.)
        return None

    def stop_loss_rule(self, position: PositionSnapshot, risk: Optional[RiskContext] = None) -> Optional[OrderIntent]:
        # Keep numeric SL centralized in risk.json (loss_zone / etc.)
        return None

    def should_scale(
        self,
        scan: ScanResult,
        position: Optional[PositionSnapshot] = None,
        risk: Optional[RiskContext] = None,
    ) -> bool:
        # For this first pass, no scaling. Keep it clean + deterministic.
        return False


e1 = E1Strategy()
