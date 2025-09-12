# strategies/base.py
# Version: 2025-09-12-EXEC
# Shared types and base class for all strategies (S1–S4).
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from abc import ABC, abstractmethod
from typing import Any, Dict, List


__all__ = ["OrderPlan", "BaseStrategy", "__version__"]
__version__ = "1.0.0"


@dataclass
class OrderPlan:
    """
    A simple instruction to open a position with a protective bracket.

    Fields:
      - symbol:    Ticker symbol (e.g., "SPY")
      - side:      "buy" or "sell" (sell = short)
      - qty:       Integer share quantity (>= 1 to execute)
      - tp_pct:    Take-profit distance as fraction (e.g., 0.006 for +0.6%)
      - sl_pct:    Stop-loss  distance as fraction (e.g., 0.003 for -0.3%)
      - meta:      Freeform dict for audit/debug (reason, indicators, etc.)
    """
    symbol: str
    side: str
    qty: int
    tp_pct: float
    sl_pct: float
    meta: Dict[str, Any] = field(default_factory=dict)

    def normalized(self) -> "OrderPlan":
        """Return a sanitized copy with standardized side and non-negative qty/pcts."""
        side = str(self.side).strip().lower()
        side = "buy" if side == "buy" else "sell"  # default to 'sell' if not 'buy'
        qty = int(self.qty or 0)
        tp = float(self.tp_pct or 0.0)
        sl = float(self.sl_pct or 0.0)
        return OrderPlan(
            symbol=str(self.symbol).strip().upper(),
            side=side,
            qty=max(qty, 0),
            tp_pct=max(tp, 0.0),
            sl_pct=max(sl, 0.0),
            meta=dict(self.meta or {}),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Dict representation (handy for JSON responses)."""
        return asdict(self)


class BaseStrategy(ABC):
    """
    Minimal interface all strategies implement.

    Implementations should:
      - Accept a 'market' object in __init__ (see services/market.Market)
      - Implement scan(now, market, symbols) -> List[OrderPlan]
        returning zero or more OrderPlans for execution.

    The concrete strategy can expose a class attribute:
      - system = "MY_SYSTEM_CODE"
    """

    # Optional identifier set by subclasses (e.g., "OPENING_RANGE", "RSI_PB")
    system: str = "UNKNOWN"

    def __init__(self, market: Any):
        self.mkt = market

    @abstractmethod
    def scan(self, now: Any, market: Any, symbols: List[str]) -> List[OrderPlan]:
        """Return a list of OrderPlan objects (may be empty)."""
        raise NotImplementedError
