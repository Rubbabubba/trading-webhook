# strategies/base.py
from dataclasses import dataclass
from typing import Dict, Any, List, Literal, Protocol

Side = Literal["buy", "sell"]

@dataclass
class OrderPlan:
    symbol: str
    side: Side
    qty: float
    tp_pct: float
    sl_pct: float
    meta: Dict[str, Any]

class BaseStrategy(Protocol):
    system: str
    def scan(self, now, market, symbols: List[str]) -> List[OrderPlan]:
        """Return zero or more OrderPlan objects based on current market state."""
        ...
