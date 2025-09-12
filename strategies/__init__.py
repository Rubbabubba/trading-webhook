# strategies/__init__.py
# Make strategies a package and expose common types.

from .base import BaseStrategy, OrderPlan  # re-export for convenience

__all__ = ["BaseStrategy", "OrderPlan"]
