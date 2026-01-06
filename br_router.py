#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""br_router.py — broker selection shim (equities-only)

This repo is an Alpaca-only **equities** trading system.
We keep this module so the rest of the code can continue importing `br_router`
without changes, but it always routes to the Alpaca adapter in `broker.py`.
"""

from __future__ import annotations

from types import ModuleType
import broker as _broker


def init_broker() -> ModuleType:
    """Return the active broker module (always Alpaca)."""
    return _broker


# Re-export the broker adapter API
get_bars = _broker.get_bars
last_price = _broker.last_price
last_trade_map = _broker.last_trade_map
market_notional = _broker.market_notional
orders = _broker.orders
positions = _broker.positions
trades_history = _broker.trades_history
