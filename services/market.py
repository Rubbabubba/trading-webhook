# services/market.py
# Version: 2025-09-15-MARKET-ENV-01
# - Adds Market.from_env()
# - Uniform Alpaca trading/data hosts + feed selection
# - Order submit/get, positions, clock, bars, simple P&L stubs
# - Plan executor used by S1–S4 routes

from __future__ import annotations

import os
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

import requests


def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    s = str(val).strip().lower()
    return s in ("1", "true", "yes", "on")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class MarketConfig:
    api_key: str
    api_secret: str
    trading_base: str
    data_base: str
    feed: str = "iex"


class Market:
    """
    Thin wrapper around Alpaca REST for:
      - trading: /v2/orders, /v2/positions, /v2/account, /v2/clock
      - market data: /v2/stocks/{symbol}/bars
    Plus a tiny plan executor used by strategies S1–S4.
    """

    def __init__(self, cfg: MarketConfig, session: Optional[requests.Session] = None) -> None:
        self.cfg = cfg
        self.session = session or requests.Session()
        self._headers = {
            "APCA-API-KEY-ID": cfg.api_key,
            "APCA-API-SECRET-KEY": cfg.api_secret,
            "Content-Type": "application/json",
            "Accept": "application/json",
            # Note: Alpaca ignores "feed" header; we pass feed via querystring for data calls.
        }

    # -------------------------------------------------------------------------
    # Factory

    @classmethod
    def from_env(cls) -> "Market":
        """
        Create a Market instance from environment variables.

        Required:
          - ALPACA_API_KEY
          - ALPACA_SECRET_KEY

        Optional:
          - ALPACA_PAPER=1 (default: 1)   -> trading_base: paper-api
          - ALPACA_LIVE=1                 -> if set, overrides and uses live trading host
          - ALPACA_TRADING_BASE           -> override full trading base URL
          - ALPACA_DATA_BASE              -> override full data base URL
          - ALPACA_FEED=iex|sip           -> default: iex
        """
        api_key = os.getenv("ALPACA_API_KEY", "").strip()
        api_secret = os.getenv("ALPACA_SECRET_KEY", "").strip()
        if not api_key or not api_secret:
            raise RuntimeError("Missing ALPACA_API_KEY or ALPACA_SECRET_KEY in environment.")

        # Determine trading base
        trading_base = os.getenv("ALPACA_TRADING_BASE")
        if not trading_base:
            # live vs paper precedence:
            if _env_bool("ALPACA_LIVE", False):
                trading_base = "https://api.alpaca.markets"
            else:
                # Default to paper unless explicitly live
                trading_base = "https://paper-api.alpaca.markets"

        # Data base (almost always data.alpaca.markets)
        data_base = os.getenv("ALPACA_DATA_BASE", "https://data.alpaca.markets").rstrip("/")

        feed = os.getenv("ALPACA_FEED", "iex").strip().lower() or "iex"

        cfg = MarketConfig(
            api_key=api_key,
            api_secret=api_secret,
            trading_base=trading_base.rstrip("/"),
            data_base=data_base,
            feed=feed,
        )
        return cls(cfg)

    # -------------------------------------------------------------------------
    # Low-level HTTP

    def _req(self, method: str, url: str, **kw) -> requests.Response:
        r = self.session.request(method, url, headers=self._headers, timeout=30, **kw)
        if not r.ok:
            # Try to surface JSON error if present
            try:
                detail = r.json()
            except Exception:
                detail = r.text
            raise requests.HTTPError(f"{r.status_code} {r.reason}: {url} -> {detail}")
        return r

    # -------------------------------------------------------------------------
    # Diagnostics & Info

    def get_alpaca_bases(self) -> Dict[str, Any]:
        return {
            "trading_base": self.cfg.trading_base,
            "data_base": self.cfg.data_base,
            "feed": self.cfg.feed,
        }

    def get_clock(self) -> Dict[str, Any]:
        url = f"{self.cfg.trading_base}/v2/clock"
        r = self._req("GET", url)
        data = r.json()
        # Normalize: ensure we always return iso timestamp as 'timestamp'
        if "timestamp" not in data:
            data["timestamp"] = _now_iso()
        return data

    # -------------------------------------------------------------------------
    # Account / Equity / Simple P&L stubs (for dashboard)

    def get_account(self) -> Dict[str, Any]:
        url = f"{self.cfg.trading_base}/v2/account"
        return self._req("GET", url).json()

    def get_equity_fast(self) -> Optional[float]:
        try:
            acct = self.get_account()
            eq = acct.get("equity")
            return float(eq) if eq is not None else None
        except Exception:
            return None

    def get_pnl_fast(self, days: int = 7) -> Optional[float]:
        # Stub: return None (or compute from activities if you want)
        return None

    def get_daily_pnl_fast(self, days: int = 7) -> List[Dict[str, Any]]:
        # Stub daily series; fill with zeros so the chart renders
        today = datetime.now(timezone.utc).date()
        out: List[Dict[str, Any]] = []
        for i in range(days):
            d = today.fromordinal(today.toordinal() - (days - 1 - i))
            out.append({"date": d.isoformat(), "pnl": 0.0})
        return out

    # -------------------------------------------------------------------------
    # Orders & Positions

    def submit_order(
        self,
        *,
        symbol: str,
        qty: int,
        side: str = "buy",
        type: str = "market",
        time_in_force: str = "day",
        limit_price: Optional[float] = None,
        stop_price: Optional[float] = None,
        client_order_id: Optional[str] = None,
        extended_hours: Optional[bool] = None,
        **extra,
    ) -> Dict[str, Any]:
        url = f"{self.cfg.trading_base}/v2/orders"
        payload: Dict[str, Any] = {
            "symbol": symbol.upper(),
            "qty": qty,
            "side": side.lower(),
            "type": type.lower(),
            "time_in_force": time_in_force.lower(),
        }
        if limit_price is not None:
            payload["limit_price"] = float(limit_price)
        if stop_price is not None:
            payload["stop_price"] = float(stop_price)
        if client_order_id:
            payload["client_order_id"] = client_order_id
        if extended_hours is not None:
            payload["extended_hours"] = bool(extended_hours)
        # allow extra passthroughs if strategies include them
        payload.update({k: v for k, v in extra.items() if v is not None})

        return self._req("POST", url, data=json.dumps(payload)).json()

    def get_open_orders(self) -> List[Dict[str, Any]]:
        url = f"{self.cfg.trading_base}/v2/orders?status=open&limit=200&nested=true"
        return self._req("GET", url).json()

    def get_orders_recent(self, limit: int = 200) -> List[Dict[str, Any]]:
        url = f"{self.cfg.trading_base}/v2/orders?status=all&limit={int(limit)}&nested=true"
        return self._req("GET", url).json()

    def get_positions(self) -> List[Dict[str, Any]]:
        url = f"{self.cfg.trading_base}/v2/positions"
        return self._req("GET", url).json()

    # -------------------------------------------------------------------------
    # Market Data (bars)

    def get_bars(
        self,
        symbol: str,
        *,
        timeframe: str = "1Min",
        limit: int = 400,
        adjustment: str = "raw",
        start: Optional[str] = None,
        end: Optional[str] = None,
        feed: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Minimal /v2/stocks/{symbol}/bars wrapper.
        Returns raw JSON from Alpaca (with 'bars' list).
        """
        params = {
            "timeframe": timeframe,
            "limit": int(limit),
            "adjustment": adjustment,
            "feed": (feed or self.cfg.feed),
        }
        if start:
            params["start"] = start
        if end:
            params["end"] = end

        url = f"{self.cfg.data_base}/v2/stocks/{symbol.upper()}/bars"
        return self._req("GET", url, params=params).json()

    # -------------------------------------------------------------------------
    # Strategy Plan Execution

    def execute_plans(self, plans: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Execute a list of generated order 'plans'.
        A plan is expected to be a dict with (at minimum) keys:
          - symbol (str)
          - side ('buy'|'sell')
          - qty (int)

        Optional keys respected if present:
          - type ('market'|'limit'|'stop'|'stop_limit'|...)
          - time_in_force ('day'|'gtc'|...)
          - limit_price (float)
          - stop_price (float)
          - extended_hours (bool)
          - client_order_id (str)
        """
        results: List[Dict[str, Any]] = []
        for plan in plans or []:
            try:
                symbol = plan.get("symbol")
                side = (plan.get("side") or "buy").lower()
                qty = int(plan.get("qty") or 0)
                if not symbol or qty <= 0:
                    continue

                order_kwargs: Dict[str, Any] = {
                    "symbol": symbol,
                    "qty": qty,
                    "side": side,
                    "type": (plan.get("type") or "market").lower(),
                    "time_in_force": (plan.get("time_in_force") or "day").lower(),
                }
                if "limit_price" in plan and plan["limit_price"] is not None:
                    order_kwargs["limit_price"] = float(plan["limit_price"])
                if "stop_price" in plan and plan["stop_price"] is not None:
                    order_kwargs["stop_price"] = float(plan["stop_price"])
                if "extended_hours" in plan and plan["extended_hours"] is not None:
                    order_kwargs["extended_hours"] = bool(plan["extended_hours"])
                if "client_order_id" in plan and plan["client_order_id"]:
                    order_kwargs["client_order_id"] = str(plan["client_order_id"])

                # passthrough any other supported fields
                passthrough_keys = (
                    "trail_price", "trail_percent", "order_class", "take_profit", "stop_loss", "notional"
                )
                for k in passthrough_keys:
                    if k in plan and plan[k] is not None:
                        order_kwargs[k] = plan[k]

                resp = self.submit_order(**order_kwargs)
                results.append({"ok": True, "plan": plan, "order": resp})
            except Exception as e:
                results.append({"ok": False, "plan": plan, "error": str(e)})
        return results
